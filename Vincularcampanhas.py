
import sys, os, site, argparse, shutil, json, subprocess, logging, uuid, socket, getpass
from pathlib import Path
from datetime import datetime, date, timezone
from zoneinfo import ZoneInfo
from typing import Optional, Tuple, Any, List
import requests
import pandas as pd
import polars as pl
import pythoncom
from win32com.client import Dispatch
from PySide6.QtWidgets import QApplication, QDialog, QVBoxLayout, QLabel, QLineEdit, QPushButton, QComboBox, QHBoxLayout
from PySide6.QtCore import QSettings
from playwright.sync_api import sync_playwright, Page, BrowserContext, expect, TimeoutError as PWTimeoutError

for sp in site.getsitepackages():
    if sp not in sys.path:
        sys.path.insert(0, sp)

MOD_DIR = Path.home()/"C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"/"Mensageria e Cargas Operacionais - 11.CelulaPython"/"graciliano"/"novo_servidor"/"modules"
sys.path.insert(0, str(MOD_DIR))
from dollynho import get_credencial
from _utilAutomacoesExec import AutomacoesExecClient

TZ = ZoneInfo("America/Sao_Paulo")
ARQUIVO_ATUAL = Path(__file__).resolve()
NOME_SCRIPT = ARQUIVO_ATUAL.stem.upper()
STEM = ARQUIVO_ATUAL.stem.lower()
NOME_SERVIDOR = "Servidor.py"
NOME_AUTOMACAO = "strauss"
HEADLESS = False
REGRAVAREXCEL = True
ENVIAR_EMAIL_FALHA = ["carlos.lsilva@c6bank.com","sofia.fernandes@c6bank.com"]
BQ_PROJECT_ID = "datalab-pagamentos"
BQ_SOURCE_PROJECT_ID = "c6-banco-comercial-analytics"
BQ_LOCATION = "US"
RET_SUCESSO = 0
RET_FALHA = 1
RET_SEM_DADOS = 2

X_RD_LOGIN_USUARIO={"css":"#login"}
X_RD_LOGIN_SENHA={"css":"#password"}
X_RD_BTN_ENTRAR={"css":"#btn-login"}
X_RD_PARAM_CAMPAIGN_ID={"label":"CAMPAIGN_ID"}
X_RD_INPUT_FILE="input[name='extra.option.FILE']"
X_RD_BTN_RUN={"css":"#execFormRunButton"}
X_RD_STATUS_OK={"css":"span.execstate.overall[data-execstate='SUCCEEDED']"}
X_RD_INTERSTITIAL={"css":"#main-frame-error"}
X_RD_BTN_LOG={"css":"#btn_view_output"}
X_RD_LOG_TEXT={"css":"span.execution-log__content-text"}

class Ambiente:
    def __init__(self):
        self.inicio_exec_sp=datetime.now(TZ)
        self.data_exec=self.inicio_exec_sp.strftime("%Y-%m-%d")
        self.hora_exec=self.inicio_exec_sp.strftime("%H:%M:%S")
        self.base_exec=Path.home()/"C6 CTVM LTDA, BANCO C6 S.A. e C6 HOLDING S.A"/"Mensageria e Cargas Operacionais - 11.CelulaPython"/"graciliano"/"automacoes"
        self.caminho_base=(self.base_exec/NOME_AUTOMACAO) if NOME_AUTOMACAO else self.base_exec
        self.caminho_artefatos=self.caminho_base/STEM/self.inicio_exec_sp.strftime("%d.%m.%Y")
        self.caminho_logs=self.caminho_base/"logs"/STEM/self.inicio_exec_sp.strftime("%d.%m.%Y")
        self.caminho_input=self.caminho_base/"arquivos_input"/STEM
        self.run_ts=self.inicio_exec_sp.strftime("%Y%m%d_%H%M%S")
        self.log_file_path=self.caminho_logs/f"{STEM}_{self.run_ts}.log"
        self.logger=self._criar_logger()
        self.cred_user,self.cred_pass=self._carregar_credencial()
        self.last_sql_corte=""
        self.last_sql_parcela=""
        self.last_rows_corte=None
        self.last_rows_parcela=None
        self.last_vencimento=None
        self.modo_execucao="AUTO"
        self.observacao="AUTO"
        self.usuario=f"{getpass.getuser()}@c6bank.com"
        self.dest_sucesso=self._destinatarios_sucesso()
    def _mkdirs(self):
        for p in [self.caminho_base,self.caminho_artefatos,self.caminho_logs,self.caminho_input]:
            p.mkdir(parents=True, exist_ok=True)
    def _criar_logger(self):
        self._mkdirs()
        logger=logging.getLogger(NOME_SCRIPT)
        logger.setLevel(logging.INFO)
        logger.propagate=False
        logger.handlers=[]
        fh=logging.FileHandler(self.log_file_path,encoding="utf-8")
        sh=logging.StreamHandler(sys.stdout)
        fmt=logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh.setFormatter(fmt); sh.setFormatter(fmt)
        logger.addHandler(fh); logger.addHandler(sh)
        return logger
    def _carregar_credencial(self)->Tuple[str,str]:
        candidatos=(ARQUIVO_ATUAL.stem,"vincular_campanhas","rundeck","attfincards",None)
        for key in candidatos:
            try:
                u,p=(get_credencial(key) if key is not None else get_credencial())
                if u and p:
                    self.logger.info("Credenciais carregadas do Dollynho para método: %s", key or "<auto>")
                    return u,p
            except Exception:
                continue
        return "",""
    def _destinatarios_sucesso(self)->List[str]:
        raw=os.getenv("DESTINATARIOS_SUCESSO","")
        xs=[x.strip() for x in raw.replace(";",",").split(",") if x.strip()]
        return xs if xs else ENVIAR_EMAIL_FALHA
    def tempo_exec_hms(self,end:Optional[datetime]=None)->str:
        dt_end=end or datetime.now(TZ)
        total=int((dt_end-self.inicio_exec_sp).total_seconds())
        h=total//3600; m=(total%3600)//60; s=total%60
        return f"{h:02d}:{m:02d}:{s:02d}"

class Rundeck:
    def __init__(self,amb:Ambiente,page:Page):
        self.amb=amb
        self.page=page
    def _login(self)->None:
        immortal_goto(self.amb,self.page,"https://tasks.corp/user/login")
        try:
            self.page.wait_for_load_state("domcontentloaded")
            try:
                self.page.wait_for_load_state("networkidle",timeout=10000)
            except Exception:
                pass
            u=self.page.locator(X_RD_LOGIN_USUARIO["css"]).first
            p=self.page.locator(X_RD_LOGIN_SENHA["css"]).first
            btn=self.page.locator(X_RD_BTN_ENTRAR["css"]).first
            u.wait_for(state="visible"); p.wait_for(state="visible")
            self.amb.logger.info("Login Rundeck como '%s'", self.amb.cred_user)
            u.click(); u.fill(""); u.type(self.amb.cred_user)
            p.click(); p.fill(""); p.type(self.amb.cred_pass)
            if btn.count()>0: btn.click()
            else: p.press("Enter")
            self.page.wait_for_load_state("domcontentloaded")
            if "/user/login" in (self.page.url or ""): raise RuntimeError("Login não efetivado.")
            self.amb.logger.info("Login Rundeck OK")
        except Exception as e:
            self.amb.logger.error("Falha login Rundeck: %s", e, exc_info=True)
            raise
    def _abrir_e_preencher(self,job_url:str,parametros:list[dict])->None:
        immortal_goto(self.amb,self.page,job_url)
        try:
            wait_visible(self.page,X_RD_BTN_RUN)
        except Exception:
            self._login()
            immortal_goto(self.amb,self.page,job_url)
            wait_visible(self.page,X_RD_BTN_RUN)
        for campo in parametros:
            if str(campo.get("campo")).upper()=="CAMPAIGN_ID":
                loc=self.page.locator("input[name='extra.option.CAMPAIGN_ID']")
                loc.wait_for(state="visible"); loc.fill(str(campo.get("valor","")))
    def rodar_job(self,parametros:list[dict],job_url:str,arquivo:str)->tuple[str,Optional[str]]:
        p=Path(arquivo or "")
        if not p.exists(): return "FALHA",None
        self._abrir_e_preencher(job_url,parametros)
        try:
            locator_from(self.page,X_RD_INPUT_FILE).set_input_files(str(p))
        except Exception as e:
            self.amb.logger.warning("Falha ao anexar arquivo: %s", e)
        immortal_click(self.amb,self.page,X_RD_BTN_RUN)
        while True:
            try:
                locator_from(self.page,X_RD_STATUS_OK).wait_for(timeout=600000)
                break
            except PWTimeoutError:
                if locator_from(self.page,X_RD_INTERSTITIAL).count()>0:
                    self._abrir_e_preencher(job_url,parametros)
                    try:
                        locator_from(self.page,X_RD_INPUT_FILE).set_input_files(str(p))
                    except Exception:
                        pass
                    immortal_click(self.amb,self.page,X_RD_BTN_RUN)
                    continue
                return "FALHA",None
        logs_txt=None
        try:
            self.page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
            immortal_click(self.amb,self.page,X_RD_BTN_LOG)
            locator_from(self.page,X_RD_LOG_TEXT).wait_for()
            logs_txt="\n".join(self.page.locator(X_RD_LOG_TEXT["css"]).all_text_contents())
        except Exception:
            logs_txt=None
        return "SUCCEEDED",logs_txt

def garantir_outlook_aberto(amb:Ambiente)->bool:
    try:
        pythoncom.CoInitialize()
    except Exception:
        pass
    try:
        Dispatch("Outlook.Application")
        amb.logger.info("Outlook disponível")
        return True
    except Exception:
        amb.logger.warning("Outlook indisponível")
        return False

def enviar_email(amb:Ambiente,status:str,tempo_hms:str,tabelas:List[str],linhas:int,anexos:List[Path],resumo:Optional[dict])->None:
    st=(status or "").strip().upper()
    subj=f"Célula Python Monitoração - {STEM} - {st}"
    if st=="SUCESSO": dest=amb.dest_sucesso
    else: dest=ENVIAR_EMAIL_FALHA
    try:
        try:
            pythoncom.CoInitialize()
        except Exception:
            pass
        outlook=Dispatch("Outlook.Application")
        mail=outlook.CreateItem(0)
        mail.Subject=subj
        mail.To="; ".join(dest)
        linhas_publico="-"
        try:
            if resumo and isinstance(resumo.get("bq_rows_parcela"),int):
                linhas_publico=str(resumo["bq_rows_parcela"])
        except Exception:
            linhas_publico="-"
        tabelas_html="<br>".join(tabelas) if tabelas else "-"
        html=(
            f"<html><body style='font-family:Arial,sans-serif'>"
            f"<div style='padding:10px;border-left:6px solid {'#2e7d32' if st=='SUCESSO' else ('#f57c00' if st=='SEM DADOS PARA PROCESSAR' else '#c62828')};background:#fafafa;font-weight:600;margin-bottom:12px'>{st}</div>"
            f"<table cellpadding='6' style='border-collapse:collapse;font-size:14px'>"
            f"<tr><td><b>Data:</b></td><td>{amb.data_exec}</td></tr>"
            f"<tr><td><b>Hora início:</b></td><td>{amb.hora_exec}</td></tr>"
            f"<tr><td><b>Tempo execução:</b></td><td>{tempo_hms}</td></tr>"
            f"<tr><td><b>Projeto fonte:</b></td><td>{BQ_SOURCE_PROJECT_ID}</td></tr>"
            f"<tr><td><b>Projeto billing:</b></td><td>{BQ_PROJECT_ID}</td></tr>"
            f"<tr><td><b>Tabelas:</b></td><td>{tabelas_html}</td></tr>"
            f"<tr><td><b>Linhas público:</b></td><td>{linhas_publico}</td></tr>"
            f"<tr><td><b>Linhas processadas:</b></td><td>{linhas}</td></tr>"
            f"</table></body></html>"
        )
        mail.HTMLBody=html
        anexos=list(anexos or [])
        if amb.log_file_path.exists():
            anexos.append(amb.log_file_path)
        for ap in anexos:
            try:
                if ap and Path(ap).exists():
                    mail.Attachments.Add(str(ap))
            except Exception:
                amb.logger.warning("Falha ao anexar: %s", ap)
        if st=="FALHA":
            try:
                tb_path=amb.caminho_logs/f"{STEM}_{amb.run_ts}_traceback.txt"
                tb_path.write_text("",encoding="utf-8")
                mail.Attachments.Add(str(tb_path))
            except Exception:
                pass
        mail.Send()
        amb.logger.info("E-mail enviado: %s", st)
    except Exception:
        amb.logger.error("Falha ao enviar e-mail", exc_info=True)
    finally:
        try:
            pythoncom.CoUninitialize()
        except Exception:
            pass

def is_execucao_servidor()->bool:
    return os.getenv("SERVIDOR_ORIGEM","").lower()==NOME_SERVIDOR.lower() or "--executado-por-servidor" in sys.argv

def coletar_contexto_manual(amb:Ambiente)->Tuple[str,str,str]:
    app=QApplication.instance() or QApplication(sys.argv)
    dlg=QDialog(); dlg.setWindowTitle("Contexto de Execução")
    layout=QVBoxLayout(dlg)
    hb=QHBoxLayout(); layout.addLayout(hb)
    hb.addWidget(QLabel("Modo:"))
    cb=QComboBox(); cb.addItems(["AUTO","SOLICITACAO"]); hb.addWidget(cb)
    layout.addWidget(QLabel("Observação:"))
    le_obs=QLineEdit(); layout.addWidget(le_obs)
    layout.addWidget(QLabel("Usuário (e-mail):"))
    le_usr=QLineEdit(); le_usr.setText(amb.usuario); layout.addWidget(le_usr)
    btn=QPushButton("OK"); btn.clicked.connect(dlg.accept); layout.addWidget(btn)
    if not dlg.exec():
        return "AUTO","AUTO",amb.usuario
    modo=cb.currentText().upper()
    obs="AUTO" if modo=="AUTO" else (le_obs.text().strip() or "Solicitacao da área")
    usr=le_usr.text().strip() or amb.usuario
    return modo,obs,usr

def _get_access_token()->str:
    tok=os.getenv("GCP_ACCESS_TOKEN") or os.getenv("BQ_TOKEN")
    if tok: return tok.strip()
    try:
        import google.auth, google.auth.transport.requests as tr
        creds,_=google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
        if not creds.valid:
            req=tr.Request()
            creds.refresh(req)
        return creds.token
    except Exception:
        pass
    out=subprocess.check_output(["gcloud","auth","print-access-token"],stderr=subprocess.STDOUT,text=True,timeout=15)
    return out.strip()

def _rows_to_polars(schema:List[dict],rows:List[dict])->pl.DataFrame:
    cols=[f["name"] for f in schema]
    data={c:[] for c in cols}
    for r in rows or []:
        f=r.get("f",[])
        for i,c in enumerate(cols):
            v=f[i]["v"] if i<len(f) else None
            data[c].append(v)
    df=pl.DataFrame(data)
    for f in schema:
        n=f["name"]; t=f.get("type","STRING")
        if t in ("INT64","INTEGER"): df=df.with_columns(pl.col(n).cast(pl.Int64,strict=False))
        elif t in ("FLOAT64","FLOAT","NUMERIC","BIGNUMERIC"): df=df.with_columns(pl.col(n).cast(pl.Float64,strict=False))
        elif t in ("BOOL","BOOLEAN"): df=df.with_columns(pl.col(n).cast(pl.Boolean,strict=False))
        elif t in ("DATE",): df=df.with_columns(pl.col(n).cast(pl.Utf8,strict=False))
        else: df=df.with_columns(pl.col(n).cast(pl.Utf8,strict=False))
    return df

def bq_query_rest(amb:Ambiente,sql:str,project_id:str=BQ_PROJECT_ID,location:str=BQ_LOCATION,timeout:int=120)->pl.DataFrame:
    token=_get_access_token()
    url=f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries"
    headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"}
    max_results=int(os.getenv("BQ_MAX_RESULTS","100000"))
    poll_interval=float(os.getenv("BQ_POLL_INTERVAL_SEC","1.5"))
    max_wait=float(os.getenv("BQ_MAX_WAIT_SEC","600"))
    payload={"query":sql,"useLegacySql":False,"location":location,"maxResults":max_results}
    ca=os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
    amb.logger.info("BQ REST submit | project=%s | location=%s", project_id, location)
    r=requests.post(url,headers=headers,data=json.dumps(payload),timeout=timeout,verify=ca if ca else True)
    if r.status_code!=200:
        amb.logger.error("BQ submit HTTP %s: %s", r.status_code, r.text)
        raise RuntimeError("BigQuery submit falhou")
    j=r.json()
    job_ref=(j.get("jobReference") or {}).get("jobId")
    complete=bool(j.get("jobComplete",True))
    page_token=j.get("pageToken")
    schema_fields=(j.get("schema") or {}).get("fields",[]) or []
    rows_acc=list(j.get("rows") or [])
    total=int(j.get("totalRows",len(rows_acc) if rows_acc else 0) or 0)
    amb.logger.info("BQ job_id=%s | complete=%s | total=%s", job_ref, complete, total)
    qurl=f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/queries/{job_ref}"
    start=datetime.now(tz=timezone.utc).timestamp()
    while not complete:
        if datetime.now(tz=timezone.utc).timestamp()-start>max_wait:
            amb.logger.error("BQ timeout aguardando jobComplete id=%s", job_ref)
            raise RuntimeError("BigQuery timeout")
        import time as _t; _t.sleep(poll_interval)
        rr=requests.get(qurl,headers=headers,params={"location":location,"maxResults":str(max_results)},timeout=timeout,verify=ca if ca else True)
        if rr.status_code!=200:
            amb.logger.error("BQ poll HTTP %s: %s", rr.status_code, rr.text)
            raise RuntimeError("BigQuery poll falhou")
        jj=rr.json()
        complete=bool(jj.get("jobComplete",True))
        if not schema_fields: schema_fields=(jj.get("schema") or {}).get("fields",[]) or schema_fields
        new=jj.get("rows") or []
        if new: rows_acc.extend(new)
        page_token=jj.get("pageToken"); total=int(jj.get("totalRows",total or 0) or 0)
    while page_token:
        rr=requests.get(qurl,headers=headers,params={"location":location,"maxResults":str(max_results),"pageToken":page_token},timeout=timeout,verify=ca if ca else True)
        if rr.status_code!=200:
            amb.logger.error("BQ page HTTP %s: %s", rr.status_code, rr.text)
            raise RuntimeError("BigQuery page falhou")
        jj=rr.json(); rows_acc.extend(jj.get("rows") or []); page_token=jj.get("pageToken")
    if not schema_fields:
        amb.logger.warning("BQ schema vazio id=%s", job_ref)
        return pl.DataFrame()
    df=_rows_to_polars(schema_fields,rows_acc)
    amb.logger.info("BQ concluído id=%s | linhas=%d | colunas=%d", job_ref, df.height, len(df.columns))
    return df

def locator_from(page:Page,spec:Any):
    if isinstance(spec,dict):
        if "role" in spec:
            role,opts=spec["role"]; return page.get_by_role(role,**(opts or {}))
        if "label" in spec: return page.get_by_label(spec["label"])
        if "placeholder" in spec: return page.get_by_placeholder(spec["placeholder"])
        if "text" in spec: return page.get_by_text(spec["text"])
        if "test_id" in spec: return page.get_by_test_id(spec["test_id"])
        if "xpath" in spec: return page.locator(f"xpath={spec['xpath']}")
        if "css" in spec: return page.locator(spec["css"])
    if isinstance(spec,str):
        if spec.strip().startswith("//"): return page.locator(f"xpath={spec}")
        if spec.startswith("css=") or spec.startswith("xpath="): return page.locator(spec)
        return page.locator(spec)
    raise ValueError("Spec inválida")

def immortal_goto(amb:Ambiente,page:Page,url:str)->None:
    end=(int(os.getenv("PLAYWRIGHT_TIMEOUT_MS","60000")))/1000.0
    start=datetime.now().timestamp(); tent=0
    while True:
        tent+=1
        try:
            amb.logger.info("Goto tentativa %d: %s", tent, url)
            page.goto(url,wait_until="domcontentloaded")
            amb.logger.info("URL atual: %s", page.url)
            return
        except Exception as e:
            if datetime.now().timestamp()-start>=end:
                amb.logger.error("Timeout goto: %s", e, exc_info=True); raise
            amb.logger.warning("Retry goto: %s", e)

def immortal_click(amb:Ambiente,page:Page,spec:Any)->None:
    end=(int(os.getenv("PLAYWRIGHT_TIMEOUT_MS","60000")))/1000.0
    start=datetime.now().timestamp(); tent=0; loc=locator_from(page,spec)
    while True:
        tent+=1
        try:
            amb.logger.info("Click tentativa %d", tent); loc.click(); return
        except Exception as e:
            if datetime.now().timestamp()-start>=end:
                amb.logger.error("Timeout click: %s", e, exc_info=True); raise
            amb.logger.warning("Retry click: %s", e)

def wait_visible(page:Page,spec:Any)->None:
    locator_from(page,spec).wait_for(state="visible")

def criar_contexto(amb:Ambiente,pw)->Tuple[BrowserContext,Page]:
    engine={"chromium":pw.chromium,"firefox":pw.firefox,"webkit":pw.webkit}.get("chromium",pw.chromium)
    browser=engine.launch(headless=HEADLESS)
    context=browser.new_context(accept_downloads=True,viewport={"width":1920,"height":1080})
    page=context.new_page(); page.set_default_timeout(int(os.getenv("PLAYWRIGHT_TIMEOUT_MS","60000")))
    return context,page

def safe_prepare_dir(amb:Ambiente,dirpath:Path,label:str)->None:
    amb.logger.info("Preparando %s: %s", label, dirpath)
    try:
        if dirpath.exists():
            try:
                backup=dirpath.with_name(f"{dirpath.name}__old_{amb.run_ts}")
                dirpath.rename(backup)
                amb.logger.info("%s renomeado para: %s", label, backup)
            except Exception:
                for root,dirs,files in os.walk(dirpath,topdown=False):
                    for name in files:
                        try: Path(root,name).unlink(missing_ok=True)
                        except Exception: pass
                    for name in dirs:
                        try: Path(root,name).rmdir()
                        except Exception: pass
        dirpath.mkdir(parents=True,exist_ok=True)
    except Exception:
        amb.logger.error("Falha ao preparar %s: %s", label, dirpath, exc_info=True); raise

def baixar_dados(amb:Ambiente,data_corte:str)->pl.DataFrame:
    safe_prepare_dir(amb,amb.caminho_input,"arquivos_input")
    try:
        datetime.strptime(data_corte,"%Y-%m-%d")
    except ValueError:
        amb.logger.error("Data inválida: %s", data_corte); raise
    amb.logger.info("BAIXAR_DADOS | data_corte=%s", data_corte)
    sql_corte=f"SELECT Data_Vencimento FROM `{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_DATA_CORTE_FATURAS` WHERE DATA_GERACAO_ROBO = '{data_corte}'"
    amb.last_sql_corte=sql_corte
    corte_df=bq_query_rest(amb,sql_corte,project_id=BQ_PROJECT_ID,location=BQ_LOCATION)
    amb.last_rows_corte=int(corte_df.height or 0)
    if corte_df.height==0:
        amb.last_vencimento=None
        amb.logger.info("Sem registros de corte para %s", data_corte)
        return pl.DataFrame()
    try:
        venc=str(corte_df["Data_Vencimento"].cast(pl.Utf8,strict=False).str.strip_chars().to_list()[0])
    except Exception:
        venc=str(corte_df["Data_Vencimento"].to_list()[0])
    amb.last_vencimento=venc
    amb.logger.info("VENCIMENTO: %s", venc)
    sql_parcela=f"SELECT ACCOUNT_ID, CAMPAIGN_ID FROM `{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_PUBLICO_PARCELAMENTO_FATURA_PF` WHERE DT_VENCIMENTOCOBRANCA = '{venc}'"
    amb.last_sql_parcela=sql_parcela
    result=bq_query_rest(amb,sql_parcela,project_id=BQ_PROJECT_ID,location=BQ_LOCATION)
    amb.last_rows_parcela=int(result.height or 0)
    if result.height==0 or "CAMPAIGN_ID" not in result.columns:
        amb.logger.info("Público zero linhas para %s", venc)
        return pl.DataFrame()
    df=(result.unique().with_columns(pl.col("CAMPAIGN_ID").cast(pl.Int64,strict=False)).drop_nulls("CAMPAIGN_ID"))
    parquet_path=amb.caminho_input/"base.parquet"
    df.write_parquet(parquet_path)
    pdf=df.to_pandas()
    for idx,(cid,grp) in enumerate(pdf.groupby("CAMPAIGN_ID"),start=1):
        path=amb.caminho_input/f"{int(cid)}.csv"
        grp[["ACCOUNT_ID"]].to_csv(path,index=False,header=False)
        amb.logger.info("[%d] CSV salvo: %s | linhas=%d", idx, path.name, len(grp))
    try:
        total_csv=0
        for f in amb.caminho_input.glob("*.csv"):
            try:
                total_csv+=sum(1 for _ in open(f,"r",encoding="utf-8",errors="ignore"))
            except Exception:
                pass
        if total_csv!=int(df.height or 0):
            amb.logger.warning("Inconsistência contagem: parquet=%d csvs=%d", df.height, total_csv)
    except Exception:
        pass
    amb.logger.info("BAIXAR_DADOS OK | linhas=%d campanhas=%d", df.height, int(pdf["CAMPAIGN_ID"].nunique()))
    return df

def rodar_campanha(amb:Ambiente,arquivo:str,cid:str,rd:Rundeck)->tuple[str,Optional[str]]:
    amb.logger.info("Rodando campanha %s", cid)
    p=Path(arquivo) if arquivo else None
    if not p or not p.exists():
        amb.logger.error("Arquivo da campanha %s não encontrado: %s", cid, arquivo)
        return "FALHA",None
    tent=0
    while True:
        tent+=1
        amb.logger.info("Tentativa rodada campanha %s | tentativa=%d", cid, tent)
        status,logs=rd.rodar_job(
            [{"campo":"CAMPAIGN_ID","valor":cid}],
            "https://tasks.corp/project/attfincards/job/show/bcd7569b-ddf2-4a4b-8561-5f0b6926175c",
            arquivo
        )
        amb.logger.info("Status campanha %s: %s", cid, status)
        if "SUCCEEDED" in str(status).upper():
            return status,logs

def remover_campanhas(amb:Ambiente,arquivo:str,rd:Rundeck)->None:
    amb.logger.info("Remoção campanhas")
    p=Path(arquivo) if arquivo else None
    if not p or not p.exists():
        amb.logger.info("Arquivo remoção inexistente")
        return
    for _ in range(50):
        status,_=rd.rodar_job([],"https://tasks.corp/project/corecardstax/job/show/6c0f32f3-317f-40de-9d2f-7ac76387f821",arquivo)
        amb.logger.info("Status remoção: %s", status)
        if "SUCCEEDED" in str(status).upper():
            return

def vincular_campanhas(amb:Ambiente,baixar:bool,data_corte:Optional[str])->tuple[int,int,Optional[Path],dict]:
    if not garantir_outlook_aberto(amb):
        amb.logger.warning("Outlook poderá falhar")
    if not amb.cred_user or not amb.cred_pass:
        amb.logger.error("Credenciais ausentes no Dollynho")
        return RET_FALHA,0,None,{"data_corte":data_corte,"vencimento":amb.last_vencimento,"bq_rows_corte":amb.last_rows_corte,"bq_rows_parcela":amb.last_rows_parcela,"campanhas_total":0,"campanhas_ok":0,"campanhas_ko":0,"linhas_persistidas":0,"tabela_destino":f"{BQ_PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.VincularCampanhas"}
    df=pl.DataFrame()
    try:
        if baixar:
            df=baixar_dados(amb,data_corte or date.today().isoformat())
        else:
            p=amb.caminho_input/"base.parquet"
            if p.exists(): df=pl.read_parquet(p)
        amb.logger.info("Dados prontos: %d linhas", df.height)
    except Exception:
        amb.logger.error("Erro em baixar_dados", exc_info=True)
        return RET_FALHA,0,None,{"data_corte":data_corte,"vencimento":amb.last_vencimento,"bq_rows_corte":amb.last_rows_corte,"bq_rows_parcela":amb.last_rows_parcela,"campanhas_total":0,"campanhas_ok":0,"campanhas_ko":0,"linhas_persistidas":0,"tabela_destino":f"{BQ_PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.VincularCampanhas"}
    if df.height==0:
        return RET_SEM_DADOS,0,None,{"data_corte":data_corte,"vencimento":amb.last_vencimento,"bq_rows_corte":amb.last_rows_corte,"bq_rows_parcela":0,"campanhas_total":0,"campanhas_ok":0,"campanhas_ko":0,"linhas_persistidas":0,"tabela_destino":f"{BQ_PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.VincularCampanhas"}
    anexos=[]
    resultados=[]; ok=0; ko=0
    try:
        with sync_playwright() as pw:
            context,page=criar_contexto(amb,pw)
            try:
                rd=Rundeck(amb,page)
                campanhas=df["CAMPAIGN_ID"].unique().drop_nulls().to_list()
                amb.logger.info("Campanhas: %s", campanhas)
                for cid in campanhas:
                    path_csv=amb.caminho_input/f"{cid}.csv"
                    status,logs=rodar_campanha(amb,str(path_csv),str(cid),rd)
                    resultados.append({"campaign_id":str(cid),"status":str(status).upper(),"log":logs or ""})
                    if str(status).upper()=="SUCCEEDED": ok+=1
                    else: ko+=1
            finally:
                try: context.close()
                except Exception: pass
    except Exception:
        amb.logger.error("Falha Playwright", exc_info=True)
        return RET_FALHA,0,None,{"data_corte":data_corte,"vencimento":amb.last_vencimento,"bq_rows_corte":amb.last_rows_corte,"bq_rows_parcela":int(df.height or 0),"campanhas_total":int(len(df.select(pl.col("CAMPAIGN_ID").unique()).to_series())),"campanhas_ok":ok,"campanhas_ko":ko,"linhas_persistidas":0,"tabela_destino":f"{BQ_PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.VincularCampanhas"}
    resultado=amb.caminho_artefatos/f"resultado_{amb.run_ts}.xlsx"
    amb.caminho_artefatos.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(resultados).to_excel(resultado,index=False)
    if resultado.exists(): anexos.append(resultado)
    try:
        token=_get_access_token(); ca=os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
        headers={"Authorization":f"Bearer {token}","Content-Type":"application/json"}
        project_id=BQ_PROJECT_ID; dataset_id="ADMINISTRACAO_CELULA_PYTHON"; table_id="VincularCampanhas"
        ds_url=f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets"
        ds_payload={"datasetReference":{"projectId":project_id,"datasetId":dataset_id},"location":BQ_LOCATION}
        rds=requests.post(ds_url,headers=headers,data=json.dumps(ds_payload),timeout=30,verify=ca if ca else True)
        if rds.status_code not in (200,409): raise RuntimeError("Falha garantir dataset")
        tb_url=f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables"
        schema={"fields":[{"name":"DATA_GERACAO_ROBO","type":"DATE","mode":"REQUIRED"},{"name":"DT_VENCIMENTOCOBRANCA","type":"DATE","mode":"REQUIRED"},{"name":"CAMPAIGN_ID","type":"INT64","mode":"REQUIRED"},{"name":"ACCOUNT_ID","type":"STRING","mode":"REQUIRED"},{"name":"DT_COLETA","type":"TIMESTAMP","mode":"REQUIRED"},{"name":"JOB_STATUS","type":"STRING","mode":"NULLABLE"},{"name":"RUN_ID","type":"STRING","mode":"REQUIRED"},{"name":"SCRIPT","type":"STRING","mode":"REQUIRED"}]}
        rtb=requests.post(tb_url,headers=headers,data=json.dumps({"tableReference":{"projectId":project_id,"datasetId":dataset_id,"tableId":table_id},"schema":schema}),timeout=30,verify=ca if ca else True)
        if rtb.status_code not in (200,409): raise RuntimeError("Falha garantir tabela")
        status_map={str(x["campaign_id"]):str(x["status"]) for x in resultados}
        ts_utc=datetime.utcnow().replace(microsecond=0).isoformat()+"Z"
        df_out=(df.select(["ACCOUNT_ID","CAMPAIGN_ID"]).with_columns([pl.lit(data_corte or date.today().isoformat()).alias("DATA_GERACAO_ROBO"),pl.lit(amb.last_vencimento or "").alias("DT_VENCIMENTOCOBRANCA"),pl.col("CAMPAIGN_ID").cast(pl.Int64,strict=False),pl.col("ACCOUNT_ID").cast(pl.Utf8,strict=False),pl.lit(ts_utc).alias("DT_COLETA"),pl.col("CAMPAIGN_ID").map_elements(lambda c: status_map.get(str(c),"UNKNOWN")).alias("JOB_STATUS"),pl.lit(amb.run_ts).alias("RUN_ID"),pl.lit(NOME_SCRIPT).alias("SCRIPT")]).select(["DATA_GERACAO_ROBO","DT_VENCIMENTOCOBRANCA","CAMPAIGN_ID","ACCOUNT_ID","DT_COLETA","JOB_STATUS","RUN_ID","SCRIPT"]))
        rows=df_out.to_dicts(); total_rows=len(rows)
        ins_url=f"https://bigquery.googleapis.com/bigquery/v2/projects/{project_id}/datasets/{dataset_id}/tables/{table_id}/insertAll"
        batch=int(os.getenv("BQ_INSERT_BATCH","1000")); inserted=0
        for i in range(0,total_rows,batch):
            chunk=rows[i:i+batch]
            payload={"kind":"bigquery#tableDataInsertAllRequest","skipInvalidRows":True,"ignoreUnknownValues":False,"rows":[{"json":r} for r in chunk]}
            rr=requests.post(ins_url,headers=headers,data=json.dumps(payload),timeout=120,verify=ca if ca else True)
            if rr.status_code!=200:
                amb.logger.error("insertAll HTTP %s: %s", rr.status_code, rr.text)
                continue
            resp=rr.json(); errs=resp.get("insertErrors") or []
            ok_rows=len(chunk)-(len(errs) if errs else 0); inserted+=ok_rows
            amb.logger.info("insertAll %d..%d ok=%d", i, i+len(chunk)-1, ok_rows)
    except Exception:
        amb.logger.error("Falha persistência BigQuery", exc_info=True)
        inserted=0
    resumo={"data_corte":data_corte,"vencimento":amb.last_vencimento,"bq_rows_corte":amb.last_rows_corte,"bq_rows_parcela":int(df.height or 0),"campanhas_total":int(len(df.select(pl.col("CAMPAIGN_ID").unique()).to_series())),"campanhas_ok":ok,"campanhas_ko":ko,"linhas_persistidas":int(inserted),"tabela_destino":f"{BQ_PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.VincularCampanhas"}
    return RET_SUCESSO,len(resultados),resultado if resultado.exists() else None,resumo

def publicar_metricas(amb:Ambiente,status:str,tempo_hms:str,tabela_ref:str)->None:
    try:
        cli=AutomacoesExecClient(logger=amb.logger,log_file=amb.log_file_path)
        cli.publicar(nome_automacao=NOME_AUTOMACAO or "",metodo_automacao=ARQUIVO_ATUAL.stem,status=status,tempo_exec=tempo_hms,data_exec=amb.data_exec,hora_exec=amb.hora_exec,usuario=amb.usuario,log_path=str(amb.log_file_path),tabela_referencia=tabela_ref,observacao=amb.observacao,modo_execucao=amb.modo_execucao,send_email=False)
    except Exception:
        amb.logger.error("Falha publicar métricas", exc_info=True)

def mover_artefatos(amb:Ambiente,arquivos:List[Path])->None:
    try:
        amb.caminho_artefatos.mkdir(parents=True,exist_ok=True)
        for p in arquivos:
            if p and p.exists() and p.is_file():
                destino=amb.caminho_artefatos/p.name
                try:
                    if destino.exists(): destino=destino.with_name(f"{destino.stem}_{amb.run_ts}{destino.suffix}")
                    shutil.move(str(p),str(destino))
                except Exception:
                    pass
    except Exception:
        amb.logger.warning("Falha ao mover artefatos", exc_info=True)

def main()->int:
    amb=Ambiente()
    amb.logger.info("INICIO | script=%s | automacao=%s", NOME_SCRIPT, NOME_AUTOMACAO or "-")
    servidor=is_execucao_servidor()
    if servidor:
        amb.modo_execucao="AUTO"; amb.observacao="AUTO"; amb.usuario=f"{getpass.getuser()}@c6bank.com"
        amb.logger.info("Execução identificada como servidor")
    else:
        amb.logger.info("Execução identificada como manual")
        m,o,u=coletar_contexto_manual(amb); amb.modo_execucao=m; amb.observacao=o; amb.usuario=u
    parser=argparse.ArgumentParser(add_help=False)
    parser.add_argument("command",nargs="?",choices=["vincular"],default="vincular")
    parser.add_argument("param",nargs="?")
    parser.add_argument("--no-baixar",action="store_false",dest="baixar")
    args,unknown=parser.parse_known_args()
    data_corte=args.param
    tempo=""
    try:
        status_code,total,resultado,resumo=vincular_campanhas(amb,baixar=args.baixar,data_corte=data_corte)
        tempo=amb.tempo_exec_hms()
        tabelas=[f"{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_DATA_CORTE_FATURAS",f"{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_PUBLICO_PARCELAMENTO_FATURA_PF",f"{BQ_PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.VincularCampanhas"]
        if status_code==RET_SEM_DADOS:
            enviar_email(amb,"SEM DADOS PARA PROCESSAR",tempo,tabelas,0,[resultado] if resultado else [],resumo)
            publicar_metricas(amb,"SEM DADOS PARA PROCESSAR",tempo,",".join(tabelas))
            mover_artefatos(amb,[resultado] if resultado else [])
            return RET_SEM_DADOS
        if status_code==RET_FALHA:
            enviar_email(amb,"FALHA",tempo,tabelas,0,[resultado] if resultado else [],resumo)
            publicar_metricas(amb,"FALHA",tempo,",".join(tabelas))
            mover_artefatos(amb,[resultado] if resultado else [])
            return RET_FALHA
        enviar_email(amb,"SUCESSO",tempo,tabelas,total,[resultado] if resultado else [],resumo)
        publicar_metricas(amb,"SUCESSO",tempo,",".join(tabelas))
        mover_artefatos(amb,[resultado] if resultado else [])
        return RET_SUCESSO
    except Exception:
        amb.logger.exception("Falha não tratada")
        try:
            tempo=amb.tempo_exec_hms()
            tabelas=[f"{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_DATA_CORTE_FATURAS",f"{BQ_SOURCE_PROJECT_ID}.SHARED_OPS.TB_PUBLICO_PARCELAMENTO_FATURA_PF",f"{BQ_PROJECT_ID}.ADMINISTRACAO_CELULA_PYTHON.VincularCampanhas"]
            enviar_email(amb,"FALHA",tempo,tabelas,0,[],None)
            publicar_metricas(amb,"FALHA",tempo,",".join(tabelas))
        except Exception:
            pass
        return RET_FALHA
    finally:
        try:
            if amb.log_file_path.exists():
                mover_artefatos(amb,[amb.log_file_path])
        except Exception:
            pass

if __name__=="__main__":
    sys.exit(main())
