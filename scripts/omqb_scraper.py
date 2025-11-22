#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
OM-QB job (EC2-ready) — integra com a saída do betfair-scraper:
- Lê o CSV de jogos (local ou S3) gerado pelo betfair-scraper.
- Faz login no OM-QB e preenche dois formulários:
    1) Back Model
    2) Indicators Model
- Extrai os textos de resultado (alerta) das duas páginas.
- Salva CSV enriquecido com colunas Back_Model e Indicators_Model.
- Usa cache por tripla de odds (H/D/A) para evitar reprocesso.

ENV principais:
  AWS_DEFAULT_REGION=sa-east-1
  S3_OMQB_BUCKET=omqb-scraper                # opcional; se ausente, usa arquivos locais
  S3_INPUT_BUCKET=betfair-scraper
  INPUT_DATE=YYYY-MM-DD                    # ou INPUT_KEY=outputs/<data>/jogos.csv
  OUTPUT_PREFIX=omqb-outputs               # default
  MAX_ROWS=30                              # p/ testes
  WAIT_SECONDS=15                          # timeout de waits

# Login / sessão
  OMQB_LOGIN_URL=https://www.om-qb.com/accounts/login/   # (do seu notebook)
  OMQB_USERNAME=ssm:///betfair/omqb/username             # ou valor literal (não recomendado)
  OMQB_PASSWORD=ssm:///betfair/omqb/password
  # cookies persistidos (opcional, evita login todo run):
  COOKIE_JAR_S3=omqb-cache/session_cookies.json          # salvo no mesmo bucket
  COOKIE_JAR_LOCAL=./session_cookies.json

# URLs e seletores (derivados do seu notebook — parametrizáveis):
  OMQB_BACK_URL=https://www.om-qb.com/formularios/trading-esportivo/back-model
  OMQB_INDIC_URL=https://www.om-qb.com/formularios/trading-esportivo/indicators-model
  ODDS_HOME_NAME=home-odds
  ODDS_AWAY_NAME=away-odds
  ODDS_DRAW_NAME=draw-odds
  SUBMIT_BTN_XPATH=//button[@class="btn btn-primary"]
  ALERT_XPATH=//div[@class="alert alert-info col-12 col-md-6 col-lg-6"]

# Cache por odds:
  ENABLE_CACHE=1
  CACHE_BUCKET=omqb-scraper
  CACHE_KEY=cache/omqb-cache.csv
  CACHE_LOCAL=./omqb-cache.csv
  CACHE_ROUND_DECIMALS=2
"""

import os
import io
import re
import sys
import json
import base64
import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, Tuple
import threading, subprocess, signal, time
import concurrent.futures as cf
from dotenv import load_dotenv

import pandas as pd

# AWS (opcional)
try:
    import boto3
except Exception:
    boto3 = None

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common import TimeoutException, WebDriverException


# ---------- Logging ----------
LOG = logging.getLogger("omqb-job")
logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ---------- Env ----------
load_dotenv("/opt/omqb/secrets.env")


AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "sa-east-1")

S3_OMQB_BUCKET = os.getenv("S3_OMQB_BUCKET", "omqb-scraper").strip()
S3_INPUT_BUCKET = os.getenv("S3_INPUT_BUCKET", "betfair-scraper").strip()
OUTPUT_PREFIX = os.getenv("OUTPUT_PREFIX", "outputs").strip()
INPUT_KEY = os.getenv("INPUT_KEY", "outputs").strip()
INPUT_DATE = os.getenv("INPUT_DATE", "").strip()
MAX_ROWS = 300
WAIT_SECONDS = int(os.getenv("WAIT_SECONDS", "15"))
MAX_ATTEMPTS = int(os.getenv("MAX_ATTEMPTS", "5"))
ATTEMPT_TIMEOUT = int(os.getenv("ATTEMPT_TIMEOUT", "25"))  # segs por tentativa
STOP_TOPIC_ARN = os.getenv(
    "STOP_TOPIC_ARN", "arn:aws:sns:sa-east-1:232219615015:ec2-stop-topic-omqb-scraper"
)
NOTIFY_SNS_ARN = os.getenv(
    "NOTIFY_SNS_ARN", "arn:aws:sns:sa-east-1:232219615015:omqb-notify-email-html"
).strip()  # e-mail

# Cache
ENABLE_CACHE = True
CACHE_BUCKET = os.getenv("CACHE_BUCKET", "").strip() or S3_OMQB_BUCKET
CACHE_KEY = os.getenv("CACHE_KEY", "cache/omqb-cache.csv").strip()
CACHE_LOCAL = os.getenv("CACHE_LOCAL", "omqb-cache.csv").strip()
CACHE_ROUND_DECIMALS = int(os.getenv("CACHE_ROUND_DECIMALS", "2"))

# Chrome
CHROME_BIN = os.getenv("CHROME_BIN", "").strip()  # vazio => Selenium Manager resolve

# URLs/Seletores conforme seu notebook
OMQB_LOGIN_URL = os.getenv(
    "OMQB_LOGIN_URL", "https://www.om-qb.com/accounts/login/"
).strip()
OMQB_BACK_URL = os.getenv(
    "OMQB_BACK_URL", "https://www.om-qb.com/formularios/trading-esportivo/back-model"
).strip()
OMQB_INDIC_URL = os.getenv(
    "OMQB_INDIC_URL",
    "https://www.om-qb.com/formularios/trading-esportivo/indicators-model",
).strip()

ODDS_HOME_NAME = os.getenv("ODDS_HOME_NAME", "home-odds")
ODDS_AWAY_NAME = os.getenv("ODDS_AWAY_NAME", "away-odds")
ODDS_DRAW_NAME = os.getenv("ODDS_DRAW_NAME", "draw-odds")
LOGIN_BTN_XPATH = os.getenv("LOGIN_BTN_XPATH", '//button[@type="submit"]')
SUBMIT_BTN_XPATH = os.getenv("SUBMIT_BTN_XPATH", '//button[@class="btn btn-primary"]')
ALERT_XPATH = os.getenv(
    "ALERT_XPATH", '//div[@class="alert alert-info col-12 col-md-6 col-lg-6"]'
)


# Login creds (use SSM de preferência)
OMQB_USERNAME = os.getenv("OMQB_USERNAME").strip()
OMQB_PASSWORD = os.getenv("OMQB_PASSWORD").strip()

COOKIE_JAR_LOCAL = os.getenv("COOKIE_JAR_LOCAL", "").strip()


# ---------- Helpers ----------
def tz_brazil_now():
    return datetime.now(timezone(timedelta(hours=-3)))


dt_brasil = tz_brazil_now().strftime("%Y-%m-%d")


def default_input_key():
    dt = INPUT_DATE or tz_brazil_now().strftime("%Y-%m-%d")
    return f"outputs/{dt}/jogos.csv"


def _num(s):
    try:
        return float(str(s).replace(",", "."))
    except Exception:
        return None


# ---------- S3 I/O ----------
def s3_read_csv(bucket, key) -> pd.DataFrame:
    if not boto3:
        raise RuntimeError("boto3 não instalado (S3).")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    LOG.info(f"Lendo CSV: s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(obj["Body"].read()))


def s3_write_csv(bucket, key, df: pd.DataFrame):
    if not boto3:
        raise RuntimeError("boto3 não instalado (S3).")
    s3 = boto3.client("s3", region_name=AWS_REGION)
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    LOG.info(f"Gravando CSV: s3://{bucket}/{key}")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


def s3_write_text(bucket, key, text, content_type="text/html"):
    import boto3, io

    s3 = boto3.client("s3", region_name=AWS_REGION)
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8"),
        ContentType=content_type,
        CacheControl="max-age=300",
    )


def make_presigned_url(bucket: str, key: str, expires=86400) -> str:
    s3 = boto3.client("s3", region_name=AWS_REGION)
    return s3.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires,
    )


# ---------- SNS ---------------


def notify_shutdown(topic_arn: str, message: str = "SCRAPER_DONE"):
    sns = boto3.client("sns", region_name=AWS_REGION)
    sns.publish(TopicArn=topic_arn, Message=message, Subject="betfair-scraper")


def sns_publish(topic_arn: str, subject: str, message: str) -> bool:
    if not topic_arn:
        logging.info("SNS topic ARN não configurado; pulando publish.")
        return False
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        sns.publish(TopicArn=topic_arn, Subject=subject, Message=message)
        logging.info(f"SNS publish OK para {topic_arn}")
        return True
    except Exception as e:
        logging.error(f"Falha SNS publish: {e}")
        return False


def publish_email(html_key, len_df, cache_hit, old_null_cache_cols, new_null_cache_cols):
    # Suponha que html_key tenha sido definido no upload do HTML:
    try:
        presigned = make_presigned_url(S3_OMQB_BUCKET, html_key)
        subject = f"Apostas — Link do Relatório"
        msg = (
            "Job finalizado com sucesso.\n\n"
            f"Relatório (link temporário):\n{presigned}\n\n"
            f"Bucket: {S3_OMQB_BUCKET}\nKey: {html_key}\n\n"
            f"Total de linhas analisadas: {len_df}\n"
            f"Total de cache hit: {cache_hit}"
            f"Linhas nulas antes (Back Model): {old_null_cache_cols}\n"
            f"Linhas nulas agora (Back Model): {new_null_cache_cols}\n"
            f"Total new cache stored: {new_null_cache_cols-old_null_cache_cols}"
        )
        sns_publish(NOTIFY_SNS_ARN, subject, msg)
    except Exception as e:
        LOG.error(f"Falha ao gerar/publish URL assinada: {e}")


# ---------- Selenium ----------
def build_driver():
    opts = Options()
    opts.add_argument("--headless=new")  # evite xvfb se puder
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1280,1024")
    opts.page_load_strategy = "eager"  # não espere tudo
    drv = webdriver.Chrome(options=opts)
    drv.set_page_load_timeout(20)  # de 60 → 20
    drv.set_script_timeout(12)
    return drv


def wait_css(drv, css, timeout=None):
    return WebDriverWait(drv, timeout or WAIT_SECONDS).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, css))
    )


def wait_xpath(drv, xpath, timeout=None):
    return WebDriverWait(drv, timeout or WAIT_SECONDS).until(
        EC.presence_of_element_located((By.XPATH, xpath))
    )


def resilient_quit(driver, hard_kill_after=3):
    """
    Tenta driver.quit() mas não deixa travar a thread principal.
    Se não encerrar em 'hard_kill_after' segundos, mata o processo do chromedriver/chrome.
    """
    done = {"ok": False}

    def _q():
        try:
            driver.quit()
            done["ok"] = True
        except Exception:
            pass

    t = threading.Thread(target=_q, daemon=True)
    t.start()
    t.join(timeout=hard_kill_after)
    if done["ok"]:
        return
    # hard kill: tenta encerrar o processo do serviço
    try:
        svc = getattr(driver, "service", None)
        proc = getattr(svc, "process", None)
        if proc and proc.poll() is None:
            try:
                proc.terminate()
                time.sleep(0.5)
                if proc.poll() is None:
                    proc.kill()
            except Exception:
                pass
    except Exception:
        pass
    # plano C (best-effort): mata qualquer resquício do chromedriver
    try:
        subprocess.run(["pkill", "-f", "chromedriver"], check=False)
    except Exception:
        pass


# ---------- Login / Cookies ----------


def login_if_needed(drv):
    user = OMQB_USERNAME
    pwd = OMQB_PASSWORD
    if not user or not pwd:
        raise RuntimeError("Defina OMQB_USERNAME e OMQB_PASSWORD (ou SSM).")

    LOG.info("Efetuando login no OM-QB...")
    drv.get(OMQB_LOGIN_URL)

    # Campos do login (DOM do site de login do OM-QB)
    # Pelo seu notebook, ids: 'id_login' e 'id_password'
    u = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.presence_of_element_located((By.ID, "id_login"))
    )
    p = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.presence_of_element_located((By.ID, "id_password"))
    )
    u.clear()
    u.send_keys(user)
    p.clear()
    p.send_keys(pwd)
    btn = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.element_to_be_clickable((By.XPATH, LOGIN_BTN_XPATH))
    )
    btn.click()

    # Verifica pós login carregando a página do Back Model
    WebDriverWait(drv, WAIT_SECONDS).until(EC.title_contains("OM Quant Betting"))
    drv.get(OMQB_BACK_URL)
    wait_xpath(drv, SUBMIT_BTN_XPATH, timeout=WAIT_SECONDS)

    LOG.info("Login OK")


# ---------- Fluxo OM-QB (2 páginas) ----------
def _fill_and_submit_form(drv, url, odd_H, odd_D, odd_A) -> str:
    drv.get(url)
    # inputs pelo NAME conforme notebook
    home = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.presence_of_element_located((By.NAME, ODDS_HOME_NAME))
    )
    away = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.presence_of_element_located((By.NAME, ODDS_AWAY_NAME))
    )
    draw = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.presence_of_element_located((By.NAME, ODDS_DRAW_NAME))
    )

    for el, val in ((home, odd_H), (away, odd_A), (draw, odd_D)):
        el.clear()
        el.send_keys(str(val))

    btn = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.element_to_be_clickable((By.XPATH, SUBMIT_BTN_XPATH))
    )
    btn.click()

    # lê o bloco de alerta (texto)
    alert = WebDriverWait(drv, WAIT_SECONDS).until(
        EC.presence_of_element_located((By.XPATH, ALERT_XPATH))
    )
    return alert.text.strip()


def run_omqb_for_odds(drv, odd_H, odd_D, odd_A) -> tuple[str, str]:
    LOG.info(f"Iniciando Back Model")
    back_text = _fill_and_submit_form(drv, OMQB_BACK_URL, odd_H, odd_D, odd_A)
    LOG.info(f"Back Model: OK")
    LOG.info(f"Iniciando Indicators Model")
    indic_text = _fill_and_submit_form(drv, OMQB_INDIC_URL, odd_H, odd_D, odd_A)
    LOG.info(f"Indicators Model: OK")
    return back_text, indic_text


def _do_one_attempt(h, d, a):
    drv = build_driver()
    try:
        login_if_needed(drv)
        back_val, indic_val = run_omqb_for_odds(drv, h, d, a)
        # considere inválido = falha (para retry)
        if not (_is_filled(back_val) and _is_filled(indic_val)):
            raise RuntimeError(
                f"Invalid scrape result (Back={back_val}, Indicators={indic_val})"
            )
        return back_val, indic_val
    finally:
        try:
            resilient_quit(drv)
        except Exception:
            pass


def process_with_retry(
    h, d, a, max_attempts=MAX_ATTEMPTS, attempt_timeout=ATTEMPT_TIMEOUT
):
    last_exc = None
    for attempt in range(1, max_attempts + 1):
        LOG.info(f"Scrape attempt {attempt}/{max_attempts} for {h}-{d}-{a}")
        with cf.ThreadPoolExecutor(max_workers=1) as ex:
            fut = ex.submit(_do_one_attempt, h, d, a)
            try:
                return fut.result(timeout=attempt_timeout)
            except cf.TimeoutError as e:
                last_exc = e
                LOG.error(
                    f"Attempt {attempt}: TIMEOUT after {attempt_timeout}s → will retry with fresh driver"
                )
                # 🔧 limpeza agressiva (evita thread zumbi segurar recursos/redes)
                try:
                    subprocess.run(["pkill", "-f", "chromedriver"], check=False)
                    subprocess.run(["pkill", "-f", "chrome"], check=False)
                    time.sleep(0.5)
                except Exception:
                    pass
            except (TimeoutException, WebDriverException, Exception) as e:
                last_exc = e
                LOG.warning(
                    f"Attempt {attempt} failed: {e.__class__.__name__}: {e} → will retry"
                )
    raise last_exc if last_exc else RuntimeError("Scraping failed without details")


# ---------- Cache ----------
def normalize_odds(h, d, a, nd=CACHE_ROUND_DECIMALS):
    try:
        h = round(float(h), nd)
        d = round(float(d), nd)
        a = round(float(a), nd)
    except Exception:
        return None, None, None, ""
    key = f"{h:.{nd}f}-{d:.{nd}f}-{a:.{nd}f}"
    return h, d, a, key


CACHE_COLS = [
    "Odd_Back_H",
    "Odd_Back_D",
    "Odd_Back_A",
    "Back_Model",
    "Indicators_Model",
    "KeyOdds",
]


def _is_filled(x) -> bool:
    # Considera preenchido somente se não for NaN/None/vazio/"nan"/"null"/"none"
    try:
        if not pd.notna(x):
            return False
        s = str(x).strip().lower()
        return s not in ("", "nan", "null", "none")
    except Exception:
        return False


def _count_valid_pairs(df) -> int:
    # Conta linhas do cache com AMBOS os campos preenchidos
    if df is None or df.empty:
        return 0
    return int(
        (
            df["Back_Model"].apply(_is_filled)
            & df["Indicators_Model"].apply(_is_filled)
        ).sum()
    )


def load_cache_df() -> pd.DataFrame:
    if not ENABLE_CACHE:
        return pd.DataFrame(columns=CACHE_COLS)
    if S3_OMQB_BUCKET and CACHE_BUCKET and boto3:
        try:
            df = s3_read_csv(CACHE_BUCKET, CACHE_KEY)
            for c in ["Odd_Back_H", "Odd_Back_D", "Odd_Back_A"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            return df
        except Exception as e:
            LOG.warning(f"Cache S3 indisponível: {e}")
            return pd.DataFrame(columns=CACHE_COLS)
    else:
        if os.path.exists(CACHE_LOCAL):
            df = pd.read_csv(CACHE_LOCAL)
            for c in ["Odd_Back_H", "Odd_Back_D", "Odd_Back_A"]:
                df[c] = pd.to_numeric(df[c], errors="coerce")
            return df
        return pd.DataFrame(columns=CACHE_COLS)


def save_cache_df(df: pd.DataFrame):
    if not ENABLE_CACHE:
        return
    df = df[CACHE_COLS].copy()
    if S3_OMQB_BUCKET and boto3:
        s3_write_csv(S3_OMQB_BUCKET, CACHE_KEY, df)
    else:
        df.to_csv(CACHE_LOCAL, index=False)
        LOG.info(f"Cache salvo local: {CACHE_LOCAL}")


def cache_lookup(df_cache: pd.DataFrame, keyodds):
    LOG.info(f"verificando cache para {keyodds}")
    if not keyodds or df_cache.empty:
        LOG.info(f"df_cache.empty: {df.cache.empty}")
        return None

    r = df_cache[df_cache["KeyOdds"] == keyodds]

    if r.shape[0] == 0:
        return None
    LOG.info(f"Existe uma linha para {keyodds}, verificando se há valores")

    # Se Back/Indicators faltam (NaN/None/""), considere MISS
    back_val = r.get("Back_Model")
    indic_val = r.get("Indicators_Model")

    if back_val.isna().values[0] or indic_val.isna().values[0]:
        LOG.info(f"valores não preenchidos")
        return None

    LOG.info("Valores Existem. Aproveitando cache")
    return {
        "Back_Model": r.get("Back_Model").values[0],
        "Indicators_Model": r.get("Indicators_Model").values[0],
        "KeyOdds": r.get("KeyOdds").values[0],
    }


def cache_upsert(df_cache: pd.DataFrame, keyodds, back_val, indic_val) -> pd.DataFrame:
    h, d, a = keyodds.split("-")
    row = {
        "Odd_Back_H": h,
        "Odd_Back_D": d,
        "Odd_Back_A": a,
        "Back_Model": back_val,
        "Indicators_Model": indic_val,
        "KeyOdds": keyodds,
    }
    if df_cache.empty:
        return pd.DataFrame([row], columns=CACHE_COLS)

    else:
        if df_cache.loc[df_cache["KeyOdds"] == keyodds, :].shape[0] == 0:
            df_cache = pd.concat([df_cache, pd.DataFrame([row])], ignore_index=True)
        else:
            df_cache.loc[
                df_cache["KeyOdds"] == keyodds, ["Back_Model", "Indicators_Model"]
            ] = [back_val, indic_val]

    df_cache = df_cache.sort_values(
        by=["Odd_Back_H", "Odd_Back_D", "Odd_Back_A"], ascending=True, ignore_index=True
    )

    LOG.info(f"cache atualizado para {keyodds}")
    return df_cache


# ---------- Input/Output ----------
def load_input_df() -> pd.DataFrame:
    key = default_input_key()
    if S3_INPUT_BUCKET:
        df = s3_read_csv(S3_INPUT_BUCKET, key)

    req = ["Date", "League", "Home", "Away", "Odd_Back_H", "Odd_Back_D", "Odd_Back_A"]
    miss = [c for c in req if c not in df.columns]
    if miss:
        raise ValueError(f"CSV de entrada sem colunas: {miss}")

    for c in ["Odd_Back_H", "Odd_Back_D", "Odd_Back_A"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["Odd_Back_H", "Odd_Back_D", "Odd_Back_A"])

    if MAX_ROWS and MAX_ROWS > 0:
        df = df.head(MAX_ROWS)
    return df


def save_output_df(df_out: pd.DataFrame):
    key = f"{OUTPUT_PREFIX}/{dt_brasil}/omqb_results.csv"
    if S3_OMQB_BUCKET:
        s3_write_csv(S3_OMQB_BUCKET, key, df_out)
    else:
        out_dir = os.getenv("OUTPUT_LOCAL_DIR", ".")
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "omqb_results.csv")
        df_out.to_csv(path, index=False)
        LOG.info(f"Gravou CSV local: {path}")
    LOG.info(f"OK: {key}")


# ----------- HTML --------------


def df_to_html_page(df: pd.DataFrame, title="OMQB Results"):
    # HTML simples, responsivo, sem libs externas
    table_html = df.to_html(index=False, border=0, classes="dataframe")
    return f"""<!doctype html>
<html lang="pt-br">
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<style>
  body {{ font-family: system-ui, -apple-system, Segoe UI, Roboto, sans-serif; margin: 24px; }}
  .wrap {{ max-width: 1200px; margin: 0 auto; }}
  table.dataframe {{ width:100%; border-collapse: collapse; }}
  table.dataframe th, table.dataframe td {{ border: 1px solid #ddd; padding: 8px; font-size: 14px; }}
  table.dataframe th {{ background:#f5f5f5; text-align:left; }}
</style>
<body><div class="wrap">
  <h1>{title}</h1>
  <p>Gerado em {tz_brazil_now().strftime("%Y-%m-%d %H:%M:%S")} BRT</p>
  {table_html}
</div></body>
</html>"""


# ---------- Main ----------
def main():
    LOG.info("Iniciando OM-QB job (cache + login + 2 formulários)...")
    df_in = load_input_df()
    LOG.info(f"Linhas de entrada: {len(df_in)}")

    df_cache = load_cache_df()
    orig_cache_len = len(df_cache)
    orig_valid_pairs = _count_valid_pairs(df_cache)
    old_null_cache_cols = df_cache[df_cache['Back_Model'].isna()].shape[0]
    cache_hits = 0
    rows = []

    # Descobre se haverá MISS no cache para decidir criar driver/login
    need_scrape = False
    if ENABLE_CACHE:
        LOG.info(f"Verificando se precisa de scrape")
        for _, row in df_in.iterrows():
            if not cache_lookup(df_cache, row["KeyOdds"]):
                LOG.info(f"Sem cache para {row['KeyOdds']}, iniciando scrape")
                need_scrape = True
                break

    for idx, row in df_in.iterrows():
        key = row["KeyOdds"]
        h = row["Odd_Back_H"]
        d = row["Odd_Back_D"]
        a = row["Odd_Back_A"]
        if not key:
            LOG.warning(f"[{idx + 1}] odds inválidas; pulando.")
            continue

        LOG.info(f"Verificando cache para {row['KeyOdds']}")
        cached = cache_lookup(df_cache, key) if ENABLE_CACHE else None
        LOG.info(f"{cached}")
        if cached and cached.get("Back_Model") and cached.get("Indicators_Model"):
            LOG.info(f"[{idx + 1}] HIT cache {key}")
            back_val = cached["Back_Model"]
            indic_val = cached["Indicators_Model"]
            cache_hits += 1
        else:
            LOG.info(f"[{idx + 1}] MISS cache {key} → scraping with retry")
            try:
                back_val, indic_val = process_with_retry(h, d, a)
            except Exception as e:
                LOG.error(f"[{idx + 1}] FAILED after retries for {key}: {e}")
                # skip this row; do not write partials
                continue
            if ENABLE_CACHE:
                df_cache = cache_upsert(df_cache, key, back_val, indic_val)

        out_row = dict(row)
        out_row["Back_Model"] = back_val
        out_row["Indicators_Model"] = indic_val
        rows.append(out_row)

    df_out = pd.DataFrame(rows)
    save_output_df(df_out)
    if ENABLE_CACHE:
        save_cache_df(df_cache)
        try:
            new_rows_appended = max(len(df_cache) - orig_cache_len, 0)
            new_pairs_filled = max(_count_valid_pairs(df_cache) - orig_valid_pairs, 0)
        except Exception:
            new_rows_appended = 0
            new_pairs_filled = 0
        LOG.info(
            f"Cache atualizado: {len(df_cache)} linhas "
            f"(novas linhas: {new_rows_appended}; pares preenchidos agora: {new_pairs_filled})."
        )

    LOG.info(f"Concluído. Linhas com resultado: {len(df_out)}")

    new_null_cache_cols = df_cache[df_cache['Back_Model'].isna()].shape[0]

    # Publicar HTML em omqb/site/{data}/index.html
    html_key = f"site/{dt_brasil}/index.html"
    html = df_to_html_page(df_out, title=f"OMQB Results - {dt_brasil}")
    if S3_OMQB_BUCKET:
        s3_write_text(S3_OMQB_BUCKET, html_key, html, content_type="text/html")
        publish_email(html_key, len(df_in), cache_hits, old_null_cache_cols, new_null_cache_cols)
    else:
        os.makedirs("site", exist_ok=True)
        with open(os.path.join("site", "index.html"), "w", encoding="utf-8") as f:
            f.write(html)
        LOG.info("HTML gerado em ./site/index.html")

    if STOP_TOPIC_ARN:
        notify_shutdown(STOP_TOPIC_ARN)


if __name__ == "__main__":
    main()
