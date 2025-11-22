#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
run_scraper_ec2.py
------------------
Script pensado para execução em EC2 Ubuntu com interface (VNC) ou via xvfb-run (display virtual).
- Lê dicionário de ligas (nome -> URL) de um JSON no S3
- Abre cada URL com Selenium (não headless por padrão)
- Extrai jogos e odds (3-way) usando seletores parecidos com os do notebook original
- Filtra por janela de dias (inclui hoje), liquidez mínima e juice máximo
- Exporta CSV para S3 em s3://<bucket>/<prefix>/<YYYY-MM-DD>/jogos.csv

Execução agendada (recomendado):
    xvfb-run -a -s "-screen 0 1280x1024x24" python3 /opt/betfair/run_scraper_ec2.py

Variáveis de ambiente:
    S3_BUCKET           (obrigatória)
    S3_CONFIG_KEY       (padrão: config/links.json)
    S3_OUTPUT_PREFIX    (padrão: outputs)
    AWS_DEFAULT_REGION  (padrão: us-east-1)
    LOOKAHEAD_DAYS      (padrão: 3)
    MIN_LIQUIDEZ        (padrão: 50)   # em BRL (string numérica)
    JUICE_MAX           (padrão: 0.20) # fração
    CHROME_BIN          (padrão: /usr/bin/chromium-browser)

Notas:
- Ajuste os seletores CSS na função parse_coupon_table() se a Betfair alterar o DOM.
- Respeite os Termos de Uso. Considere a API oficial para dados de eventos.
"""

import os
import io
import re
import sys
import json
import time
import math
import logging
from datetime import datetime, timedelta, timezone
from dateutil.parser import isoparse

import boto3
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# -------------------------
# Configuração e utilidades
# -------------------------
LOG = logging.getLogger("betfair-scraper")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

S3_BUCKET = os.getenv("S3_BUCKET", "").strip()
S3_CONFIG_KEY = os.getenv("S3_CONFIG_KEY", "leagues/leagues.json").strip()
S3_OUTPUT_PREFIX = os.getenv("S3_OUTPUT_PREFIX", "outputs").strip()
AWS_REGION = os.getenv("AWS_DEFAULT_REGION", "sa-east-1").strip()
LOOKAHEAD_DAYS = int(os.getenv("LOOKAHEAD_DAYS", "5"))
MIN_LIQUIDEZ = int(os.getenv("MIN_LIQUIDEZ", "50"))
JUICE_MAX = float(os.getenv("JUICE_MAX", "0.20"))
CHROME_BIN = os.getenv("CHROME_BIN", "/usr/bin/chromium-browser").strip()
STOP_TOPIC_ARN = os.getenv("STOP_TOPIC_ARN")

if not S3_BUCKET:
    raise SystemExit("Defina S3_BUCKET nas variáveis de ambiente.")

s3 = boto3.client("s3", region_name=AWS_REGION)

# cria fuso de São Paulo (UTC-3)
tz_brasil = timezone(timedelta(hours=-3))


# mapeamentos PT-BR do notebook original
MAPPING_MONTHS = {
    "jan": "01",
    "fev": "02",
    "mar": "03",
    "abr": "04",
    "mai": "05",
    "jun": "06",
    "jul": "07",
    "ago": "08",
    "set": "09",
    "out": "10",
    "nov": "11",
    "dez": "12",
}
WEEKDAY_MAP = {"seg": 0, "ter": 1, "qua": 2, "qui": 3, "sex": 4, "sáb": 5, "dom": 6}


def utc_now():
    return datetime.now(timezone.utc)


def within_next_days(kickoff_dt, days):
    start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
    end = start + timedelta(days=days + 1)  # até 23:59 do último dia
    return start <= kickoff_dt < end


def convert_date_pt(date_str):
    """
    Converte strings de data/hora no formato da Betfair em datetime UTC.
    Exemplo: 'qua 16:30', 'jan 18 09:30', 'Hoje às 21:30'
    """
    try:
        today = datetime.now()
        if not isinstance(date_str, str) or not date_str.strip():
            return None

        parts = date_str.split()

        # CASO 1: "Hoje às 21:30"
        if "hoje" in date_str.lower():
            # extrai hora com regex
            match = re.search(r"(\d{1,2}:\d{2})", date_str)
            if match:
                time_str = match.group(1)
                match_datetime = datetime.strptime(
                    f"{today.strftime('%d/%m/%Y')} {time_str}", "%d/%m/%Y %H:%M"
                )
                return match_datetime.replace(tzinfo=timezone.utc)
            return None

        # CASO 2: "qua 16:30"
        if parts[0][:3].lower() in WEEKDAY_MAP and len(parts) >= 2:
            weekday_abbr = parts[0][:3].lower()
            time_str = parts[1]
            weekday_diff = (WEEKDAY_MAP[weekday_abbr] - today.weekday()) % 7
            match_date = today + timedelta(days=weekday_diff)
            if weekday_diff == 0 and time_str < today.strftime("%H:%M"):
                match_date += timedelta(days=1)
            match_datetime = datetime.strptime(
                f"{match_date.strftime('%d/%m/%Y')} {time_str}", "%d/%m/%Y %H:%M"
            )
            return match_datetime.replace(tzinfo=timezone.utc)

        # CASO 3: "jan 18 09:30"
        if len(parts) >= 3:
            month_abbr = parts[0][:3].lower()
            day = parts[1]
            time_str = parts[2]
            month_number = MAPPING_MONTHS.get(month_abbr)
            if not month_number:
                raise ValueError(f"Mês desconhecido: {month_abbr}")
            current_year = today.year
            date_string = f"{current_year}-{month_number}-{day.zfill(2)} {time_str}"
            match_datetime = datetime.strptime(date_string, "%Y-%m-%d %H:%M")
            return match_datetime.replace(tzinfo=timezone(timedelta(hours=-3)))

    except Exception as e:
        LOG.warning(f"Erro convertendo data '{date_str}': {e}")
    return None


# -------------------------
# S3 helpers
# -------------------------
def s3_read_json(bucket, key):
    LOG.info(f"Lendo JSON de s3://{bucket}/{key}")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read().decode("utf-8"))


def s3_write_csv(bucket, key, df):
    csv_buf = io.StringIO()
    df.to_csv(csv_buf, index=False)
    LOG.info(f"Gravando CSV em s3://{bucket}/{key}")
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )


# -------------------------
# Selenium
# -------------------------

CHROME_BIN = os.getenv("CHROME_BIN", "").strip()


def build_driver():
    opts = Options()
    # NÃO headless por padrão (para ver via VNC). Para agendado, use xvfb-run.
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--window-size=1280,1024")

    # Só define binary_location se CHROME_BIN estiver setado
    if CHROME_BIN:
        opts.binary_location = CHROME_BIN

    # Cria o driver — Selenium Manager baixa o chromedriver se necessário
    driver = webdriver.Chrome(options=opts)
    driver.set_page_load_timeout(60)
    return driver


def dismiss_cookies(driver):
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "onetrust-policy-title"))
        )
        banner = driver.find_element(By.ID, "onetrust-policy-title")
        if banner.is_displayed():
            LOG.info("Encontrou banner de cookies; clicando 'Rejeitar'.")
            time.sleep(0.5)
            reject = driver.find_element(By.ID, "onetrust-reject-all-handler")
            time.sleep(0.5)
            reject.click()
            time.sleep(0.5)
    except Exception as e:
        LOG.info(f"Cookie banner não manipulado: {e}")


def parse_coupon_table(driver, league_name):
    """
    Retorna lista de dicts com campos:
    Date (datetime ISO), League, Home, Away, Odd_Back_H, Odd_Lay_H, Odd_Back_D, Odd_Lay_D, Odd_Back_A, Odd_Lay_A, Liquidez (BRL)
    """
    data = []
    # aguarda as tabelas
    WebDriverWait(driver, 15).until(
        EC.presence_of_all_elements_located((By.CSS_SELECTOR, "table.coupon-table"))
    )
    time.sleep(1.5)

    coupon_tables = driver.find_elements(By.CSS_SELECTOR, "table.coupon-table")
    for table in coupon_tables:
        rows = table.find_elements(By.CSS_SELECTOR, "tr[ng-repeat-start]")
        for row in rows:
            try:
                # pula jogos ao vivo
                if row.find_elements(By.CSS_SELECTOR, "div.bf-livescores-time-elapsed"):
                    continue

                # times
                club_elems = row.find_elements(By.CSS_SELECTOR, "ul.runners li.name")
                clubs = [e.text.strip() for e in club_elems if e.text.strip()]
                if len(clubs) < 2:
                    continue

                # data/hora
                start_span = row.find_element(
                    By.CSS_SELECTOR, "div.start-date-wrapper span"
                )
                start_raw = start_span.text.strip() if start_span else ""
                start_dt = convert_date_pt(start_raw)
                if not start_dt:
                    continue

                # odds (labels alternados: back/lay H, back/lay D, back/lay A)
                label_elems = row.find_elements(By.CSS_SELECTOR, ".coupon-runner label")
                labels_text = [le.text.strip() for le in label_elems]
                # precisamos de ao menos 11 elementos para acessar [0,2,4,6,8,10]
                if len(labels_text) < 11:
                    continue

                # liquidez (title da ul.matched-amount)
                matched = row.find_element(By.CSS_SELECTOR, "ul.matched-amount")
                matched_title = matched.get_attribute("title") if matched else ""
                matched_digits = (
                    re.sub(r"[^\d]", "", matched_title) if matched_title else "0"
                )
                liquidez = int(matched_digits) if matched_digits.isdigit() else 0

                data.append(
                    {
                        "Date": start_dt.isoformat(),
                        "League": league_name,
                        "Home": clubs[0],
                        "Away": clubs[1],
                        "Odd_Back_H": labels_text[0],
                        "Odd_Lay_H": labels_text[2],
                        "Odd_Back_D": labels_text[4],
                        "Odd_Lay_D": labels_text[6],
                        "Odd_Back_A": labels_text[8],
                        "Odd_Lay_A": labels_text[10],
                        "Liquidez (BRL)": liquidez,
                    }
                )
            except Exception as e:
                LOG.debug(f"Falha ao parsear uma linha: {e}")
                continue
    return data


# -------------------------
# Notificação Shutdown
# -------------------------


def notify_shutdown(topic_arn: str, message: str = "SCRAPER_DONE"):
    sns = boto3.client("sns", region_name=os.getenv("AWS_DEFAULT_REGION", "sa-east-1"))
    sns.publish(TopicArn=topic_arn, Message=message, Subject="betfair-scraper")


# -------------------------
# Pipeline principal
# -------------------------
def main():
    LOG.info("Iniciando scraper...")
    leagues = s3_read_json(S3_BUCKET, S3_CONFIG_KEY)  # {name: url}

    all_rows = []
    driver = build_driver()
    try:
        # abre a página inicial uma vez para banner de cookies
        driver.get("https://www.betfair.bet.br/exchange/plus")
        dismiss_cookies(driver)

        for league_name, url in leagues.items():
            try:
                LOG.info(f"Abrindo liga: {league_name} -> {url}")
                driver.get(url)
                rows = parse_coupon_table(driver, league_name)
                all_rows.extend(rows)
                time.sleep(0.5)
            except Exception as e:
                LOG.warning(f"Falha na liga '{league_name}': {e}")
                continue
    finally:
        try:
            driver.quit()
        except Exception:
            pass

    if not all_rows:
        LOG.warning("Nenhum jogo coletado.")
        # ainda exporta CSV vazio para rastreabilidade
    df = pd.DataFrame(all_rows)

    if not df.empty:
        # tipos numéricos para odds
        for col in ["Odd_Back_H", "Odd_Back_D", "Odd_Back_A"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        df["Odd_Back_H"] = df["Odd_Back_H"].round(2)
        df["Odd_Back_D"] = df["Odd_Back_D"].round(2)
        df["Odd_Back_A"] = df["Odd_Back_A"].round(2)

        df["KeyOdds"] = (
            df["Odd_Back_H"].apply(lambda x: f"{x:.2f}")
            + "-"
            + df["Odd_Back_D"].apply(lambda x: f"{x:.2f}")
            + "-"
            + df["Odd_Back_A"].apply(lambda x: f"{x:.2f}")
        )

        # converter Date (ISO) -> datetime
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce", utc=True)

        # filtros
        df = df.dropna(subset=["Date", "Odd_Back_H", "Odd_Back_D", "Odd_Back_A"])
        # janela de dias
        start = utc_now().replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=LOOKAHEAD_DAYS + 1)
        df = df[(df["Date"] >= start) & (df["Date"] < end)]
        # liquidez mínima
        df = df[df["Liquidez (BRL)"].astype(int) >= MIN_LIQUIDEZ]

        # juice = 1/oddH + 1/oddD + 1/oddA - 1
        df["Juice"] = (
            (1 / df["Odd_Back_H"]) + (1 / df["Odd_Back_D"]) + (1 / df["Odd_Back_A"]) - 1
        )
        df = df[df["Juice"] <= JUICE_MAX]

        # ordenação
        df = df.sort_values(by=["Date", "League", "Home", "Away"])

    # grava no S3
    dt_brasil = datetime.now(tz=tz_brasil).strftime("%Y-%m-%d")
    key = f"{S3_OUTPUT_PREFIX}/{dt_brasil}/jogos.csv"
    s3_write_csv(
        S3_BUCKET,
        key,
        df
        if not df.empty
        else pd.DataFrame(
            columns=[
                "Date",
                "League",
                "Home",
                "Away",
                "Odd_Back_H",
                "Odd_Lay_H",
                "Odd_Back_D",
                "Odd_Lay_D",
                "Odd_Back_A",
                "Odd_Lay_A",
                "Liquidez (BRL)",
                "Juice",
            ]
        ),
    )

    LOG.info(
        f"Concluído. Linhas: {0 if df is None else len(df)} | s3://{S3_BUCKET}/{key}"
    )

    if STOP_TOPIC_ARN:
        notify_shutdown(STOP_TOPIC_ARN)


if __name__ == "__main__":
    main()
