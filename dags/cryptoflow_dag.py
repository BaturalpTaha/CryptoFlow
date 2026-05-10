"""
CryptoFlow ETL DAG
==================
Orchestrates the full Extract → Transform → Load pipeline for cryptocurrency
market data from the Binance public REST API.

Data flow:
  Binance API (OHLCV klines)
      │
      ▼
  extract_data   – fetch candlestick data; synthetic fallback if API is down
      │
      ▼
  transform_data – compute SMA-20, RSI-14, Bollinger Bands; flag anomalies
      │
     ╱ ╲
    ▼   ▼
  load_postgres  load_elasticsearch
      │               │
      ▼               ▼
  PostgreSQL     Elasticsearch  ──→  Kibana dashboards
      │
      ▼
  FastAPI REST endpoints
"""

import os
import json
import random
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from ta.momentum import RSIIndicator
from ta.volatility import BollingerBands
from ta.trend import SMAIndicator

from airflow import DAG
from airflow.operators.python import PythonOperator

import psycopg2
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

# ─── Configuration (from environment / Airflow env vars) ────────────────────
SYMBOLS       = os.environ.get("BINANCE_SYMBOLS", "BTCUSDT,ETHUSDT,SOLUSDT").split(",")
INTERVAL      = os.environ.get("BINANCE_INTERVAL", "1m")
LIMIT         = int(os.environ.get("BINANCE_LIMIT", "200"))

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_DB   = os.environ.get("CRYPTO_DB", "cryptoflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "cryptoflow")
POSTGRES_PASS = os.environ.get("POSTGRES_PASSWORD", "cryptoflow_pass")

ELASTIC_HOST  = os.environ.get("ELASTIC_HOST", "http://elasticsearch:9200")
ELASTIC_PASS  = os.environ.get("ELASTIC_PASSWORD", "elastic_pass")
ELASTIC_INDEX = os.environ.get("ELASTIC_INDEX", "cryptoflow-market-data")

log = logging.getLogger(__name__)

# ─── Base prices for synthetic fallback ─────────────────────────────────────
BASE_PRICES = {"BTCUSDT": 65000.0, "ETHUSDT": 3500.0, "SOLUSDT": 150.0}


# ════════════════════════════════════════════════════════════════════════════
# Task 1 – Extract
# ════════════════════════════════════════════════════════════════════════════

def extract_data(**context):
    """Fetch OHLCV klines from Binance for all configured symbols.

    Falls back to deterministic synthetic data when the API is unavailable,
    ensuring the pipeline remains testable without internet access.
    """
    extracted = []

    for symbol in SYMBOLS:
        url = (
            f"https://api.binance.com/api/v3/klines"
            f"?symbol={symbol}&interval={INTERVAL}&limit={LIMIT}"
        )
        try:
            resp = requests.get(url, timeout=15)
            resp.raise_for_status()
            raw = resp.json()

            for row in raw:
                extracted.append({
                    "symbol":    symbol,
                    "timestamp": datetime.fromtimestamp(row[0] / 1000).isoformat(),
                    "open":      float(row[1]),
                    "high":      float(row[2]),
                    "low":       float(row[3]),
                    "close":     float(row[4]),
                    "volume":    float(row[5]),
                })
            log.info("Extracted %d records for %s from Binance.", len(raw), symbol)

        except requests.RequestException as exc:
            log.warning("Binance unavailable for %s (%s). Using synthetic fallback.", symbol, exc)
            base = BASE_PRICES.get(symbol, 100.0)
            now  = datetime.utcnow()
            random.seed(symbol)          # deterministic per symbol
            for i in range(LIMIT):
                t       = now - timedelta(minutes=LIMIT - i)
                open_p  = base * (1 + random.uniform(-0.005, 0.005))
                close_p = open_p * (1 + random.uniform(-0.005, 0.005))
                high_p  = max(open_p, close_p) * (1 + random.uniform(0, 0.003))
                low_p   = min(open_p, close_p) * (1 - random.uniform(0, 0.003))
                extracted.append({
                    "symbol":    symbol,
                    "timestamp": t.isoformat(),
                    "open":      round(open_p,  4),
                    "high":      round(high_p,  4),
                    "low":       round(low_p,   4),
                    "close":     round(close_p, 4),
                    "volume":    round(random.uniform(10, 1000), 4),
                })

    context["ti"].xcom_push(key="raw_data", value=extracted)
    log.info("Total extracted: %d records across %d symbols.", len(extracted), len(SYMBOLS))


# ════════════════════════════════════════════════════════════════════════════
# Task 2 – Transform
# ════════════════════════════════════════════════════════════════════════════

def transform_data(**context):
    """Enrich raw OHLCV bars with technical indicators and anomaly flags.

    Indicators computed per symbol:
    • SMA-20  – 20-period simple moving average of close
    • RSI-14  – 14-period relative strength index
    • BB-high / BB-low – Bollinger Bands (20-period, 2 std-dev)

    Anomaly rules:
    • Close price breaks above BB-high or below BB-low
    • Volume > mean + 2×std  (volume spike)
    """
    raw_data = context["ti"].xcom_pull(key="raw_data", task_ids="extract_data")
    if not raw_data:
        log.warning("No raw data received – skipping transform.")
        return

    df = pd.DataFrame(raw_data)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    transformed = []

    for symbol in SYMBOLS:
        sdf = df[df["symbol"] == symbol].copy().sort_values("timestamp").reset_index(drop=True)

        if len(sdf) < 20:
            log.warning("Not enough bars for %s (got %d, need ≥20). Skipping.", symbol, len(sdf))
            continue

        # Technical indicators
        sdf["sma_20"]  = SMAIndicator(close=sdf["close"], window=20).sma_indicator()
        sdf["rsi_14"]  = RSIIndicator(close=sdf["close"], window=14).rsi()
        bb             = BollingerBands(close=sdf["close"], window=20, window_dev=2)
        sdf["bb_high"] = bb.bollinger_hband()
        sdf["bb_low"]  = bb.bollinger_lband()

        # Anomaly detection
        vol_mean = sdf["volume"].mean()
        vol_std  = sdf["volume"].std()

        sdf["is_anomaly"]    = False
        sdf["anomaly_reason"] = None

        for idx in sdf.index:
            reasons = []
            if sdf.at[idx, "close"] > sdf.at[idx, "bb_high"]:
                reasons.append("Price above BB_High")
            if sdf.at[idx, "close"] < sdf.at[idx, "bb_low"]:
                reasons.append("Price below BB_Low")
            if sdf.at[idx, "volume"] > vol_mean + 2 * vol_std:
                reasons.append("Abnormal Volume Spike")

            if reasons:
                sdf.at[idx, "is_anomaly"]    = True
                sdf.at[idx, "anomaly_reason"] = " | ".join(reasons)

        # Drop warm-up rows where indicators are NaN
        sdf = sdf.dropna(subset=["sma_20", "rsi_14", "bb_high", "bb_low"])

        # Stringify timestamp for XCom serialisation
        sdf["timestamp"] = sdf["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")

        transformed.extend(sdf.to_dict("records"))
        log.info("Transformed %d bars for %s (%d anomalies).",
                 len(sdf), symbol, sdf["is_anomaly"].sum())

    context["ti"].xcom_push(key="transformed_data", value=transformed)
    log.info("Total transformed: %d records.", len(transformed))


# ════════════════════════════════════════════════════════════════════════════
# Task 3a – Load to PostgreSQL
# ════════════════════════════════════════════════════════════════════════════

def load_to_postgres(**context):
    """Upsert transformed records into the cryptoflow.market_data table.

    Uses ON CONFLICT DO NOTHING to make the load idempotent: re-running the
    DAG for the same interval does not produce duplicates.
    """
    data = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")
    if not data:
        log.warning("No transformed data to load into PostgreSQL.")
        return

    conn = psycopg2.connect(
        host=POSTGRES_HOST, dbname=POSTGRES_DB,
        user=POSTGRES_USER, password=POSTGRES_PASS
    )
    cur = conn.cursor()

    sql = """
        INSERT INTO market_data
            (symbol, timestamp, open, high, low, close, volume,
             sma_20, rsi_14, bb_high, bb_low, is_anomaly, anomaly_reason)
        VALUES
            (%(symbol)s, %(timestamp)s, %(open)s, %(high)s, %(low)s,
             %(close)s, %(volume)s, %(sma_20)s, %(rsi_14)s,
             %(bb_high)s, %(bb_low)s, %(is_anomaly)s, %(anomaly_reason)s)
        ON CONFLICT (symbol, timestamp) DO NOTHING;
    """
    cur.executemany(sql, data)
    conn.commit()
    cur.close()
    conn.close()

    log.info("Loaded %d records into PostgreSQL (idempotent upsert).", len(data))


# ════════════════════════════════════════════════════════════════════════════
# Task 3b – Load to Elasticsearch
# ════════════════════════════════════════════════════════════════════════════

def load_to_elasticsearch(**context):
    """Bulk-index transformed records into Elasticsearch.

    Creates the index with explicit field mappings on first run.
    Uses the elasticsearch-py `bulk` helper for efficient batched indexing.
    Document _id = '{symbol}_{timestamp}' for idempotent re-indexing.
    """
    data = context["ti"].xcom_pull(key="transformed_data", task_ids="transform_data")
    if not data:
        log.warning("No transformed data to load into Elasticsearch.")
        return

    es = Elasticsearch([ELASTIC_HOST])

    # Create index with mappings if it does not exist
    if not es.indices.exists(index=ELASTIC_INDEX):
        mapping = {
            "mappings": {
                "properties": {
                    "symbol":        {"type": "keyword"},
                    "timestamp":     {"type": "date", "format": "yyyy-MM-dd HH:mm:ss"},
                    "open":          {"type": "float"},
                    "high":          {"type": "float"},
                    "low":           {"type": "float"},
                    "close":         {"type": "float"},
                    "volume":        {"type": "float"},
                    "sma_20":        {"type": "float"},
                    "rsi_14":        {"type": "float"},
                    "bb_high":       {"type": "float"},
                    "bb_low":        {"type": "float"},
                    "is_anomaly":    {"type": "boolean"},
                    "anomaly_reason":{"type": "text"},
                }
            }
        }
        es.indices.create(index=ELASTIC_INDEX, body=mapping)
        log.info("Created Elasticsearch index: %s", ELASTIC_INDEX)

    actions = [
        {
            "_index": ELASTIC_INDEX,
            "_id":    f"{doc['symbol']}_{doc['timestamp']}",
            "_source": doc,
        }
        for doc in data
    ]

    success, failed = bulk(es, actions, raise_on_error=False)
    if failed:
        log.error("%d documents failed to index.", len(failed))
    log.info("Indexed %d documents into Elasticsearch.", success)


# ════════════════════════════════════════════════════════════════════════════
# DAG definition
# ════════════════════════════════════════════════════════════════════════════

default_args = {
    "owner":            "cryptoflow",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
}

with DAG(
    dag_id="cryptoflow_etl",
    default_args=default_args,
    description="CryptoFlow: Binance OHLCV → Transform → PostgreSQL & Elasticsearch",
    schedule_interval=timedelta(minutes=5),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["cryptoflow", "etl", "crypto"],
) as dag:

    t_extract = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t_elastic = PythonOperator(
        task_id="load_to_elasticsearch",
        python_callable=load_to_elasticsearch,
    )

    # extract → transform → [postgres, elasticsearch] (parallel loads)
    t_extract >> t_transform >> [t_postgres, t_elastic]
