import os
from typing import List, Optional
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

app = FastAPI(
    title="CryptoFlow API",
    description=(
        "RESTful interface for querying cryptocurrency market data, "
        "technical indicators, and anomaly events stored in PostgreSQL."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_conn():
    try:
        return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unavailable: {e}")


# ─── Models ─────────────────────────────────────────────────────────────────

class MarketData(BaseModel):
    id: int
    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    sma_20: Optional[float] = None
    rsi_14: Optional[float] = None
    bb_high: Optional[float] = None
    bb_low: Optional[float] = None
    is_anomaly: bool
    anomaly_reason: Optional[str] = None


class SummaryStats(BaseModel):
    symbol: str
    total_records: int
    anomaly_count: int
    avg_close: float
    min_close: float
    max_close: float
    last_updated: Optional[datetime] = None


# ─── Endpoints ──────────────────────────────────────────────────────────────

@app.get("/health", tags=["System"])
def health():
    """Liveness probe – verifies API and database connectivity."""
    conn = get_conn()
    conn.close()
    return {"status": "ok", "database": "connected"}


@app.get("/api/v1/market-data", response_model=List[MarketData], tags=["Market Data"])
def get_market_data(
    symbol: Optional[str] = Query(None, description="Filter by trading pair, e.g. BTCUSDT"),
    from_ts: Optional[datetime] = Query(None, description="Start timestamp (ISO-8601)"),
    to_ts:   Optional[datetime] = Query(None, description="End timestamp (ISO-8601)"),
    limit:   int = Query(100, le=1000),
    offset:  int = 0,
):
    """Return paginated OHLCV records with computed technical indicators."""
    conn = get_conn()
    cur  = conn.cursor()

    where, params = [], []
    if symbol:
        where.append("symbol = %s");  params.append(symbol)
    if from_ts:
        where.append("timestamp >= %s"); params.append(from_ts)
    if to_ts:
        where.append("timestamp <= %s"); params.append(to_ts)

    sql = "SELECT * FROM market_data"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
    params += [limit, offset]

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


@app.get("/api/v1/anomalies", response_model=List[MarketData], tags=["Anomalies"])
def get_anomalies(
    symbol: Optional[str] = Query(None),
    limit:  int = Query(100, le=1000),
    offset: int = 0,
):
    """Return records flagged as anomalies (BB breakout or volume spike)."""
    conn = get_conn()
    cur  = conn.cursor()

    where  = ["is_anomaly = TRUE"]
    params = []
    if symbol:
        where.append("symbol = %s"); params.append(symbol)

    sql = "SELECT * FROM market_data WHERE " + " AND ".join(where)
    sql += " ORDER BY timestamp DESC LIMIT %s OFFSET %s"
    params += [limit, offset]

    cur.execute(sql, tuple(params))
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


@app.get("/api/v1/summary", response_model=List[SummaryStats], tags=["Market Data"])
def get_summary():
    """Per-symbol summary: record count, anomaly count, close price stats."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT
            symbol,
            COUNT(*)                    AS total_records,
            SUM(is_anomaly::int)        AS anomaly_count,
            ROUND(AVG(close)::numeric, 4) AS avg_close,
            MIN(close)                  AS min_close,
            MAX(close)                  AS max_close,
            MAX(timestamp)              AS last_updated
        FROM market_data
        GROUP BY symbol
        ORDER BY symbol
    """)
    rows = cur.fetchall()
    cur.close(); conn.close()
    return rows


@app.get("/api/v1/indicators/{symbol}", tags=["Indicators"])
def get_indicators(symbol: str, limit: int = Query(50, le=500)):
    """Latest SMA-20, RSI-14, Bollinger Bands for a given symbol."""
    conn = get_conn()
    cur  = conn.cursor()
    cur.execute("""
        SELECT timestamp, close, sma_20, rsi_14, bb_high, bb_low
        FROM market_data
        WHERE symbol = %s AND sma_20 IS NOT NULL
        ORDER BY timestamp DESC
        LIMIT %s
    """, (symbol.upper(), limit))
    rows = cur.fetchall()
    cur.close(); conn.close()
    if not rows:
        raise HTTPException(status_code=404, detail=f"No data found for symbol {symbol}")
    return rows
