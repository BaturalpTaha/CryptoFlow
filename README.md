# CryptoFlow

**End-to-End Containerized Data Engineering Pipeline for Cryptocurrency Market Analysis and Anomaly Detection**

> YZV 322E — Applied Data Engineering · Spring 2026 · Istanbul Technical University

---

## Team Members

| Name | Student ID | Role |
|---|---|---|
| Baturalp Taha Yılmaz | 150220302 | Airflow DAG & Elasticsearch Integration |
| Yusuf Solmaz | 150220306 | Docker Infrastructure & FastAPI |
| Mhd Laith Alkurdi | 150230909 | PostgreSQL Schema & Data Transformation |

---

## Project Summary

CryptoFlow fetches live OHLCV (Open/High/Low/Close/Volume) candlestick data from the **Binance public REST API** for three trading pairs (BTC/USDT, ETH/USDT, SOL/USDT), enriches it with technical indicators (SMA-20, RSI-14, Bollinger Bands), and detects market anomalies. The enriched data is stored in **PostgreSQL** for relational queries and **Elasticsearch** for fast aggregations and Kibana dashboards. A **FastAPI** service exposes REST endpoints for programmatic access. The entire stack is orchestrated by **Apache Airflow** and runs inside Docker containers.

---

## Architecture

```
Binance REST API
      │  (HTTP GET klines)
      ▼
┌─────────────────────────────────────────────────────┐
│              Apache Airflow (DAG: cryptoflow_etl)   │
│                                                     │
│  extract_data → transform_data → load_to_postgres   │
│                              └──→ load_to_elasticsearch │
└─────────────────────────────────────────────────────┘
         │                          │
         ▼                          ▼
    PostgreSQL                Elasticsearch
    (market_data)         (cryptoflow-market-data)
         │                          │
         ▼                          ▼
    FastAPI REST               Kibana Dashboards
    /api/v1/...               (port 5601)
```

**Data flow:** Every 5 minutes Airflow triggers the ETL DAG. After extraction and transformation, parallel tasks write to both storage backends. Kibana reads from Elasticsearch; FastAPI reads from PostgreSQL.

---

## Tools Used

| Tool | Role | Port |
|---|---|---|
| Apache Airflow 2.9.3 | Workflow orchestration (DAG scheduling, retries) | 8080 |
| PostgreSQL 16 | Relational storage (OLTP queries, idempotent upsert) | 5432 |
| pgAdmin 4 | Database administration UI | 5050 |
| Elasticsearch 8.12 | Document indexing, time-range search, aggregations | 9200 |
| Kibana 8.12 | Interactive dashboards (price charts, anomaly timelines) | 5601 |
| FastAPI | RESTful API for programmatic data access | 8000 |

---

## Setup Instructions

### Prerequisites
- Docker ≥ 24  
- Docker Compose ≥ 2.20  
- 8 GB free RAM recommended  

### 1. Clone and configure
```bash
git clone https://github.com/BaturalpTaha/CryptoFlow.git
cd CryptoFlow
cp .env.example .env
```
The `.env` file ships with working defaults — no editing needed.

### 2. Start the full stack
```bash
docker compose up --build -d
```
The first run downloads images and builds custom layers (~5–8 min depending on network speed). Subsequent starts take <30 seconds.

### 3. Wait for services and access them
```bash
docker compose ps        # all services should show "healthy" or "running"
```

| Service | URL | Login |
|---|---|---|
| Airflow Web UI | http://localhost:8080 | admin / admin |
| Kibana | http://localhost:5601 | No login needed |
| FastAPI Swagger UI | http://localhost:8000/docs | No login needed |
| pgAdmin | http://localhost:5050 | admin@cryptoflow.com / admin |

### 4. Enable the Airflow DAG
Open the Airflow UI → toggle **cryptoflow_etl** ON → trigger a run manually or wait for the 5-minute schedule.

---

## Example API Commands

```bash
# Health check
curl http://localhost:8000/health

# Latest 10 BTC records
curl "http://localhost:8000/api/v1/market-data?symbol=BTCUSDT&limit=10"

# All anomalies for ETH
curl "http://localhost:8000/api/v1/anomalies?symbol=ETHUSDT"

# Per-symbol summary statistics
curl http://localhost:8000/api/v1/summary

# Latest indicators for SOL
curl http://localhost:8000/api/v1/indicators/SOLUSDT
```

---

## Repository Structure

```
CryptoFlow/
├── dags/
│   └── cryptoflow_dag.py       # Airflow ETL DAG
├── docker/
│   ├── airflow/Dockerfile      # Airflow image with pip deps
│   ├── fastapi/Dockerfile      # FastAPI image
│   └── pgadmin/servers.json    # pgAdmin auto-connect config
├── src/
│   └── api/
│       ├── main.py             # FastAPI application
│       └── requirements.txt
├── sql/
│   └── init.sql                # PostgreSQL schema + indexes
├── elasticsearch/
│   ├── mappings.json           # ES index field mappings
│   └── export.ndjson           # Kibana Saved Objects (dashboard import)
├── logs/                       # Airflow task logs (gitignored)
├── plugins/                    # Airflow plugins (empty)
├── docker-compose.yml          # Single-command stack definition
├── sample_market_data.csv      # Demo dataset
├── start.ps1                   # Windows one-click startup script
├── .env.example                # Template credentials (copy to .env)
├── .gitignore
└── README.md
```

---

## Known Limitations

- **LocalExecutor**: Airflow runs tasks sequentially on one node; horizontal scaling would require CeleryExecutor + Redis.
- **Elasticsearch memory**: Capped at 512 MB heap (1 GB container) to run comfortably on a 16 GB laptop.
- **Binance API rate limits**: Free tier allows ~1,200 requests/minute; the 5-minute schedule stays well within this limit.
- **Synthetic fallback**: When the Binance API is unreachable, deterministic synthetic data is generated so the pipeline keeps running.
- **No TLS**: Services communicate over plain HTTP within the Docker network. Production deployments should add TLS termination.

---

## License

MIT
