# 🪙 Crypto ETL Pipeline

Automated data pipeline that extracts real-time cryptocurrency prices from the CoinGecko public API, transforms and cleanses the data with pandas, and loads it into a PostgreSQL database — orchestrated with Apache Airflow.

> This project runs as part of a [centralized data engineering server](https://github.com/sabnei/portfolio-server) using Docker and Git submodules. It does not include its own infrastructure — see the server repo for setup.

---

## 🏗️ Architecture

```
CoinGecko API (free, no auth)
       │
       │ JSON — top 100 coins by market cap
       ▼
┌──────────────┐
│   EXTRACT    │  fetch_crypto_data()
│  extract.py  │  requests.get() · timeout · error handling
└──────┬───────┘
       │ raw list[dict]
       ▼
┌──────────────┐
│  TRANSFORM   │  transform_data()
│ transform.py │  • Select & rename 8 columns
│              │  • Cast last_updated → TIMESTAMPTZ
│              │  • Drop rows missing price or market_cap
│              │  • Add extracted_at audit timestamp (UTC)
└──────┬───────┘
       │ clean pd.DataFrame
       ▼
┌──────────────┐
│     LOAD     │  load_data()
│   load.py    │  shared_etl.db.get_engine() → df.to_sql()
│              │  → postgres-central : crypto.crypto_prices
└──────────────┘
       │
       ▼
 Airflow DAG — schedule: @hourly
 3 tasks: extract_task → transform_task → load_task
 XCom passes data between tasks
```

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8.1 |
| Data source | CoinGecko API (free tier) |
| Transform | Python 3.11 · pandas |
| Storage | PostgreSQL 15 · schema `crypto` |
| DB client | SQLAlchemy · psycopg2 |
| Infrastructure | Docker (managed by portfolio-server) |

---

## 📁 Project Structure

```
crypto-etl-pipeline/
├── dags/
│   └── crypto_pipeline_dag.py   # Airflow DAG — 3 tasks with XCom
├── etl/
│   ├── __init__.py
│   ├── extract.py               # CoinGecko API call
│   ├── transform.py             # pandas cleaning & normalization
│   └── load.py                  # writes to crypto.crypto_prices
├── tests/
│   ├── test_extract.py
│   ├── test_transform.py
│   └── test_load.py
├── .env.example
├── requirements.txt
└── README.md
```

> `docker-compose.yml` and `sql/` are intentionally absent.
> Infrastructure is managed centrally by [portfolio-server](https://github.com/sabnei/portfolio-server).

---

## 🗄️ Database Schema

Lives in `crypto.crypto_prices` on the shared PostgreSQL instance:

```sql
CREATE TABLE crypto.crypto_prices (
    id               SERIAL PRIMARY KEY,
    coin_id          VARCHAR(150)   NOT NULL,   -- e.g. "bitcoin"
    symbol           VARCHAR(50)    NOT NULL,   -- e.g. "btc"
    name             VARCHAR(150)   NOT NULL,   -- e.g. "Bitcoin"
    price_usd        DECIMAL(20, 8) NOT NULL,
    market_cap_usd   BIGINT,
    volume_24h       BIGINT,
    change_pct_24h   DECIMAL(8, 4),
    last_updated_api TIMESTAMPTZ,
    extracted_at     TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 🚀 Running This Project

### On the central server (production)

This project runs as a Git submodule inside [portfolio-server](https://github.com/sabnei/portfolio-server).

```bash
# Add to the server (run once)
cd ~/portfolio-server/dags
git submodule add https://github.com/sabnei/crypto-etl-pipeline.git crypto-etl-pipeline
cd .. && git add . && git commit -m "feat: add crypto-etl-pipeline" && git push
```

Airflow detects the new DAG automatically within ~30 seconds. No restart needed.

### Local development

```bash
git clone https://github.com/sabnei/crypto-etl-pipeline.git
cd crypto-etl-pipeline
python -m venv venv && source venv/bin/activate
pip install -r requirements.txt
cp .env.example .env   # fill in your local DB credentials
pytest tests/
```

---

## 📊 Sample Queries

```sql
-- Latest snapshot
SELECT coin_id, name, price_usd, change_pct_24h
FROM crypto.crypto_prices
WHERE extracted_at = (SELECT MAX(extracted_at) FROM crypto.crypto_prices)
ORDER BY market_cap_usd DESC;

-- Bitcoin price history (last 24 runs)
SELECT extracted_at, price_usd
FROM crypto.crypto_prices
WHERE coin_id = 'bitcoin'
ORDER BY extracted_at DESC
LIMIT 24;

-- Most volatile coins
SELECT coin_id, ROUND(AVG(ABS(change_pct_24h))::numeric, 2) AS avg_volatility
FROM crypto.crypto_prices
GROUP BY coin_id
ORDER BY avg_volatility DESC
LIMIT 10;
```

---

## ⚙️ DAG Configuration

| Parameter | Value | Reason |
|---|---|---|
| `schedule` | `@hourly` | Captures meaningful price movement |
| `catchup` | `False` | Only runs from now, no backfill |
| `retries` | `2` | Handles transient API failures |
| `retry_delay` | `5 min` | Respects CoinGecko rate limits |

**Why XCom between tasks?** Each task is independently retryable. If `load_task` fails due to a DB issue, Airflow retries only that step — it doesn't re-call the API or re-run the transform.

---

## 🔮 Possible Improvements

- Add data quality checks with **Great Expectations** before loading
- Connect **Grafana** to `crypto.crypto_prices` for live price charts
- Add a sensor task that skips runs when CoinGecko returns stale data
- Store raw API response in a bronze layer before transforming

---

## 📄 License

MIT — see [LICENSE](LICENSE).
