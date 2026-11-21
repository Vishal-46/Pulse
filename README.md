# Pulse: Market Data Engineering Pipeline

Pulse ingests NSE equity market data through **Kafka** and a batch path, and processes it with
**PySpark** (Structured Streaming + batch) into a **Bronze → Silver → Gold** lakehouse on
S3-compatible object storage. It finishes with automated data-quality checks and Spark SQL analytics.

```
Yahoo Finance ─▶ Producer (validate, key=symbol) ─▶ Redpanda/Kafka ─▶ Spark Structured Streaming ─┐
      │                                                                                            ▼
      └────────▶ Daily history (Spark batch) ─────────────────────────────────────────────▶ BRONZE (raw)
                                                                                                   │
                         Spark: parse · validate · quarantine · dedupe ──────────────────▶ SILVER (trusted)
                         Spark: window functions · aggregations · broadcast join ──────────▶ GOLD (analytics)
                         Spark: quality checks (fail on error) · Spark SQL report
```

## What it does
- **Streaming ingestion:** 1-minute bars for 10 NSE large caps → a Kafka topic (3 partitions, keyed by symbol, `acks=all`, idempotent producer, dead-letter topic). Live, record and replay modes.
- **Exactly-once Bronze:** Spark Structured Streaming with checkpoints writes raw payloads plus lineage (partition, offset, ingest time) as Parquet.
- **Batch ingestion:** 2 years of daily bars, with idempotent re-runs via dynamic partition overwrite.
- **Silver:** explicit-schema JSON parsing, 7 validation rules with a **quarantine** table, deterministic **deduplication** (`row_number` over `event_id`), India-time trade dates, market-hours flags.
- **Gold:** daily returns, 20/50-day moving averages, annualised volatility, drawdown, volume spikes, intraday OHLC + **VWAP**, a reconciliation of the streaming vs batch pipelines, and symbol and sector performance (**broadcast join**).
- **Quality gate:** 16 automated checks; ERROR-level failures fail the run.
- **Tests:** 34 unit tests (including Spark transformations) + 3 integration tests on the real lake.

## Tech stack
Python 3.12 · **Apache Spark 4.3 / PySpark** (Structured Streaming, DataFrames, window functions, Spark SQL) ·
Kafka API via **Redpanda** · confluent-kafka · **Parquet** · S3-compatible object storage (**RustFS**, `s3a://`) ·
pydantic · Docker Compose · pytest · uv

## Quick start
```bash
make setup     # Python 3.12 venv + dependencies + .env
make demo      # start Kafka + storage, publish a recorded session, run the whole pipeline
make test      # unit tests (no Docker needed)
```
Requires Docker, Java 17+, `uv` and `make`. The first Spark run downloads ~650 MB of connector jars once.

| UI | URL |
|---|---|
| Redpanda Console (topics, partitions, offsets) | http://localhost:8081 |
| Object storage console (the lake) | http://localhost:9001/rustfs/console/ |
| Spark UI (while a job runs) | http://localhost:4040 |

## Sample output (real run)
```
silver 1m: {'bronze': 7236, 'quarantined': 0, 'duplicates_removed': 3618, 'silver': 3618}
gold rows: {'daily_metrics': 4958, 'intraday_summary': 10, 'reconciliation': 10,
            'symbol_performance': 10, 'sector_performance': 6}
quality:   16/16 PASS
```
| symbol | sector | 1y return | volatility | max drawdown |
|---|---|---|---|---|
| SBIN.NS | Financials | +22.1% | 22.0% | -23.5% |
| ICICIBANK.NS | Financials | +18.3% | 18.9% | -19.0% |
| INFY.NS | Information Technology | -28.8% | 28.8% | -50.7% |
