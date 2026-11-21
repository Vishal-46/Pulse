# Pulse 2.0: The Complete Guide

A guide to **what** Pulse does, **why** each piece exists, **how** the code works, and
**which commands** to run, written so you can rebuild, run, debug and explain the project in an
interview.

> Every number in this guide (row counts, timings, query results) comes from a real run of the
> pipeline on 21 Nov 2026. Your numbers will differ slightly if you fetch fresh data.

---

## Contents

1. [What Pulse is (the 30-second pitch)](#1-what-pulse-is)
2. [Architecture: the big picture](#2-architecture)
3. [Technology choices and why](#3-technology-choices-and-why)
4. [Project structure](#4-project-structure)
5. [Setup from zero](#5-setup-from-zero)
6. [Running the project](#6-running-the-project)
7. [Layer by layer: concepts and code](#7-layer-by-layer-concepts-and-code)
   - 7.1 [The data contract](#71-the-data-contract-schemaspy)
   - 7.2 [The source adapter](#72-the-source-adapter-sourcesyahoopy)
   - 7.3 [Kafka and the producer](#73-kafka-and-the-producer-ingestionproducerpy)
   - 7.4 [Bronze, streaming: Spark Structured Streaming](#74-bronze-streaming-spark-structured-streaming)
   - 7.5 [Bronze, batch: daily history](#75-bronze-batch-daily-history)
   - 7.6 [Silver: clean, validate, deduplicate](#76-silver-clean-validate-deduplicate)
   - 7.7 [Gold: analytics with window functions](#77-gold-analytics-with-window-functions)
   - 7.8 [Data quality checks](#78-data-quality-checks)
   - 7.9 [Analytics with Spark SQL](#79-analytics-with-spark-sql)
8. [Spark performance concepts used](#8-spark-performance-concepts-used)
9. [Testing](#9-testing)
10. [Real bugs we hit and what they teach](#10-real-bugs-we-hit-and-what-they-teach)
11. [Command cheat sheets: Docker, rpk, make](#11-command-cheat-sheets)
12. [Interview preparation](#12-interview-preparation)
13. [Limitations and next steps](#13-limitations-and-next-steps)
14. [Glossary](#14-glossary)

---

## 1. What Pulse is

Pulse is a **market data engineering pipeline** for 10 large Indian (NSE) stocks.

- It **ingests** 1-minute price bars through **Kafka** (streaming) and 2 years of daily bars through a **batch** job.
- It stores them in a **data lake** on S3-compatible object storage, organised as **Bronze → Silver → Gold**.
- It **processes** everything with **PySpark**: Structured Streaming for Kafka, and batch jobs with window functions, aggregations and joins.
- It **checks** data quality and produces **analytics** with Spark SQL: returns, moving averages, volatility, volume spikes, sector performance and VWAP.

**Resume line:**
> Built a market-data pipeline: Kafka (Redpanda) ingestion → PySpark Structured Streaming and batch jobs implementing a Bronze/Silver/Gold lakehouse on S3-compatible storage, with window-function analytics, deduplication and automated data-quality checks.

---

## 2. Architecture

```
                  STREAMING PATH (1-minute bars)
 ┌──────────────┐   ┌─────────────────────┐   ┌────────────────────┐   ┌───────────────────────┐
 │ Yahoo Finance│──▶│ Producer (Python)   │──▶│ Redpanda (Kafka)   │──▶│ Spark Structured      │
 │ (yfinance)   │   │ validate → JSON     │   │ topic market.bars.1m│  │ Streaming             │──┐
 └──────────────┘   │ key = symbol        │   │ 3 partitions       │   │ checkpointed          │  │
        │           │ LIVE / REPLAY mode  │   │ + DLQ topic        │   └───────────────────────┘  │
        │           └─────────────────────┘   └────────────────────┘                              │
        │                                                                                         ▼
        │         BATCH PATH (2 years of daily bars)                         ┌──────────────────────────┐
        └────────▶ batch_extract.py (Spark) ───────────────────────────────▶ │  DATA LAKE  (RustFS, S3) │
                                                                            │                          │
                                                                            │  bronze/  raw JSON       │
                   Spark batch:  bronze ──▶ silver  (parse, validate, ────▶ │  silver/  clean, typed,  │
                                                     quarantine, dedupe)    │           deduplicated   │
                   Spark batch:  silver ──▶ gold    (windows, aggs, ──────▶ │  gold/    analytics      │
                                                     broadcast join)        └────────────┬─────────────┘
                                                                                         │
                   Spark:  quality checks (fail the run on errors)  ◀────────────────────┤
                   Spark SQL:  analytics report                     ◀────────────────────┘
```

### Why two ingestion paths?

| | Streaming path | Batch path |
|---|---|---|
| Data | 1-minute bars, arriving continuously | 2 years of daily bars, a fixed, known range |
| Tool | Kafka + Spark Structured Streaming | Spark batch job, no Kafka |
| Why | Data never "ends": you need a durable buffer and incremental processing | Bounded data: a re-runnable batch job is simpler and enough |

Both paths land in **Bronze**, and **Silver** treats them the same way. Knowing *when not to use Kafka* is as important as knowing how to use it.

### The medallion layers

| Layer | Contents | Rule |
|---|---|---|
| **Bronze** | Raw JSON string, exactly as received + lineage (Kafka partition/offset, ingest time) | Never modify. It's your "undo button": everything downstream can be rebuilt from it. |
| **Silver** | Typed columns, UTC timestamps, one row per event, flags | Trustworthy: validated and deduplicated. Bad rows go to a **quarantine** table, never silently dropped. |
| **Gold** | Business tables: returns, moving averages, volatility, VWAP, sector stats | Built for answering questions. Small and fast to query. |

### The full run in one picture

```
make demo
  │
  ├─ make up              docker compose: Redpanda + Console + RustFS (waits until healthy)
  ├─ make replay          producer: 3,618 recorded bars → Kafka topic
  └─ make pipeline-offline  ONE Spark session runs, in order:
        extract  → bronze/bars_1d        4,958 rows   (~5 s)
        bronze   → bronze/bars_1m        3,618 rows   (~2 s)   Kafka → Parquet via checkpoint
        silver   → silver/bars_1m, bars_1d            (~6 s)   parse, quarantine, dedupe
        gold     → 5 gold tables                      (~4 s)   windows, aggregations, joins
        quality  → 16 checks                          (~2 s)   fails the run on ERROR
        report   → 6 Spark SQL queries                (~1 s)
  Total from zero ≈ 76 s (after the one-time jar download)
```

---

## 3. Technology choices and why

| Technology | Role in Pulse | Why this, not something else |
|---|---|---|
| **Python 3.12** (via `uv`) | All code | A version PySpark and its dependencies support well. The system Python (3.14) is newer than the PySpark ecosystem is tested on. `uv` creates an isolated 3.12 env just for this project. |
| **yfinance** | Market data source | Free, no API key, has NSE data. It's *unofficial* and can break, so the code retries, validates, and has replay/sample files for offline use. |
| **pydantic** | Data contract (`Bar`) | Validates every record when it enters the system: types, price rules, timezone-aware timestamps. |
| **Redpanda** | Kafka-compatible broker | Speaks the Kafka protocol (same clients, same concepts), but it's one small binary, so it suits a 16 GB laptop. |
| **confluent-kafka** | Python Kafka client | The industry-standard client (built on librdkafka). Supports `acks=all` and the idempotent producer. |
| **Apache Spark 4.3 / PySpark** | All processing: streaming + batch | The core skill. Distributed DataFrames, Structured Streaming, window functions, SQL. Runs as `local[*]` on the laptop CPU. |
| **Parquet** | Storage format | Columnar (reads only the columns needed), compressed, typed, and the standard for data lakes. |
| **RustFS** | S3-compatible object storage | Local stand-in for AWS S3 / Azure Blob. MinIO was the plan, but MinIO stopped publishing public Docker images in 2025, so we switched. The code only speaks the **S3 API** (`s3a://`), so the swap changed zero Python. |
| **Docker Compose** | Runs the infrastructure | One file starts all services with pinned versions, the same way on any machine. |
| **pytest** | Tests | 34 unit tests (including Spark logic) + 3 integration tests on the real lake. |
| **ruff** | Linting | Catches mistakes and keeps style consistent. |

**Deliberately NOT used (and why you should say so in an interview):**

| Not used | Why |
|---|---|
| dbt | v1's README claimed it but never used it. Spark SQL does the transformations. Never list a tool you can't explain. |
| Airflow | Postponed to keep scope tight. `python -m pulse.pipeline` runs the steps in dependency order. Airflow would add scheduling, retries per task and backfills (see §13). |
| Delta Lake / Iceberg | Rebuilding Silver/Gold each run, plus dynamic partition overwrite, already makes re-runs safe. A table format would add ACID transactions and time travel, which is the next upgrade (§13). |
| Redis | No caching or fast shared-state need exists. Redis and Kafka solve different problems. |
| pandas (for processing) | Used only at the API boundary, because yfinance returns pandas. All real processing is Spark. |

---

## 4. Project structure

```
Pulse/
├── docker-compose.yml          Redpanda + Console + RustFS, pinned versions
├── Makefile                    short commands (make help)
├── pyproject.toml / uv.lock    dependencies, locked to exact versions
├── .env.example                config template (copy to .env, never commit .env)
├── data/
│   ├── reference/symbols.csv   10 NSE stocks + company + sector (dimension table)
│   ├── replay/bars_1m_*.jsonl  one recorded trading session (3,618 bars) for replay
│   └── sample/bars_1d.jsonl    2 years of daily bars (4,958) for offline runs
├── src/pulse/
│   ├── config.py               all settings from env vars (PULSE_*)
│   ├── logging_setup.py        one log format for every process
│   ├── schemas.py              the Bar data contract (pydantic) + event_id
│   ├── storage.py              creates the lake bucket (idempotent)
│   ├── sources/yahoo.py        Yahoo adapter: retries, validation, closed-bars-only
│   ├── ingestion/
│   │   ├── producer.py         Kafka producer: live / replay / record modes
│   │   └── batch_extract.py    daily history → Bronze (Spark, idempotent)
│   ├── streaming/
│   │   └── bronze_stream.py    Spark Structured Streaming: Kafka → Bronze
│   ├── transform/
│   │   ├── spark.py            builds the SparkSession (connectors, tuning)
│   │   ├── silver.py           Bronze → Silver
│   │   └── gold.py             Silver → Gold
│   ├── quality/
│   │   ├── rules.py            NSE market-hours rule (Python + Spark versions)
│   │   └── checks.py           data quality checks
│   ├── analytics/report.py     Spark SQL queries on Gold
│   └── pipeline.py             runs every step in order, in one Spark session
├── tests/                      unit tests + integration tests
└── docs/GUIDE.md               this file
```

**Design rule used everywhere:** each job has a **pure transformation function** (DataFrame in, DataFrame out, no reading or writing) and a separate `run()` that does the I/O. Pure functions can be unit-tested with tiny hand-made DataFrames, without Kafka or S3.

| Job | Pure function (tested) | I/O wrapper |
|---|---|---|
| Bronze stream | `to_bronze(kafka_df)` | `run()` |
| Silver | `build_silver(bronze_df, interval)` | `run()` |
| Gold | `daily_metrics()`, `intraday_summary()`, `symbol_performance()`… | `run()` |

---

## 5. Setup from zero

### 5.1 Prerequisites (one time)

| Need | Check | Install (CachyOS / Arch) |
|---|---|---|
| Docker + Compose | `docker --version` | `sudo pacman -Syu --needed docker docker-compose docker-buildx` then `sudo systemctl enable --now docker` and `sudo usermod -aG docker $USER`, then reboot |
| Java 17+ (Spark runs on the JVM) | `java -version` | `sudo pacman -S jdk17-openjdk` |
| uv (Python env manager) | `uv --version` | `sudo pacman -S uv` |
| make | `make --version` | `sudo pacman -S make` |

> **Why `-Syu` and never `-Sy <pkg>`?** On Arch-based systems, refreshing the package list without upgrading creates a *partial upgrade*, where new packages are built against libraries you don't have. We hit a related error (a mirror returned 404 because the local package list was 8 days old).

### 5.2 Project setup

```bash
cd ~/Wrks/Pros/Pulse
make setup        # = uv sync  + copy .env.example to .env if missing
```
- `uv sync` reads `pyproject.toml` + `uv.lock`, downloads Python 3.12 if needed, creates `.venv/` and installs the **exact** locked versions. Same versions on every machine.
- `.env` holds local settings and secrets. It's in `.gitignore`, so it's never committed. Change `PULSE_S3_SECRET_KEY` to any random string.

### 5.3 Start the infrastructure

```bash
make up           # = docker compose up -d --wait
```
- `up` creates the containers; `-d` runs them in the background; `--wait` blocks until the health checks pass.

| Service | URL / port | What it is |
|---|---|---|
| Redpanda | `localhost:19092` (Kafka API) | The broker |
| Redpanda Console | http://localhost:8081 | Web UI: topics, partitions, messages, offsets |
| RustFS S3 API | `localhost:9000` | Where Spark reads and writes the lake |
| RustFS Console | http://localhost:9001/rustfs/console/ | Web UI to browse the lake (log in with the S3 keys from `.env`) |

**Why two Kafka addresses?** Redpanda advertises `redpanda:9092` to other containers (Docker's internal network) and `localhost:19092` to your laptop. A client is told "connect to *this* address" after its first contact, so each side needs an address it can reach. That's why `docker-compose.yml` has two listeners.

---

## 6. Running the project

### 6.1 Everything in one command

```bash
make demo
```
That's `make up` → `make replay` → `make pipeline-offline`. It needs no internet except for the Docker images and Spark jars the first time.

> **First Spark run downloads ~650 MB** of connector jars (Kafka connector + `hadoop-aws` + AWS SDK) into `~/.ivy2`. That takes about 3.5 min once; later runs start in seconds.

### 6.2 Step by step (what `make demo` does, so you understand each piece)

**Step 1: publish data to Kafka**
```bash
make replay            # = python -m pulse.ingestion.producer replay --speed 600
```
Publishes the recorded session (`data/replay/`) at 600× speed (about 40 s).
✅ You should see: `created topic market.bars.1m` (first time only), then `replay done: sent=3618 failed=0`.

Producer modes:
| Command | When to use |
|---|---|
| `make live` | During NSE hours (Mon–Fri 09:15–15:30 IST): real bars every 60 s |
| `make record` | Any time after a session: saves that day's bars into `data/replay/` |
| `make replay` | Any time: replays recorded bars (demos, testing, nights and weekends) |

**Step 2: look inside Kafka**
```bash
docker exec -it pulse-redpanda rpk topic describe market.bars.1m -p
```
✅ Real output:
```
PARTITION  ...  HIGH-WATERMARK
0               1447      ← HDFCBANK, HINDUNILVR, INFY, TCS        (4 symbols)
1               1809      ← BHARTIARTL, ICICIBANK, ITC, RELIANCE, SBIN (5)
2               362       ← LT                                      (1)
```
1,447 + 1,809 + 362 = **3,618**, so nothing was lost. The uneven split is **key skew** (see §7.3).

**Step 3: run the pipeline**
```bash
make pipeline          # Yahoo for daily history (needs internet)
make pipeline-offline  # daily history from data/sample/ (offline)
```
✅ Real log (shortened):
```
▶ step extract   bronze daily: wrote 4958 rows -> s3a://pulse-lake/bronze/bars_1d
▶ step bronze    bronze stream finished: 1 micro-batch(es), 3618 new rows
▶ step silver    silver 1m: {'bronze': 7236, 'quarantined': 0, 'duplicates_removed': 3618, 'silver': 3618}
                 silver 1d: {'bronze': 4958, 'quarantined': 0, 'duplicates_removed': 0, 'silver': 4958}
▶ step gold      gold rows: {'daily_metrics': 4958, 'intraday_summary': 10, 'reconciliation': 10,
                             'symbol_performance': 10, 'sector_performance': 6}
▶ step quality   [PASS] x16
▶ step report    (6 tables, see §7.9)
```
(The `7236 → 3618` line comes from a run where the session had been replayed **twice** on purpose: Silver removed every duplicate.)

**Run only some steps:**
```bash
.venv/bin/python -m pulse.pipeline --steps silver gold quality report
.venv/bin/python -m pulse.pipeline --explain     # also print Spark's physical plan
```

**Step 4: continuous streaming (optional)**
```bash
make stream    # Kafka → Bronze every 30 s, runs until Ctrl+C
```
Run `make live` (market hours) or `make replay` in another terminal and watch micro-batches appear.

### 6.3 Where the data ends up

Browse http://localhost:9001/rustfs/console/ → bucket `pulse-lake`:
```
pulse-lake/
├── bronze/
│   ├── bars_1m/ingest_date=2026-11-21/part-*.parquet     (+ _spark_metadata/)
│   ├── bars_1d/ingest_date=2026-11-21/part-*.parquet
│   └── _rejects/bars_1d/…                                 rows that failed validation
├── silver/
│   ├── bars_1m/trade_date=2026-11-18/part-*.parquet       one folder per trading day
│   ├── bars_1d/part-*.parquet                             one file (small table)
│   └── _quarantine/…                                      invalid rows + reason
├── gold/
│   ├── daily_metrics/   intraday_summary/   reconciliation/
│   └── symbol_performance/   sector_performance/
└── _checkpoints/bronze_bars_1m/                           the streaming "bookmark"
    ├── offsets/0   commits/0   metadata   sources/
```

---

## 7. Layer by layer: concepts and code

### 7.1 The data contract ([schemas.py](../src/pulse/schemas.py))

**Problem:** the producer and the Spark jobs are separate programs. If the producer silently renames a field, every downstream job breaks. So both sides agree on an explicit, validated, versioned **contract**.

A **bar** = one stock over one time interval:

| Field | Meaning |
|---|---|
| `symbol` | e.g. `TCS.NS` |
| `interval` | `1m` or `1d` |
| `ts` | **event time**: when the bar started on the exchange, always UTC |
| `open/high/low/close` | prices during the interval |
| `volume` | shares traded |
| `produced_at` | **processing time**: when our code created the message |
| `schema_version` | lets the format evolve safely |
| `event_id` | `sha1(symbol | interval | ts)`, the **idempotency key** |

```python
class Bar(BaseModel):
    open: float = Field(gt=0)            # price must be > 0
    volume: int = Field(ge=0)            # volume >= 0 (0 is valid, see below)
    interval: Literal["1m", "1d"]        # only these two values

    @field_validator("ts", "produced_at")
    def _must_be_utc(cls, v):            # reject naive times, convert to UTC
    @model_validator(mode="after")
    def _ohlc_consistent(self):          # high >= low, open/close within [low, high]
```

**Key concepts**
- **Event time vs processing time.** Analytics must use *event time*. Data can arrive late or be replayed days later, and processing time would put it on the wrong day.
- **Store everything in UTC.** Convert to local time only for display or business rules (like trading hours).
- **Deterministic `event_id`.** The same bar always produces the same id, so if it's sent twice, Silver keeps one copy.
- **Zero volume is valid.** NSE runs a pre-open call auction, and Yahoo reports the 09:15 bar with volume 0 (we found 14 such bars). It's flagged in Silver, not rejected. *Unusual ≠ invalid.*

### 7.2 The source adapter ([sources/yahoo.py](../src/pulse/sources/yahoo.py))

An **adapter** turns an external API's format into our contract. Everything Yahoo-specific lives here.

| Function | What it does | Why |
|---|---|---|
| `_download()` | One `yf.download()` call for all symbols, wrapped in `@retry` (4 attempts, 2 s → 4 s → 8 s) | Fewer calls = fewer rate limits. **Exponential backoff** stops a struggling API from being hammered. |
| `frame_to_bars()` | pandas table → `Bar` objects; failures go to `rejects` with the error | One bad row can't kill the batch, and it isn't silently lost. |
| `fetch_intraday()` | Today's 1-minute bars, **only closed bars** (`ts + 1 min <= now`) | The current minute's bar keeps changing until the minute ends. |
| `fetch_daily()` | Daily bars for a date range | Feeds the batch path. |

**Real fix made here:** `yf.download()` returns **naive** daily dates (no timezone). The contract rejected all 4,960 rows. The fix belongs in the adapter, not in weakening the contract: a daily NSE date is an India date, so we attach `Asia/Kolkata`:
```python
if df.index.tz is None:
    df.index = df.index.tz_localize(market_tz)
```

### 7.3 Kafka and the producer ([ingestion/producer.py](../src/pulse/ingestion/producer.py))

#### Why Kafka at all?
Without a buffer, a slow or crashed consumer means lost data. (v1's consumer piled up 340 unsaved messages.) Kafka is a **durable, replayable log** between producers and consumers.

```
Producer ──writes──▶ [ topic: market.bars.1m ] ──read by──▶ Spark (keeps its own bookmark)
                       messages KEPT for 7 days            other consumers read independently
```

**Kafka vs a normal queue:** reading from a queue removes the message, like a to-do list. Reading from Kafka **doesn't**, like a logbook. Each consumer just tracks its **offset**. That's why we can replay, reprocess after fixing a bug, and run several consumers on the same data.

#### Core vocabulary
| Term | Meaning | In Pulse |
|---|---|---|
| Broker | The Kafka server | Redpanda container |
| Topic | A named log | `market.bars.1m`, `market.bars.dlq` |
| Partition | A topic split into ordered sub-logs, the unit of parallelism | 3 |
| Offset | A message's position inside a partition (0, 1, 2…) | Spark's checkpoint stores these |
| Key | Decides the partition: same key → same partition | the stock symbol |
| Consumer group | Consumers sharing the work; each partition goes to one member | Spark manages its own offsets via checkpoints |
| Retention | How long messages are kept | 7 days |
| DLQ | Dead-letter queue: where invalid records go | `market.bars.dlq` |

#### Partitioning and key skew (real data)
```
hash("TCS.NS") % 3 = 0      ┌ partition 0: HDFCBANK HINDUNILVR INFY TCS        → 1,447 msgs
hash("SBIN.NS") % 3 = 1     ├ partition 1: BHARTIARTL ICICIBANK ITC RELIANCE SBIN → 1,809 msgs
hash("LT.NS") % 3 = 2       └ partition 2: LT                                    →   362 msgs
```
- **Same key → same partition → ordered.** Kafka only guarantees order **within** a partition, so all TCS bars stay in time order.
- **Skew:** with 3 consumers, one gets 5× the work of another. Fixes at scale: more partitions, or a better-distributed key. At our size it doesn't matter, but it's a classic interview question.
- **Partition count is hard to change:** you can't decrease it, and increasing it changes `hash % n`, which breaks per-key ordering for existing keys.

#### Producer settings
```python
Producer({
    "acks": "all",                 # success only after the broker fully stored the message
    "enable.idempotence": True,    # broker drops duplicates caused by the producer's own RETRIES
    "linger.ms": 50,               # wait up to 50 ms to batch messages (throughput)
    "compression.type": "zstd",    # compress batches (less network/disk)
    "delivery.timeout.ms": 120000, # give up on a message after 2 min of retries
})
producer.produce(topic, key=symbol, value=json_bytes, on_delivery=callback)
```
- `produce()` is **asynchronous**: it queues the message and returns immediately.
- `on_delivery` is called later with success or error. `poll(0)` runs pending callbacks; `flush()` **waits** for everything queued.

#### Delivery semantics: the most important Kafka interview topic
| Semantics | Meaning | How you get it |
|---|---|---|
| At-most-once | May lose, never duplicates | Commit the offset *before* processing (v1 did this by accident) |
| **At-least-once** | Never loses, may duplicate | Retry until acknowledged, commit after processing |
| Exactly-once (effective) | No loss, no duplicates in the *result* | At-least-once + **idempotent processing** (dedupe by `event_id`) |

**Pulse is at-least-once with idempotent processing.** The producer keeps `last_sent` per symbol **in memory**. After a restart it re-sends the day's bars (duplicates), and Silver removes them by `event_id`.

> ⚠️ The idempotent producer only removes duplicates from **its own network retries**. If *our application* sends the same bar twice, Kafka stores it twice. We proved this: replaying twice gave 7,236 messages.

#### Other details worth knowing
- `ensure_topics()` is **idempotent**: running it twice is a no-op. (It crashed on the second run until we fixed it; see §10.)
- `auto_create_topics_enabled=false` (set in `docker-compose.yml`): a misspelled topic name now **errors** instead of silently creating a 1-partition topic.
- **Graceful shutdown:** `SIGTERM` (sent by `docker stop`) and `SIGINT` (Ctrl+C) set `_running = False`. The loop finishes its cycle and `flush()`es instead of dying with queued messages.

### 7.4 Bronze, streaming: Spark Structured Streaming

File: [streaming/bronze_stream.py](../src/pulse/streaming/bronze_stream.py)

**Idea:** a stream is an **unbounded table**. New Kafka messages are new rows. You write normal DataFrame code, and Spark runs it on each **micro-batch** of new rows.

```python
spark.readStream.format("kafka")               # readStream, not read: a streaming DataFrame
     .option("subscribe", "market.bars.1m")
     .option("startingOffsets", "earliest")     # ONLY used on the very first run
     .option("maxOffsetsPerTrigger", 50000)     # cap batch size (protects memory on big backlogs)
     .load()
# Kafka gives fixed columns: key, value (bytes), topic, partition, offset, timestamp

to_bronze(df):                                  # keep the payload RAW, add lineage
    value.cast("string") AS raw_value           # untouched JSON
    partition, offset                           # identify the message uniquely
    current_timestamp() AS ingested_at, ingest_date

.writeStream.format("parquet")
    .option("checkpointLocation", ".../_checkpoints/bronze_bars_1m")   # THE BOOKMARK
    .partitionBy("ingest_date")
    .trigger(availableNow=True)   # or processingTime="30 seconds"
```

#### The checkpoint: how Spark never loses or repeats data
```
 Micro-batch N:
  1. plan:    write offsets/N    {"market.bars.1m": {"0":1447, "1":1809, "2":362}}
  2. process: read those Kafka offsets → write Parquet files
  3. commit:  write commits/N    + the file sink logs the files in _spark_metadata/

 Crash during step 2?  On restart: offsets/N exists but commits/N doesn't
                       → redo batch N with the SAME offsets
                       → half-written files are not in _spark_metadata → readers ignore them
 Result: each Kafka message lands in Bronze EXACTLY ONCE.
```
This is the real checkpoint file from our run, `offsets/0`:
```json
{"market.bars.1m": {"0": 1447, "1": 1809, "2": 362}}
```
**Proof it works:** run the stream again with nothing new in Kafka → `0 new rows`.

#### Triggers
| Trigger | Behaviour | Use |
|---|---|---|
| `processingTime="30 seconds"` | Runs forever, one micro-batch every 30 s | `make stream` (continuous) |
| `availableNow=True` | Processes everything waiting (in several batches if needed), then stops | `pipeline`: **streaming code used as an incremental batch job**, ideal for a scheduler |

**Lazy evaluation:** `to_bronze()` does no work. It only builds a plan. Nothing runs until `.start()` (streams) or an **action** (`count()`, `write`, `collect()`) in batch jobs.

### 7.5 Bronze, batch: daily history

File: [ingestion/batch_extract.py](../src/pulse/ingestion/batch_extract.py)

```
Yahoo (2 years, 10 symbols) → Bar validation → JSON → Spark DataFrame → bronze/bars_1d/ingest_date=…/
                                    └─ rejects → bronze/_rejects/bars_1d/   (batch "DLQ")
```
- **End date = today, exclusive**, so it covers up to yesterday. Today's daily bar is still forming (the same "closed bars only" rule as the producer).
- **Idempotent re-runs:** `mode("overwrite")` + `spark.sql.sources.partitionOverwriteMode=dynamic` → only the `ingest_date` partition being written is replaced. We ran the extract 3 times and Silver still had exactly 4,958 rows.
- **Fails loudly:** 0 valid bars → `RuntimeError`, instead of overwriting good data with an empty partition.
- **Real rejects found:** 2 rows (BHARTIARTL and HINDUNILVR on 2026-11-20) had **no close price**, because Yahoo hadn't finalised them. They're stored in `_rejects`, so those two stocks' latest date is 2026-11-19.
- `--save-sample` writes `data/sample/bars_1d.jsonl`, so anyone can run offline with `--source sample`.

### 7.6 Silver: clean, validate, deduplicate

File: [transform/silver.py](../src/pulse/transform/silver.py)

```
bronze (raw JSON)
  │ 1. parse_bronze     from_json(raw_value, schema) → typed columns; cast ts to timestamp
  │ 2. with_dq_issue    first rule broken → dq_issue column (NULL = OK)
  │         ├── dq_issue NOT NULL ──▶ silver/_quarantine  (raw_value + reason)
  │ 3. dedupe_latest    row_number() over (partition by event_id order by produced_at desc)
  │ 4. enrich           trade_date (India date), in_session, is_zero_volume
  ▼
silver/bars_1m (partitioned by trade_date)      silver/bars_1d (one file)
```

**1. Parsing with an explicit schema**
```python
F.from_json("raw_value", "event_id STRING, symbol STRING, ..., open DOUBLE, volume BIGINT, ...")
```
An explicit schema is faster than inference (Spark doesn't scan the data to guess), and it's a contract. Malformed JSON gives NULLs, which rule 1 catches.

**2. Validation rules (quarantine, don't drop)**

| dq_issue | Rule |
|---|---|
| `unparseable_or_missing_key` | event_id / symbol / ts is NULL |
| `unexpected_interval` | a `1d` bar inside the `1m` table, or the reverse |
| `missing_value` | any price or volume is NULL |
| `non_positive_price` | any price ≤ 0 |
| `high_below_low` | high < low |
| `open_close_outside_range` | open or close not between low and high |
| `negative_volume` | volume < 0 |

**Missing values are quarantined, not filled.** Forward-filling a price would invent a trade that never happened. For market data, an honest gap beats a fabricated number.

**3. Deduplication: why `row_number()` and not `dropDuplicates()`**
```python
w = Window.partitionBy("event_id").orderBy(F.col("produced_at").desc(), F.col("ingested_at").desc())
df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1")
```
```
event_id   produced_at  close  rn
abc123     03:50        100.7   1   ← kept (newest version)
abc123     03:46        100.5   2   ← removed
```
`dropDuplicates(["event_id"])` keeps an **arbitrary** row, so two runs could keep different versions. `row_number()` over an ordered window is **deterministic**.

**4. The timezone trap: `trade_date` must use India time**
```
Daily bar for 21 Nov:  2026-11-21 00:00 IST  =  2026-11-20 18:30 UTC
to_date(ts)                                  → 2026-11-20   ✗ wrong by one day
to_date(from_utc_timestamp(ts, "Asia/Kolkata")) → 2026-11-21   ✓
```

**Market-hours rule** ([quality/rules.py](../src/pulse/quality/rules.py)) is written twice, for Python and for Spark, and a test checks that both agree:
```python
# Python                                   # Spark
local = ts.astimezone(NSE_TZ)              local = F.from_utc_timestamp(col, "Asia/Kolkata")
local.weekday() < 5                        F.dayofweek(local).between(2, 6)   # Sun=1..Sat=7 !
time(9,15) <= local.time() < time(15,30)   555 <= hour*60+minute < 930
```
Note that `weekday()` (Mon=0) and Spark's `dayofweek()` (Sun=1) number days differently.

**Writing**
- `silver/bars_1m`: `repartition("trade_date")` + `partitionBy("trade_date")` gives one folder per day and one file per folder.
- `silver/bars_1d`: only ~5,000 rows. Partitioning by date would create ~500 folders of 10 rows each (the **small-files problem**: slow listing, poor compression). One file via `coalesce(1)`.
- **Full rebuild each run** is simple and always correct at this size. At terabyte scale you'd process only new `ingest_date` partitions incrementally.

### 7.7 Gold: analytics with window functions

File: [transform/gold.py](../src/pulse/transform/gold.py)

#### Window functions, the core Spark skill here
A window function computes a value for each row **using neighbouring rows**, without collapsing them (unlike `groupBy`).

```python
w = Window.partitionBy("symbol").orderBy("trade_date")
```
- `partitionBy("symbol")` means "do it separately per stock". Spark **shuffles** so each symbol's rows sit together.
- `orderBy("trade_date")` processes rows in date order.

```
symbol  trade_date  close   lag(close)  daily_return   ma_20 (last 20 rows incl. today)
TCS     day 1       100     NULL        NULL           NULL   ← warm-up: window not full
TCS     day 2       110     100         +10.0%         NULL
TCS     day 3        99     110         -10.0%         NULL
…
TCS     day 20      …       …           …              avg(day 1..20)   ← first valid value
INFY    day 1       …       NULL        NULL           ← windows NEVER cross into another symbol
```

| Gold column | Spark code | Meaning |
|---|---|---|
| `daily_return` | `close / lag(close) - 1` | % change from the previous day |
| `log_return` | `log(close / lag(close))` | used for volatility (adds up over time) |
| `ma_20`, `ma_50` | `avg(close).over(w.rowsBetween(-19, 0))` | 20/50-day moving averages; NULL until the window is full |
| `volatility_20d` | `stddev(log_return) over 20 rows * sqrt(252)` | annualised risk |
| `avg_volume_20d` | `avg(volume).over(w.rowsBetween(-20, -1))` | the 20 days **before** today (excludes today) |
| `volume_ratio`, `is_volume_spike` | `volume / avg_volume_20d`, `>= 2` | unusual trading activity |
| `ret_21d / 63d / 252d` | `close / lag(close, n) - 1` | ~1 month, 3 months, 1 year returns |
| `ytd_return` | `first(close)` over (symbol, year) | year-to-date return |
| `running_max_close`, `drawdown` | `max(close)` over unbounded preceding | % below the highest price so far |
| `trend` | `ma_20 > ma_50` | up / down |
| `is_suspicious_move` | `abs(daily_return) > 20%` | catches stock splits (we store *unadjusted* prices) |

**Frame types:** `rowsBetween(-19, 0)` = "the 19 previous rows plus this one". `Window.unboundedPreceding` = "from the first row".

#### Gold tables

| Table | Built with | Rows |
|---|---|---|
| `daily_metrics` | window functions over `silver/bars_1d` | 4,958 |
| `intraday_summary` | `groupBy(symbol, trade_date).agg(...)` on 1-minute bars | 10 |
| `reconciliation` | join intraday_summary ⋈ daily bars | 10 |
| `symbol_performance` | latest row per symbol (`row_number`) ⋈ risk aggregates ⋈ **broadcast** dim_symbols | 10 |
| `sector_performance` | `groupBy(sector)` + `max_by(symbol, ret_252d)` | 6 |

**Aggregation:** `intraday_summary` turns ~362 one-minute rows into 1 row per stock per day:
```python
.groupBy("symbol", "trade_date").agg(
    F.min_by("open", "ts"),     # open of the EARLIEST bar
    F.max("high"), F.min("low"),
    F.max_by("close", "ts"),    # close of the LATEST bar
    F.sum("volume"),
    sum(typical_price * volume) / sum(volume) AS vwap,   # volume-weighted average price
    F.count("*") AS bar_count)  # completeness = bar_count / 375
```

**Reconciliation**, a check that two pipelines agree: comparing our 1-minute-derived numbers with Yahoo's daily bar for 18 Nov gave
- close difference **0.0%** for all 10 stocks ✓
- volume coverage **92–98%**: the 1-minute data misses ~13 bars per day and part of the auction volume. A real data-quality insight, found by building a check, not by assuming.

### 7.8 Data quality checks

File: [quality/checks.py](../src/pulse/quality/checks.py). They run after Gold on every pipeline run.

| Severity | Effect | Examples |
|---|---|---|
| **ERROR** | Raises an exception, the pipeline **fails** (non-zero exit) | not empty, `event_id` unique, no null keys or prices, high ≥ low, one bar per (symbol, ts), Gold row count = Silver row count, volatility ≥ 0 |
| **WARN** | Logged only | >20% daily moves (splits?), session completeness < 90%, bars outside NSE hours, reconciliation close difference > 1% |

Real result: **16/16 PASS**. Why fail loudly? A scheduler (like Airflow later) marks the run red and stops downstream consumers from reading bad data. Silent garbage is the worst failure mode in data engineering.

### 7.9 Analytics with Spark SQL

File: [analytics/report.py](../src/pulse/analytics/report.py)

Each Gold folder is registered as a SQL view, then queried with plain SQL:
```python
spark.read.parquet("s3a://pulse-lake/gold/daily_metrics").createOrReplaceTempView("daily_metrics")
spark.sql("SELECT ... FROM daily_metrics ...")
```
Spark SQL and the DataFrame API compile to the **same** optimised plan (Catalyst), so you can choose whichever is clearer for each problem.

**Real results** (data up to 20 Nov 2026):

**1. Symbol leaderboard**
| symbol | sector | 1y % | YTD % | volatility % | max drawdown % |
|---|---|---|---|---|---|
| SBIN.NS | Financials | 22.1 | 18.3 | 22.0 | -23.5 |
| ICICIBANK.NS | Financials | 18.3 | 22.6 | 18.9 | -19.0 |
| LT.NS | Industrials | 5.2 | 1.7 | 24.3 | -24.3 |
| BHARTIARTL.NS | Communication Services | 3.4 | 1.6 | 21.6 | -19.5 |
| RELIANCE.NS | Energy | -8.8 | -12.6 | 22.2 | -27.3 |
| HDFCBANK.NS | Financials | -14.3 | -14.6 | 20.2 | -32.8 |
| HINDUNILVR.NS | Consumer Staples | -14.4 | -10.7 | 21.0 | -32.6 |
| TCS.NS | Information Technology | -23.5 | -26.4 | 25.8 | -55.7 |
| ITC.NS | Consumer Staples | -26.3 | -17.8 | 20.3 | -45.8 |
| INFY.NS | Information Technology | -28.8 | -32.6 | 28.8 | -50.7 |

**2. Sector performance:** Financials best (+8.7% avg 1y), IT worst (-26.1%, and the most volatile at 27.3%).

**3. Moving-average crossovers (last 90 days)** use `LAG(trend) OVER (PARTITION BY symbol ORDER BY trade_date)` to find days where the trend flipped. 8 of the 10 stocks had a bearish "death cross" between 3 Sep and 21 Oct; all 10 then had a bullish "golden cross" between 27 Oct and 17 Nov.

**4. Volume spikes (last 30 days):** biggest was RELIANCE on 2026-11-02, with **5.8×** its 20-day average volume and +11.6% that day.

**5. Intraday quality (18 Nov):** 361–362 of 375 bars (≈96.5% complete), 1–2 zero-volume bars per stock (the opening auction).

**6. Reconciliation:** close matches the daily bar exactly; 1-minute volume covers 92–98% of daily volume.

> These are pipeline outputs, not investment advice.

---

## 8. Spark performance concepts used

All of these are set in [transform/spark.py](../src/pulse/transform/spark.py) or used in the jobs. Each one is a likely interview question.

| Concept | Where | What and why |
|---|---|---|
| **`local[*]`** | `spark_master` | Driver and executors in **one JVM** using all 12 CPU threads. Same code runs on a cluster by changing the master URL. Honest caveat: this is single-machine Spark, not a cluster. |
| **Lazy evaluation** | everywhere | Transformations build a plan; **actions** (`count`, `write`, `collect`) execute it. Spark can then optimise the whole plan at once. |
| **Shuffle** | `groupBy`, `Window.partitionBy`, joins | Rows move between partitions so that equal keys meet. It's the most expensive operation (disk + network). Shows as **`Exchange`** in plans. |
| **`spark.sql.shuffle.partitions = 12`** | config | Default 200 would mean 200 tiny tasks for MBs of data. Match it to your cores and data size. |
| **AQE** (Adaptive Query Execution) | `spark.sql.adaptive.enabled` | Re-plans **at runtime** with real sizes: merges tiny shuffle partitions, switches to broadcast joins, splits skewed partitions. |
| **Broadcast join** | `F.broadcast(dim)` in `symbol_performance` | Copies the small table (10 rows) to every executor, so the big side isn't shuffled. Spark also auto-broadcasts tables under `spark.sql.autoBroadcastJoinThreshold` (10 MB). |
| **Caching** | `metrics.cache()` in Gold, `bronze.cache()` in Silver | A DataFrame used by several outputs would otherwise be **recomputed** from scratch for each one (lazy evaluation). Always `unpersist()` afterwards. |
| **Partition pruning** | `partitionBy("ingest_date"/"trade_date")` | Filters on the partition column only read the matching folders. |
| **`coalesce` vs `repartition`** | Gold writes / Silver 1m | `coalesce(n)` reduces partitions **without a shuffle** (merges). `repartition(col)` **shuffles** to group by a column. |
| **Small files** | `silver/bars_1d` not partitioned | Thousands of tiny files are slow to list and read. Size partitions to the data. |
| **Dynamic partition overwrite** | config | `overwrite` replaces only the partitions present in the DataFrame, so re-runs are safe. |
| **Explicit schemas** | `from_json` in Silver | No inference pass, and the types are guaranteed. |

### Reading a physical plan
```bash
make explain        # = python -m pulse.transform.gold --explain
```
Real output (trimmed), read **bottom-up**:
```
BroadcastHashJoin LeftOuter BuildRight      ← join with dim_symbols: small side broadcast (our hint)
:- BroadcastHashJoin Inner BuildRight       ← join latest ⋈ risk: NO hint, Spark chose broadcast itself
:  :- Filter (rn = 1)
:  :  +- Window                             ← row_number() for "latest row per symbol"
:  :     +- WindowGroupLimit                ← Spark optimisation: keeps only top-1 per group early
:  :        +- Exchange                     ← SHUFFLE by symbol
:  :           +- InMemoryTableScan         ← reading the CACHED daily_metrics
:  :              +- Window … Sort
:  :                 +- Exchange            ← shuffle by symbol for the window functions
:  :                    +- Scan parquet     ← reading silver/bars_1d from S3
:  +- BroadcastExchange
:     +- HashAggregate ← Exchange ← HashAggregate   ← groupBy in two phases (partial → final)
```
The second broadcast had no hint: the `risk` table is tiny, so Spark broadcast it on its own (it's below `spark.sql.autoBroadcastJoinThreshold`, and AQE can also convert joins at runtime).

What to look for: **`Exchange`** = a shuffle (expensive), **`BroadcastHashJoin`** = good for small tables, **`InMemoryTableScan`** = the cache is used, **`HashAggregate` → `Exchange` → `HashAggregate`** = a two-phase aggregation (partial sums before the shuffle, so less data moves).

**Spark UI:** while a job runs, open http://localhost:4040 to see jobs, stages, shuffles, task times and cached data.

---

## 9. Testing

```bash
make test        # 34 unit tests, ~14 s, no Docker needed
make test-int    # 3 integration tests on the real lake (needs make up + make pipeline)
make lint        # ruff static checks
```

| Test file | What it proves |
|---|---|
| `test_schemas_and_source.py` | Contract rejects bad bars, accepts zero volume, converts to UTC, `event_id` is deterministic; adapter splits valid/rejects, drops still-open bars, localises naive daily dates |
| `test_exercise_market_hours.py` | NSE session rule at the edges: 09:14, 09:15, 15:29, 15:30, UTC input, Saturday |
| `test_producer.py` | `ensure_topics` creates missing topics and is **idempotent** (uses a fake admin client) |
| `test_spark_transforms.py` | Bronze lineage; Silver dedupe keeps the newest version; each quarantine reason; India `trade_date`; Spark and Python session rules agree; returns, drawdown, MA warm-up; windows don't mix symbols; VWAP and OHLC aggregation |
| `test_integration_pipeline.py` (`-m integration`) | Every Gold table exists with rows; Silver has no duplicates; all ERROR quality checks pass on the real lake |

**Testing techniques used:**
- **Pure functions + tiny DataFrames:** Spark logic is tested with 2–3 hand-made rows, where the right answer can be computed by hand.
- **Mocking:** `monkeypatch` replaces Yahoo and the Kafka admin client with fakes, so tests are fast and offline.
- **One SparkSession per test run** (`conftest.py`, `scope="session"`), because starting a JVM takes seconds.
- **Parametrised tests:** one test function, many inputs (`@pytest.mark.parametrize`).
- **Markers:** integration tests are excluded by default (`addopts = -m 'not integration'` in `pyproject.toml`).

---

## 10. Real bugs we hit and what they teach

These happened while building Pulse. They make great interview stories: *"tell me about a bug you fixed"*.

| # | Symptom | Root cause | Fix | Lesson |
|---|---|---|---|---|
| 1 | v1 DAG would write the same 10 rows every hour | `KafkaConsumer` with no `group_id` → no committed offsets → always reads from offset 0 | v2: Spark checkpoints | Offsets must be tracked, and commits tied to successful writes |
| 2 | v1 consumer stuck at "Added 340/5" | Write failed after `append`, so `batch = []` never ran; `except: print` hid it | v2: no half-finished state, `log.exception` | Never swallow errors in pipelines |
| 3 | `minio/minio` image not found | MinIO stopped publishing public images (2025) | RustFS, same S3 API | Code against protocols, not products |
| 4 | pacman 404 on signature files | Local package list 8 days old, mirror had moved on | re-rank mirrors + `pacman -Syu` | Stale metadata vs a changing source |
| 5 | `ValueError: Expected non-empty list of NewTopic` | `ensure_topics` assumed a first run | early return + test | Setup code must be idempotent |
| 6 | `PYTHON_VERSION_MISMATCH: worker 3.14, driver 3.12` | Spark launched workers with system `python3` | `PYSPARK_PYTHON = sys.executable` | Driver and workers must run the same Python |
| 7 | 0 daily bars, 4,960 rejects | `yf.download` returns naive dates; the contract rejected them | localise in the adapter | Fix at the boundary; don't weaken the contract |
| 8 | Extract overwrote the sample file with nothing | No guard for "0 valid rows" | raise `RuntimeError` | Fail loudly rather than write empty data |
| 9 | 2 daily bars with no close (20 Nov) | Yahoo hadn't finalised them | stored in `_rejects`; those symbols' latest date is 19 Nov | Validation earns its keep on real data |
| 10 | Spark tests failed on timestamp equality | PySpark reads **naive** Python datetimes in the laptop's local zone (IST) | tz-aware datetimes in tests | Naive timestamps are bugs waiting to happen |
| 11 | `make demo` could run steps in parallel | Shell alias `make` → `make -j12` | `.NOTPARALLEL:` in Makefile | Know your environment |
| 12 | Topics could be auto-created by typos | Redpanda dev default `auto_create_topics_enabled=true` | `--set ...=false` in compose | Prefer explicit over implicit infrastructure |

---

## 11. Command cheat sheets

### make (project commands)
```bash
make help              # list everything
make setup             # venv + dependencies + .env
make up / make down    # start / stop the stack (down keeps data)
make reset             # DANGER: delete containers AND volumes (all Kafka + lake data)
make status            # containers + topic offsets
make replay | live | record     # producer modes
make stream            # continuous Kafka → Bronze
make extract           # daily history → Bronze
make pipeline          # everything (Yahoo)       make pipeline-offline  # everything (sample)
make report            # Spark SQL report         make explain           # physical plan
make test | test-int | lint
make demo              # from zero: up + replay + pipeline-offline
```

### Docker
| Command | What it does |
|---|---|
| `docker compose up -d --wait` | Create and start all services in the background, wait until healthy |
| `docker compose ps` | List this project's containers and their status/health |
| `docker compose logs -f redpanda` | Follow one service's logs (Ctrl+C stops watching, not the service) |
| `docker compose stop` / `start` | Pause / resume (containers and data kept) |
| `docker compose down` | Remove containers and the network; **volumes kept** |
| `docker compose down -v` | Also remove **volumes**, deleting all data |
| `docker exec -it pulse-redpanda <cmd>` | Run a command **inside** a running container |
| `docker volume ls` | List named volumes (`pulse_redpanda_data`, `pulse_objectstore_data`) |
| `docker images` | Downloaded images |

Concepts: **image** = packaged program (like a class); **container** = running instance (like an object); **volume** = storage that survives container deletion; **port mapping** `19092:19092` = laptop port → container port; **network**: containers reach each other by service name (`redpanda:9092`).

### rpk (Redpanda CLI, runs inside the container)
```bash
docker exec -it pulse-redpanda rpk cluster info                       # brokers
docker exec -it pulse-redpanda rpk topic list                         # topics + partition counts
docker exec -it pulse-redpanda rpk topic describe market.bars.1m -p   # per-partition offsets
docker exec -it pulse-redpanda rpk topic consume market.bars.1m --num 5                     # read 5 messages
docker exec -it pulse-redpanda rpk topic consume market.bars.1m --format '%p | %o | %k\n' --num 20  # partition|offset|key
docker exec -it pulse-redpanda rpk topic delete market.bars.1m        # delete topic (and its data)
docker exec -it pulse-redpanda rpk cluster config get auto_create_topics_enabled
```

### Running modules directly
```bash
.venv/bin/python -m pulse.ingestion.producer {live|replay|record} [--speed N]
.venv/bin/python -m pulse.streaming.bronze_stream [--available-now]
.venv/bin/python -m pulse.ingestion.batch_extract [--source yahoo|sample] [--years 2] [--save-sample]
.venv/bin/python -m pulse.transform.silver
.venv/bin/python -m pulse.transform.gold [--explain]
.venv/bin/python -m pulse.quality.checks
.venv/bin/python -m pulse.analytics.report
.venv/bin/python -m pulse.pipeline [--steps ...] [--daily-source yahoo|sample] [--explain]
```
(On fish, `source .venv/bin/activate.fish` lets you type `python` instead of `.venv/bin/python`.)

### Configuration (`.env`, all prefixed `PULSE_`)
| Variable | Default | Meaning |
|---|---|---|
| `PULSE_KAFKA_BOOTSTRAP` | `localhost:19092` | Kafka address (inside Docker it would be `redpanda:9092`) |
| `PULSE_S3_ENDPOINT` | `http://localhost:9000` | Object storage S3 API |
| `PULSE_S3_ACCESS_KEY` / `PULSE_S3_SECRET_KEY` | — | Storage credentials (also used by docker-compose) |
| `PULSE_LAKE_BACKEND` | `s3` | `s3` (RustFS) or `local` (folder `./lake`) |
| `PULSE_SPARK_DRIVER_MEMORY` | `2g` | Spark memory |
| `PULSE_SPARK_SHUFFLE_PARTITIONS` | `12` | Shuffle parallelism |
| `PULSE_LOG_LEVEL` | `INFO` | `DEBUG` shows every Kafka delivery |

---

## 12. Interview preparation

### Explain the project in 60 seconds
> "Pulse ingests NSE market data on two paths. One-minute bars are validated against a pydantic contract and published to a Kafka-compatible broker, keyed by symbol so each stock's bars stay ordered. A PySpark Structured Streaming job lands them in a Bronze layer on S3-compatible storage, using checkpoints for exactly-once file output. Two years of daily history comes in through a batch job, because bounded data doesn't need a broker. Spark batch jobs then build Silver, where JSON is parsed with an explicit schema, invalid rows are quarantined with a reason, and duplicates are removed deterministically with row_number over a deterministic event_id. Gold uses window functions for returns, moving averages, volatility and drawdown, a groupBy for intraday VWAP, and a broadcast join for sector data. Automated quality checks fail the run on errors, and Spark SQL produces the analytics. Everything is tested with pytest, including Spark logic on small DataFrames."

### Kafka
1. **What's a partition, and why did you key by symbol?** It's an ordered sub-log and the unit of parallelism. Keying by symbol keeps each stock's bars in one partition, so they stay in order.
2. **What problem did keying by symbol cause?** Key skew: 4/5/1 symbols per partition. Fix with more partitions or a different key.
3. **What does `enable.idempotence` protect against, and what doesn't it?** It removes duplicates from producer retries, not from the application re-sending (our restart case).
4. **What delivery semantics does Pulse have?** At-least-once delivery plus idempotent processing (dedupe on `event_id`) gives effectively-exactly-once results.
5. **A consumer crashes. What happens?** Spark restarts from the checkpoint's offsets and re-processes the uncommitted batch. The file sink log prevents duplicate files.
6. **Kafka vs a queue (e.g. RabbitMQ)?** Kafka keeps messages after they're read (retention) and consumers track offsets, so it supports replay and multiple independent readers.
7. **Why not Kafka for the 2-year history?** It's bounded, bulk data, so a batch job is simpler. Kafka is for continuous streams and decoupling.

### Spark
1. **What's lazy evaluation and why is it useful?** Transformations build a plan; actions run it. Spark optimises the whole plan (predicate pushdown, column pruning, join choice).
2. **What causes a shuffle in your jobs?** `Window.partitionBy("symbol")`, `groupBy`, non-broadcast joins, `repartition`. You see it as `Exchange` in the plan.
3. **Why a broadcast join for `dim_symbols`?** 10 rows: copying it to every executor avoids shuffling the large side.
4. **Why cache `daily_metrics`?** Several outputs use it. Without caching, the window pipeline is recomputed for each one.
5. **`row_number` vs `dropDuplicates`?** `row_number` with an ordered window is deterministic; `dropDuplicates` keeps an arbitrary row.
6. **`coalesce` vs `repartition`?** coalesce reduces partitions without a shuffle; repartition shuffles (can increase partitions, or group by a column).
7. **What is AQE?** Runtime re-optimisation using real statistics: coalesces shuffle partitions, switches joins to broadcast, handles skew.
8. **Why `shuffle.partitions = 12` and not 200?** Small data on 12 cores; 200 means mostly-empty tasks and scheduling overhead.
9. **What's a checkpoint in Structured Streaming?** The stored offsets plus commit log that make a streaming query restartable and exactly-once with idempotent sinks.
10. **`availableNow` vs `processingTime` trigger?** Drain-then-stop (schedulable incremental batch) vs continuous micro-batches.
11. **What's the small-files problem?** Many tiny files mean slow listing and overhead. That's why `bars_1d` isn't partitioned by date.

### Data modelling and quality
1. **Why Bronze/Silver/Gold?** Raw replayable history → trusted, cleaned data → business-ready tables. Each layer can be rebuilt from the one before.
2. **Why quarantine instead of drop?** Auditability: you can see what was rejected and why, and reprocess after a fix.
3. **Why not forward-fill missing prices?** It fabricates trades. Gaps are honest.
4. **Event time vs processing time?** When it happened vs when we saw it. Analytics use event time.
5. **What timezone bug did you avoid?** Daily bars at 00:00 IST are 18:30 UTC the previous day, so `trade_date` must be computed in India time.

### Scenario questions
- *"Yahoo is down for an hour during market hours."* The producer retries with backoff, logs the failure and tries again next cycle. Bars are re-fetched later because `period=1d` returns the whole day. Nothing downstream breaks; Silver dedupes any overlap.
- *"You found a bug in the Gold volatility formula."* Fix it and re-run `--steps gold`. Bronze and Silver are untouched. Gold is fully rebuilt (overwrite).
- *"You need 100 symbols instead of 10."* Same code. Consider more partitions for parallelism, and more `shuffle.partitions`.
- *"Data volume grows 1000×."* Incremental Silver (only new `ingest_date` partitions), a table format (Delta/Iceberg) for MERGE-based upserts, a real cluster instead of `local[*]`.

### Common junior mistakes (that this project avoids)
- `:latest` Docker tags · committed credentials · `except: pass` · naive timestamps · no message key · `dropDuplicates` for "latest version" · `collect()` on big data · default 200 shuffle partitions on tiny data · over-partitioning small tables · listing tools you didn't use.

---

## 13. Limitations and next steps

Being honest about these is a strength in interviews.

| Limitation | Next step |
|---|---|
| Steps are run by `pulse.pipeline`, not a scheduler | **Apache Airflow** DAG: daily schedule, per-task retries, backfills with logical dates, alerting. Every step is already idempotent, which Airflow needs. |
| Local only | **Azure ADLS Gen2**: switch `s3a://` to `abfss://` (hadoop-azure connector) with a service principal and RBAC role `Storage Blob Data Contributor`. |
| Plain Parquet | **Delta Lake / Iceberg**: ACID writes, `MERGE` for incremental upserts, time travel, schema evolution. |
| `local[*]` Spark | Run the same jobs on Databricks or a Spark cluster by changing `spark_master`. |
| Unadjusted prices | Add `adj_close` (schema_version 2) so splits and dividends don't distort returns; `is_suspicious_move` flags them for now. |
| No holiday calendar | Add an NSE holiday table so expected-bars and session checks know non-trading days. |
| JSON messages | Avro/Protobuf + Schema Registry (Redpanda includes one) for enforced schema evolution. |
| Producer memory state | Store `last_sent` durably (or read the latest offset) to reduce re-sends after restarts. |

---

## 14. Glossary

| Term | Meaning |
|---|---|
| **OHLCV** | Open, High, Low, Close, Volume: the fields of a price bar |
| **VWAP** | Volume-weighted average price: Σ(price × volume) / Σ volume |
| **Drawdown** | % below the highest price reached so far |
| **Volatility (annualised)** | stddev of daily log returns × √252 |
| **Golden / death cross** | 20-day MA crosses above / below the 50-day MA |
| **Idempotent** | Running it twice gives the same result as running it once |
| **Lineage** | Where a row came from (topic, partition, offset, ingest time) |
| **DLQ / quarantine** | Where invalid records go, kept for inspection |
| **Micro-batch** | One small batch of new rows processed by a streaming query |
| **Checkpoint** | Saved offsets and commit log of a streaming query |
| **Shuffle** | Redistributing rows across partitions by key; expensive |
| **Broadcast** | Copying a small table to every executor to avoid a shuffle |
| **AQE** | Adaptive Query Execution: runtime re-optimisation |
| **Partition (Kafka)** | Ordered sub-log of a topic |
| **Partition (Spark / lake)** | A chunk of a DataFrame / a folder of a table (`trade_date=…`) |
| **Event time** | When something happened in the real world |
| **Processing time** | When the system processed it |
