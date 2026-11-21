"""Batch path: historical DAILY bars -> Bronze (no Kafka).

Why not Kafka? History is bounded (we know exactly which days we want) and arrives in bulk.
A broker adds nothing here; a batch job that can be re-run safely is the right tool.

Idempotency: output is partitioned by ingest_date and written with DYNAMIC partition
overwrite, so running the job twice on the same day REPLACES that day's folder
instead of appending a second copy.

    python -m pulse.ingestion.batch_extract                  # Yahoo, last 2 years
    python -m pulse.ingestion.batch_extract --source sample  # offline: data/sample/bars_1d.jsonl
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, date, datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from pulse.config import PROJECT_ROOT, Settings, get_settings
from pulse.ingestion.producer import load_symbols
from pulse.logging_setup import setup_logging
from pulse.quality.rules import NSE_TZ
from pulse.schemas import Bar
from pulse.sources import yahoo

log = setup_logging("pulse.extract")
SAMPLE_FILE = PROJECT_ROOT / "data" / "sample" / "bars_1d.jsonl"
BRONZE_DAILY_SCHEMA = "symbol_key STRING, raw_value STRING, source_system STRING, ingested_at TIMESTAMP"


def get_daily_bars(s: Settings, source: str, years: int, save_sample: bool) -> tuple[list[Bar], list[dict]]:
    if source == "sample":
        lines = SAMPLE_FILE.read_text().splitlines()
        return [Bar.model_validate_json(line) for line in lines if line], []

    # end = today (exclusive) -> up to YESTERDAY. Today's daily bar is still forming,
    # same "only closed bars" rule as the 1-minute producer.
    end = datetime.now(NSE_TZ).date()
    start = end - timedelta(days=365 * years)
    _, res = yahoo.fetch_daily(load_symbols(s), start, end)
    log.info("yahoo: %d daily bars, %d rejects (%s -> %s)", len(res.bars), len(res.rejects), start, end)
    if not res.bars:
        # Fail LOUDLY: writing an empty Bronze partition would silently wipe that day's data.
        sample_err = res.rejects[0]["error"] if res.rejects else "empty response"
        raise RuntimeError(f"daily extract produced 0 valid bars; first error: {sample_err}")
    if save_sample:
        SAMPLE_FILE.parent.mkdir(parents=True, exist_ok=True)
        SAMPLE_FILE.write_text("\n".join(b.model_dump_json() for b in res.bars) + "\n")
        log.info("saved offline sample -> %s", SAMPLE_FILE)
    return res.bars, res.rejects


def run(spark: SparkSession, s: Settings, source: str = "yahoo", years: int = 2,
        save_sample: bool = False, ingest_date: date | None = None) -> int:
    bars, rejects = get_daily_bars(s, source, years, save_sample)
    now = datetime.now(UTC)
    day = ingest_date or now.date()
    if rejects:
        # Batch equivalent of the Kafka DLQ: keep rejected rows + reason for auditing.
        rej = [(json.dumps(r["raw"], default=str), r["error"], now) for r in rejects]
        (spark.createDataFrame(rej, "raw_value STRING, error STRING, ingested_at TIMESTAMP")
         .withColumn("ingest_date", F.lit(day).cast("date"))
         .coalesce(1).write.mode("overwrite").partitionBy("ingest_date")
         .parquet(s.path("bronze", "_rejects", "bars_1d")))
        log.warning("%d rejected rows -> bronze/_rejects/bars_1d", len(rejects))
    rows = [(b.symbol, json.dumps(b.to_message()), f"{source}_batch", now) for b in bars]
    df = (
        spark.createDataFrame(rows, BRONZE_DAILY_SCHEMA)
        .withColumn("ingest_date", F.lit(day).cast("date"))
    )
    out = s.path("bronze", "bars_1d")
    # mode("overwrite") + partitionOverwriteMode=dynamic (set in get_spark):
    # only the ingest_date partition we are writing is replaced -> safe re-runs.
    df.coalesce(1).write.mode("overwrite").partitionBy("ingest_date").parquet(out)
    log.info("bronze daily: wrote %d rows -> %s", len(rows), out)
    return len(rows)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=["yahoo", "sample"], default="yahoo")
    p.add_argument("--years", type=int, default=2)
    p.add_argument("--save-sample", action="store_true", help="also save fetched bars for offline use")
    a = p.parse_args()
    from pulse.storage import ensure_lake
    from pulse.transform.spark import get_spark

    s = get_settings()
    ensure_lake(s)
    run(get_spark("pulse-extract-daily", s), s, a.source, a.years, a.save_sample)


if __name__ == "__main__":
    main()
