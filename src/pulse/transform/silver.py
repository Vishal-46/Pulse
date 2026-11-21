"""Phase 3 - Bronze -> Silver (Spark batch).

Silver = data you can TRUST:
  1. parse      raw JSON string -> typed columns (from_json with an explicit schema)
  2. validate   rows breaking a rule go to a QUARANTINE table with the reason (never silently dropped)
  3. dedupe     one row per event_id, keeping the newest version (row_number over a window)
  4. enrich     trade_date in INDIA time, in_session flag, zero-volume flag

Silver is rebuilt from Bronze on every run. That is simple and always correct at our size
(thousands of rows); at TB scale you would process only new ingest_date partitions.

    python -m pulse.transform.silver
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

from pulse.config import Settings, get_settings
from pulse.logging_setup import setup_logging
from pulse.quality.rules import in_session_col, to_nse_local

log = setup_logging("pulse.silver")

# The event contract (schemas.Bar) written as a Spark DDL schema.
# Timestamps are read as STRING and cast afterwards: casting handles "...Z" and microseconds.
BAR_JSON_SCHEMA = (
    "event_id STRING, schema_version INT, symbol STRING, `interval` STRING, ts STRING, "
    "open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT, "
    "source STRING, produced_at STRING"
)
PRICE_COLS = ["open", "high", "low", "close"]


def parse_bronze(bronze: DataFrame) -> DataFrame:
    """raw_value JSON -> typed columns. Unparseable JSON gives NULL fields (caught by dq_issue)."""
    return (
        bronze.withColumn("j", F.from_json("raw_value", BAR_JSON_SCHEMA))
        .select("j.*", "raw_value", "ingested_at")
        .withColumn("ts", F.col("ts").cast("timestamp"))
        .withColumn("produced_at", F.col("produced_at").cast("timestamp"))
    )


def with_dq_issue(df: DataFrame, interval: str) -> DataFrame:
    """Add dq_issue: NULL if the row is fine, else the FIRST rule it breaks.

    Missing prices are QUARANTINED, not filled: forward-filling a price would invent a trade
    that never happened. For market data, a gap is more honest than a made-up number.
    """
    c = F.col
    any_null = c("event_id").isNull() | c("symbol").isNull() | c("ts").isNull()
    any_price_null = F.greatest(*[c(x).isNull() for x in [*PRICE_COLS, "volume"]])
    return df.withColumn(
        "dq_issue",
        F.when(any_null, "unparseable_or_missing_key")
        .when(c("interval") != interval, "unexpected_interval")
        .when(any_price_null, "missing_value")
        .when(F.least(*[c(x) for x in PRICE_COLS]) <= 0, "non_positive_price")
        .when(c("high") < c("low"), "high_below_low")
        .when(~c("open").between(c("low"), c("high")) | ~c("close").between(c("low"), c("high")),
              "open_close_outside_range")
        .when(c("volume") < 0, "negative_volume"),
    )


def dedupe_latest(df: DataFrame, key: str = "event_id") -> DataFrame:
    """Keep ONE row per key: the newest produced/ingested version.

    Why not df.dropDuplicates([key])? It keeps an ARBITRARY row, so two runs could keep
    different versions. row_number() over an ordered window is deterministic.
    """
    w = Window.partitionBy(key).orderBy(F.col("produced_at").desc(), F.col("ingested_at").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def enrich(df: DataFrame, interval: str) -> DataFrame:
    # trade_date MUST be computed in India time: a daily bar stamped 2026-11-18 00:00 IST
    # is 2026-11-17 18:30 UTC. Taking the UTC date would shift every daily bar one day back.
    df = df.withColumn("trade_date", F.to_date(to_nse_local("ts")))
    if interval == "1m":
        df = df.withColumn("in_session", in_session_col("ts")).withColumn(
            "is_zero_volume", F.col("volume") == 0)  # opening auction minute: real, but flagged
    return df


def build_silver(bronze: DataFrame, interval: str) -> tuple[DataFrame, DataFrame]:
    """Pure transformation: Bronze DataFrame -> (silver, quarantine). No I/O, unit-testable."""
    checked = with_dq_issue(parse_bronze(bronze), interval)
    quarantine = checked.filter(F.col("dq_issue").isNotNull()).select(
        "dq_issue", "raw_value", "ingested_at")
    valid = checked.filter(F.col("dq_issue").isNull()).drop("dq_issue", "raw_value")
    silver = enrich(dedupe_latest(valid), interval).drop("ingested_at")
    return silver, quarantine


def _read_or_none(spark: SparkSession, path: str) -> DataFrame | None:
    try:
        return spark.read.parquet(path)
    except AnalysisException:  # path does not exist yet
        log.warning("no data at %s, skipping", path)
        return None


def run(spark: SparkSession, s: Settings) -> dict:
    stats = {}
    quarantines = []
    for interval, bronze_name in (("1m", "bars_1m"), ("1d", "bars_1d")):
        # Reading a folder written by a streaming file sink: Spark uses its _spark_metadata
        # log, so half-written files from a crashed batch are never read.
        bronze = _read_or_none(spark, s.path("bronze", bronze_name))
        if bronze is None:
            continue
        bronze = bronze.cache()  # read 3x below (count, silver, quarantine): read storage once
        silver, quarantine = build_silver(bronze, interval)
        out = s.path("silver", f"bars_{interval}")
        if interval == "1m":
            # One folder per trading day, and repartition so each day is ONE file.
            silver.repartition("trade_date").write.mode("overwrite").partitionBy("trade_date").parquet(out)
        else:
            # ~5k rows total: partitioning by date would create ~500 folders of 10 rows each
            # (the "small files problem"). One file is the right size here.
            silver.coalesce(1).write.mode("overwrite").parquet(out)

        written = spark.read.parquet(out).count()
        n_bronze, n_quar = bronze.count(), quarantine.count()
        stats[interval] = {"bronze": n_bronze, "quarantined": n_quar,
                           "duplicates_removed": n_bronze - n_quar - written, "silver": written}
        log.info("silver %s: %s", interval, stats[interval])
        quarantines.append(quarantine.withColumn("interval", F.lit(interval)))
        bronze.unpersist()

    if quarantines:
        q = quarantines[0]
        for other in quarantines[1:]:
            q = q.unionByName(other)
        q.coalesce(1).write.mode("overwrite").parquet(s.path("silver", "_quarantine"))
    return stats


def main() -> None:
    from pulse.transform.spark import get_spark

    s = get_settings()
    run(get_spark("pulse-silver", s), s)


if __name__ == "__main__":
    main()
