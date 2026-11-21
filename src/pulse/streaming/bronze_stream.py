"""Phase 2 - Spark Structured Streaming: Kafka topic -> Bronze Parquet files.

Bronze rule: store EXACTLY what arrived (raw JSON string) + where/when it came from.
No parsing, no cleaning: if later code has a bug, we can rebuild everything from Bronze.

Run continuously (new data every 30 s):   python -m pulse.streaming.bronze_stream
Process what's there, then stop:          python -m pulse.streaming.bronze_stream --available-now
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from pulse.config import Settings, get_settings
from pulse.logging_setup import setup_logging
from pulse.storage import ensure_lake
from pulse.transform.spark import get_spark

log = setup_logging("pulse.bronze")


def read_kafka(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    """A STREAMING DataFrame: an 'unbounded table' that grows as messages arrive."""
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # only used on the very FIRST run (no checkpoint yet)
        .option("maxOffsetsPerTrigger", 50_000)  # cap batch size so a big backlog can't blow memory
        .load()
    )


def to_bronze(kafka_df: DataFrame) -> DataFrame:
    """Kafka gives binary key/value + metadata columns. Keep the payload raw, add lineage.

    Pure transformation (no I/O), so it can be unit-tested with a normal DataFrame.
    """
    return kafka_df.select(
        F.col("key").cast("string").alias("symbol_key"),
        F.col("value").cast("string").alias("raw_value"),  # the untouched JSON
        F.col("topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),  # (partition, offset) uniquely identifies a message
        F.col("timestamp").alias("kafka_ts"),  # when the broker received it
        F.current_timestamp().alias("ingested_at"),  # when Spark wrote it
    ).withColumn("ingest_date", F.to_date("ingested_at"))


def run(spark: SparkSession, s: Settings, available_now: bool = True) -> int:
    """Start the stream. available_now=True: drain what is in Kafka, then return rows processed."""
    out, ckpt = s.path("bronze", "bars_1m"), s.path("_checkpoints", "bronze_bars_1m")

    bronze = to_bronze(read_kafka(spark, s.kafka_bootstrap, s.topic_bars))
    writer = (
        bronze.writeStream.format("parquet")
        .option("path", out)
        .option("checkpointLocation", ckpt)  # offsets + commit log: the "bookmark"
        .partitionBy("ingest_date")  # one folder per day -> later jobs read only the days they need
        .outputMode("append")  # Bronze only ever adds rows
    )
    writer = writer.trigger(availableNow=True) if available_now \
        else writer.trigger(processingTime="30 seconds")

    query = writer.start()
    log.info("streaming %s -> %s (checkpoint %s)", s.topic_bars, out, ckpt)
    query.awaitTermination()
    rows = sum(p["numInputRows"] for p in query.recentProgress)
    log.info("bronze stream finished: %d micro-batch(es), %d new rows",
             len([p for p in query.recentProgress if p["numInputRows"]]), rows)
    return rows


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--available-now", action="store_true",
                   help="process everything available, then stop (incremental batch mode)")
    args = p.parse_args()
    s = get_settings()
    ensure_lake(s)
    run(get_spark("pulse-bronze-stream", s), s, available_now=args.available_now)


if __name__ == "__main__":
    main()
