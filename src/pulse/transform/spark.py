"""One place that builds the SparkSession for every Pulse job.

Spark itself only knows local files. To read Kafka and S3 storage it needs extra
"connector" jars, which Spark downloads once (into ~/.ivy2) via spark.jars.packages.
"""

import os
import sys

from pyspark.sql import SparkSession

from pulse.config import Settings, get_settings

# Spark starts separate Python WORKER processes for Python code (e.g. createDataFrame from
# Python objects). By default they run `python3`, which on this machine is the system 3.14,
# while the driver runs the venv's 3.12, and PySpark refuses mixed versions.
# Pin workers to the exact interpreter running the driver.
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

SPARK_VERSION = "4.3.0"
HADOOP_VERSION = "3.5.0"  # must match the hadoop-client jars bundled inside pyspark
PACKAGES = [
    f"org.apache.spark:spark-sql-kafka-0-10_2.13:{SPARK_VERSION}",  # Kafka source/sink
    f"org.apache.hadoop:hadoop-aws:{HADOOP_VERSION}",  # s3a:// filesystem
]


def get_spark(app_name: str, s: Settings | None = None) -> SparkSession:
    s = s or get_settings()
    b = (
        SparkSession.builder.appName(app_name)
        .master(s.spark_master)  # local[*] = driver + executors in ONE JVM, all CPU cores
        .config("spark.driver.memory", s.spark_driver_memory)
        .config("spark.jars.packages", ",".join(PACKAGES))
        # Shuffle partitions: default 200 means 200 tiny tasks for our MBs of data.
        .config("spark.sql.shuffle.partitions", s.spark_shuffle_partitions)
        # Adaptive Query Execution: re-plans at runtime using real sizes (merges tiny partitions,
        # switches to broadcast joins, splits skewed partitions).
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")  # all timestamps in UTC, always
        # Overwrite only the partitions we write, not the whole table -> re-runs are safe.
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if s.lake_backend == "s3":
        b = (
            b.config("spark.hadoop.fs.s3a.endpoint", s.s3_endpoint)
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")  # required by SDK, ignored by RustFS
            .config("spark.hadoop.fs.s3a.access.key", s.s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s.s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")  # http://host/bucket/key
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")  # local only
        )
    spark = b.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
