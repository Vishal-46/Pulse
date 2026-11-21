"""Shared pytest fixtures.

One local SparkSession for the whole test run (starting a JVM takes seconds, so we do it once).
No Kafka/S3 jars: unit tests only exercise pure DataFrame -> DataFrame transformations.
"""

import os
import sys

import pytest

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[2]")
        .appName("pulse-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()
