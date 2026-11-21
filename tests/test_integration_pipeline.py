"""Integration test: runs against the REAL lake produced by `python -m pulse.pipeline`.

    docker compose up -d && python -m pulse.pipeline && pytest -m integration
"""

import pytest

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def lake():
    from pulse.config import get_settings
    from pulse.transform.spark import get_spark

    s = get_settings()
    return get_spark("pulse-integration-test", s), s


def test_every_gold_table_exists_and_has_rows(lake):
    spark, s = lake
    from pulse.analytics.report import GOLD_TABLES
    for t in GOLD_TABLES:
        assert spark.read.parquet(s.path("gold", t)).count() > 0, t


def test_silver_has_no_duplicates_even_if_bronze_does(lake):
    spark, s = lake
    bronze = spark.read.parquet(s.path("bronze", "bars_1m"))
    silver = spark.read.parquet(s.path("silver", "bars_1m"))
    assert silver.count() == silver.select("event_id").distinct().count()
    assert bronze.count() >= silver.count()


def test_quality_checks_pass(lake):
    spark, s = lake
    from pulse.quality import checks
    assert all(c.passed for c in checks.run(spark, s) if c.severity == "ERROR")
