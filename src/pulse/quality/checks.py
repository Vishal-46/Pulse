"""Data quality checks on Silver and Gold, run after every pipeline run.

ERROR   -> the pipeline FAILS (exit code 1): the data is wrong and must not be used.
WARN    -> logged: worth a look, but can legitimately happen (e.g. Yahoo gaps).

A failing check should stop bad data from reaching reports. Loud failure beats silent garbage.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession

from pulse.config import Settings, get_settings
from pulse.logging_setup import setup_logging

log = setup_logging("pulse.quality")


@dataclass
class CheckResult:
    name: str
    severity: str  # "ERROR" or "WARN"
    passed: bool
    detail: str


def _count(df: DataFrame, cond: str) -> int:
    return df.filter(cond).count()


def run_checks(silver_1m: DataFrame, silver_1d: DataFrame, gold_metrics: DataFrame,
               intraday: DataFrame, recon: DataFrame) -> list[CheckResult]:
    r: list[CheckResult] = []

    def add(name, severity, bad, detail=""):
        r.append(CheckResult(name, severity, bad == 0, f"{bad} offending rows {detail}".strip()))

    for label, df in (("silver_1m", silver_1m), ("silver_1d", silver_1d)):
        add(f"{label}: not empty", "ERROR", 1 if df.limit(1).count() == 0 else 0)
        add(f"{label}: event_id unique", "ERROR",
            df.groupBy("event_id").count().filter("count > 1").count())
        add(f"{label}: no null keys/prices", "ERROR",
            _count(df, "symbol IS NULL OR ts IS NULL OR close IS NULL OR volume IS NULL"))
        add(f"{label}: high >= low", "ERROR", _count(df, "high < low"))
        add(f"{label}: one bar per (symbol, ts)", "ERROR",
            df.groupBy("symbol", "ts").count().filter("count > 1").count())

    add("gold daily_metrics: same row count as silver_1d", "ERROR",
        abs(gold_metrics.count() - silver_1d.count()))
    add("gold daily_metrics: volatility >= 0", "ERROR", _count(gold_metrics, "volatility_20d < 0"))
    add("gold daily_metrics: suspicious >20% daily moves (splits?)", "WARN",
        _count(gold_metrics, "is_suspicious_move"))
    add("intraday: session completeness >= 90%", "WARN", _count(intraday, "completeness_pct < 90"))
    add("silver_1m: bars outside NSE session", "WARN", _count(silver_1m, "NOT in_session"))
    add("reconciliation: intraday close within 1% of daily close", "WARN",
        _count(recon, "abs(close_diff_pct) > 1"))
    return r


def run(spark: SparkSession, s: Settings) -> list[CheckResult]:
    rd = spark.read.parquet
    results = run_checks(
        rd(s.path("silver", "bars_1m")), rd(s.path("silver", "bars_1d")),
        rd(s.path("gold", "daily_metrics")), rd(s.path("gold", "intraday_summary")),
        rd(s.path("gold", "reconciliation")),
    )
    for c in results:
        mark = "PASS" if c.passed else ("FAIL" if c.severity == "ERROR" else "WARN")
        log.info("[%s] %-55s %s", mark, c.name, "" if c.passed else c.detail)
    failed = [c for c in results if c.severity == "ERROR" and not c.passed]
    if failed:
        raise RuntimeError(f"{len(failed)} data quality check(s) failed: {[c.name for c in failed]}")
    return results


def main() -> None:
    from pulse.transform.spark import get_spark

    s = get_settings()
    run(get_spark("pulse-quality", s), s)


if __name__ == "__main__":
    main()
