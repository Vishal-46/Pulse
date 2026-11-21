"""Analytics on the Gold layer with Spark SQL.

Each Gold folder is registered as a temporary VIEW, so analysts can use plain SQL
(window functions, CTEs, joins) on the Parquet files: no database server involved.

    python -m pulse.analytics.report
"""

from pyspark.sql import SparkSession

from pulse.config import Settings, get_settings
from pulse.logging_setup import setup_logging

log = setup_logging("pulse.report")

GOLD_TABLES = ["daily_metrics", "intraday_summary", "reconciliation",
               "symbol_performance", "sector_performance"]

QUERIES = {
    "1. Symbol leaderboard (1-year return, risk, trend)": """
        SELECT symbol, sector, as_of_date, round(close, 2) AS close,
               round(ret_252d * 100, 1)          AS return_1y_pct,
               round(ytd_return * 100, 1)        AS ytd_pct,
               round(annual_volatility * 100, 1) AS volatility_pct,
               round(max_drawdown * 100, 1)      AS max_drawdown_pct,
               trend
        FROM symbol_performance
        ORDER BY ret_252d DESC
    """,
    "2. Sector performance": "SELECT * FROM sector_performance",
    "3. Moving-average crossovers in the last 90 days (20d crosses 50d)": """
        WITH t AS (
          SELECT symbol, trade_date, close, ma_20, ma_50, trend,
                 LAG(trend) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_trend
          FROM daily_metrics
        )
        SELECT symbol, trade_date, round(close, 2) AS close,
               CASE WHEN trend = 'up' THEN 'golden cross (bullish)' ELSE 'death cross (bearish)' END AS signal
        FROM t
        WHERE prev_trend IS NOT NULL AND trend <> prev_trend
          AND trade_date >= date_sub((SELECT max(trade_date) FROM daily_metrics), 90)
        ORDER BY trade_date DESC, symbol
    """,
    "4. Volume spikes in the last 30 days (volume >= 2x its 20-day average)": """
        SELECT symbol, trade_date, volume, round(volume_ratio, 2) AS volume_ratio,
               round(daily_return * 100, 2) AS return_pct
        FROM daily_metrics
        WHERE is_volume_spike
          AND trade_date >= date_sub((SELECT max(trade_date) FROM daily_metrics), 30)
        ORDER BY volume_ratio DESC
        LIMIT 10
    """,
    "5. Intraday session quality and VWAP (from 1-minute bars)": """
        SELECT symbol, trade_date, bar_count, completeness_pct, zero_volume_bars,
               round(open, 2) AS open, round(close, 2) AS close, round(vwap, 2) AS vwap
        FROM intraday_summary ORDER BY symbol
    """,
    "6. Reconciliation: 1-minute pipeline vs daily bar": """
        SELECT symbol, trade_date, bar_count, close_diff_pct, volume_coverage_pct
        FROM reconciliation ORDER BY abs(close_diff_pct) DESC
    """,
}


def register_views(spark: SparkSession, s: Settings) -> None:
    for t in GOLD_TABLES:
        spark.read.parquet(s.path("gold", t)).createOrReplaceTempView(t)


def run(spark: SparkSession, s: Settings) -> None:
    register_views(spark, s)
    for title, sql in QUERIES.items():
        print(f"\n=== {title} ===")
        spark.sql(sql).show(20, truncate=False)


def main() -> None:
    from pulse.transform.spark import get_spark

    s = get_settings()
    run(get_spark("pulse-report", s), s)


if __name__ == "__main__":
    main()
