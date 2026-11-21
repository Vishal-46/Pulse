"""Phase 4 - Silver -> Gold (Spark batch): analytics-ready tables.

  gold/daily_metrics        per symbol per day: returns, moving averages, volatility, volume ratio, drawdown
  gold/intraday_summary     per symbol per session from 1-minute bars: OHLC, VWAP, completeness
  gold/reconciliation       intraday-derived daily numbers vs. the official daily bar
  gold/symbol_performance   one row per symbol: latest state + period returns + risk, with sector
  gold/sector_performance   one row per sector

Spark techniques used: window functions (lag, rolling avg/stddev, running max, row_number),
groupBy aggregations (shuffle), min_by/max_by, broadcast join, caching, explain().

    python -m pulse.transform.gold
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from pulse.config import Settings, get_settings
from pulse.logging_setup import setup_logging
from pulse.quality.rules import SESSION_MINUTES

log = setup_logging("pulse.gold")
TRADING_DAYS_PER_YEAR = 252


def daily_metrics(d1: DataFrame) -> DataFrame:
    """Window functions over each symbol's own time series.

    Window.partitionBy("symbol") = "do this separately per symbol" (Spark shuffles so each
    symbol's rows sit together), orderBy("trade_date") = "in date order".
    """
    w = Window.partitionBy("symbol").orderBy("trade_date")

    def last_n(n: int) -> Window:  # current row + previous n-1 rows
        return w.rowsBetween(-(n - 1), 0)

    prev_20 = w.rowsBetween(-20, -1)  # the 20 days BEFORE today (excludes today)
    so_far = w.rowsBetween(Window.unboundedPreceding, 0)  # first row up to today
    year_so_far = (Window.partitionBy("symbol", F.year("trade_date")).orderBy("trade_date")
                   .rowsBetween(Window.unboundedPreceding, 0))
    c = F.col

    df = (
        d1.select("symbol", "trade_date", "open", "high", "low", "close", "volume")
        .withColumn("n", F.row_number().over(w))  # how many days of history so far
        .withColumn("prev_close", F.lag("close").over(w))
        .withColumn("daily_return", c("close") / c("prev_close") - 1)
        .withColumn("log_return", F.log(c("close") / c("prev_close")))
        # Moving averages are only meaningful once the window is full ("warm-up" period).
        .withColumn("ma_20", F.when(c("n") >= 20, F.avg("close").over(last_n(20))))
        .withColumn("ma_50", F.when(c("n") >= 50, F.avg("close").over(last_n(50))))
        # Annualised volatility = stddev of daily log returns * sqrt(252 trading days).
        .withColumn("volatility_20d",
                    F.when(c("n") >= 21, F.stddev("log_return").over(last_n(20))
                           * F.sqrt(F.lit(TRADING_DAYS_PER_YEAR))))
        .withColumn("avg_volume_20d", F.when(c("n") > 20, F.avg("volume").over(prev_20)))
        .withColumn("volume_ratio", c("volume") / c("avg_volume_20d"))
        .withColumn("is_volume_spike", F.coalesce(c("volume_ratio") >= 2, F.lit(False)))
        .withColumn("ret_21d", c("close") / F.lag("close", 21).over(w) - 1)  # ~1 month
        .withColumn("ret_63d", c("close") / F.lag("close", 63).over(w) - 1)  # ~3 months
        .withColumn("ret_252d", c("close") / F.lag("close", 252).over(w) - 1)  # ~1 year
        .withColumn("ytd_return", c("close") / F.first("close").over(year_so_far) - 1)
        .withColumn("running_max_close", F.max("close").over(so_far))
        .withColumn("drawdown", c("close") / c("running_max_close") - 1)  # % below the peak so far
        .withColumn("trend", F.when(c("ma_50").isNull(), None)
                    .when(c("ma_20") > c("ma_50"), "up").otherwise("down"))
        # Raw (unadjusted) prices: a stock split shows up as a fake -50% day. Flag big moves.
        .withColumn("is_suspicious_move", F.coalesce(F.abs(c("daily_return")) > 0.2, F.lit(False)))
        .drop("n")
    )
    return df


def intraday_summary(m1: DataFrame) -> DataFrame:
    """groupBy aggregation: ~375 one-minute rows -> 1 row per symbol per session (a shuffle)."""
    typical = (F.col("high") + F.col("low") + F.col("close")) / 3
    return (
        m1.filter("in_session")
        .groupBy("symbol", "trade_date")
        .agg(
            F.min_by("open", "ts").alias("open"),  # open of the EARLIEST bar
            F.max("high").alias("high"),
            F.min("low").alias("low"),
            F.max_by("close", "ts").alias("close"),  # close of the LATEST bar
            F.sum("volume").alias("volume"),
            # VWAP: volume-weighted average price, the "fair" average price of the day.
            (F.sum(typical * F.col("volume")) / F.sum("volume")).alias("vwap"),
            F.count("*").alias("bar_count"),
            F.sum(F.col("is_zero_volume").cast("int")).alias("zero_volume_bars"),
            F.min("ts").alias("first_bar_ts"),
            F.max("ts").alias("last_bar_ts"),
        )
        .withColumn("completeness_pct", F.round(F.col("bar_count") / SESSION_MINUTES * 100, 1))
    )


def reconciliation(intraday: DataFrame, d1: DataFrame) -> DataFrame:
    """Do our two pipelines agree? Compare 1-minute-derived numbers with the daily bar."""
    d = d1.select("symbol", "trade_date", F.col("close").alias("daily_close"),
                  F.col("volume").alias("daily_volume"))
    return (
        intraday.select("symbol", "trade_date", "close", "volume", "bar_count")
        .join(d, ["symbol", "trade_date"], "inner")
        .withColumn("close_diff_pct", F.round((F.col("close") / F.col("daily_close") - 1) * 100, 3))
        .withColumn("volume_coverage_pct", F.round(F.col("volume") / F.col("daily_volume") * 100, 1))
    )


def symbol_performance(metrics: DataFrame, dim: DataFrame) -> DataFrame:
    latest_w = Window.partitionBy("symbol").orderBy(F.col("trade_date").desc())
    latest = (
        metrics.withColumn("rn", F.row_number().over(latest_w)).filter("rn = 1")
        .select("symbol", F.col("trade_date").alias("as_of_date"), "close", "ma_20", "ma_50", "trend",
                "ret_21d", "ret_63d", "ret_252d", "ytd_return", "volatility_20d")
    )
    risk = metrics.groupBy("symbol").agg(
        F.min("drawdown").alias("max_drawdown"),
        (F.stddev("log_return") * F.sqrt(F.lit(TRADING_DAYS_PER_YEAR))).alias("annual_volatility"),
        F.avg("volume").cast("long").alias("avg_daily_volume"),
        F.count("*").alias("trading_days"),
        F.sum(F.col("is_volume_spike").cast("int")).alias("volume_spike_days"),
    )
    # BROADCAST JOIN: dim has 10 rows. Instead of shuffling BOTH sides by symbol, Spark copies
    # the small table to every executor and joins locally: no shuffle of the big side.
    return latest.join(risk, "symbol").join(F.broadcast(dim), "symbol", "left")


def sector_performance(perf: DataFrame) -> DataFrame:
    return (
        perf.groupBy("sector")
        .agg(
            F.count("*").alias("symbols"),
            F.round(F.avg("ret_252d") * 100, 2).alias("avg_return_1y_pct"),
            F.round(F.avg("ytd_return") * 100, 2).alias("avg_ytd_pct"),
            F.round(F.avg("annual_volatility") * 100, 2).alias("avg_volatility_pct"),
            F.max_by("symbol", "ret_252d").alias("best_symbol_1y"),
        )
        .orderBy(F.col("avg_return_1y_pct").desc())
    )


def load_dim_symbols(spark: SparkSession, s: Settings) -> DataFrame:
    return spark.read.csv(f"file://{s.symbols_file}", header=True)


def run(spark: SparkSession, s: Settings, show_plan: bool = False) -> dict:
    d1 = spark.read.parquet(s.path("silver", "bars_1d"))
    m1 = spark.read.parquet(s.path("silver", "bars_1m"))
    dim = load_dim_symbols(spark, s)

    # CACHE: metrics is used by 2 outputs below. Without cache(), Spark would recompute the
    # whole window pipeline (read + shuffle + windows) for EACH output, because of lazy evaluation.
    metrics = daily_metrics(d1).cache()
    intraday = intraday_summary(m1).cache()
    perf = symbol_performance(metrics, dim).cache()

    outputs = {
        "daily_metrics": metrics,
        "intraday_summary": intraday,
        "reconciliation": reconciliation(intraday, d1),
        "symbol_performance": perf,
        "sector_performance": sector_performance(perf),
    }
    if show_plan:
        perf.explain(mode="formatted")  # look for BroadcastHashJoin and Exchange (= shuffle)

    stats = {}
    for name, df in outputs.items():
        # Gold tables are small: coalesce(1) -> one file per table instead of 12 tiny ones.
        df.coalesce(1).write.mode("overwrite").parquet(s.path("gold", name))
        stats[name] = spark.read.parquet(s.path("gold", name)).count()
    log.info("gold rows: %s", stats)
    for df in (metrics, intraday, perf):
        df.unpersist()
    return stats


def main() -> None:
    import argparse

    from pulse.transform.spark import get_spark

    p = argparse.ArgumentParser()
    p.add_argument("--explain", action="store_true", help="print the physical plan of symbol_performance")
    s = get_settings()
    run(get_spark("pulse-gold", s), s, show_plan=p.parse_args().explain)


if __name__ == "__main__":
    main()
