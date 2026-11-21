"""Spark transformation tests: Bronze -> Silver -> Gold logic on tiny hand-made DataFrames."""

import json
from datetime import UTC, date, datetime, timedelta

import pytest
from pyspark.sql import functions as F

from pulse.quality.rules import in_session_col, is_nse_session
from pulse.streaming.bronze_stream import to_bronze
from pulse.transform import gold, silver

# Always tz-aware: PySpark reads NAIVE datetimes in the laptop's local timezone (IST here)!
UTC_NOW = datetime(2026, 11, 21, 5, 0, tzinfo=UTC)


def bar_json(symbol="TCS.NS", ts="2026-11-18T03:45:00Z", interval="1m", close=100.5,
             high=101.0, low=99.0, volume=10, produced_at="2026-11-18T03:46:00Z", event_id=None):
    return json.dumps({"event_id": event_id or f"{symbol}|{interval}|{ts}", "schema_version": 1,
                       "symbol": symbol, "interval": interval, "ts": ts, "open": 100.0,
                       "high": high, "low": low, "close": close, "volume": volume,
                       "source": "yahoo", "produced_at": produced_at})


def bronze_df(spark, payloads):
    return spark.createDataFrame([(p, UTC_NOW) for p in payloads], "raw_value STRING, ingested_at TIMESTAMP")


# ------------------------------------------------------------------ bronze
def test_to_bronze_keeps_raw_payload_and_lineage(spark):
    kafka = spark.createDataFrame(
        [(b"TCS.NS", b'{"a":1}', "market.bars.1m", 2, 41, UTC_NOW)],
        "key BINARY, value BINARY, topic STRING, partition INT, offset LONG, timestamp TIMESTAMP")
    row = to_bronze(kafka).collect()[0]
    assert row.raw_value == '{"a":1}' and row.symbol_key == "TCS.NS"
    assert (row.kafka_partition, row.kafka_offset) == (2, 41)
    assert row.ingest_date is not None


# ------------------------------------------------------------------ silver
def test_silver_removes_duplicates_keeping_latest_version(spark):
    first = bar_json(close=100.5, produced_at="2026-11-18T03:46:00Z")
    resent = bar_json(close=100.7, produced_at="2026-11-18T03:50:00Z")  # same event_id, newer
    other = bar_json(ts="2026-11-18T03:46:00Z")
    s, q = silver.build_silver(bronze_df(spark, [first, resent, other]), "1m")
    assert s.count() == 2 and q.count() == 0
    kept = s.filter(F.col("ts") == datetime(2026, 11, 18, 3, 45, tzinfo=UTC)).collect()[0]
    assert kept.close == 100.7  # newest version wins, deterministically


@pytest.mark.parametrize("payload, reason", [
    ("{not json", "unparseable_or_missing_key"),
    (bar_json(high=98.0), "high_below_low"),
    (bar_json(close=None), "missing_value"),
    (bar_json(volume=-5), "negative_volume"),
    (bar_json(interval="1d"), "unexpected_interval"),
])
def test_silver_quarantines_bad_rows_with_reason(spark, payload, reason):
    s, q = silver.build_silver(bronze_df(spark, [payload, bar_json()]), "1m")
    assert s.count() == 1
    assert [r.dq_issue for r in q.collect()] == [reason]


def test_trade_date_uses_india_time_not_utc(spark):
    # Daily bar for 21 Nov is stamped 00:00 IST = 20 Nov 18:30 UTC.
    s, _ = silver.build_silver(
        bronze_df(spark, [bar_json(interval="1d", ts="2026-11-20T18:30:00Z")]), "1d")
    assert s.collect()[0].trade_date == date(2026, 11, 21)


def test_in_session_spark_matches_python_rule(spark):
    times = [datetime(2026, 11, 18, 3, 44, tzinfo=UTC), datetime(2026, 11, 18, 3, 45, tzinfo=UTC),
             datetime(2026, 11, 18, 9, 59, tzinfo=UTC), datetime(2026, 11, 18, 10, 0, tzinfo=UTC),
             datetime(2026, 11, 21, 5, 0, tzinfo=UTC)]  # pre-open, open, last bar, close, Saturday
    df = spark.createDataFrame([(t,) for t in times], "ts TIMESTAMP").withColumn("s", in_session_col("ts"))
    expected = [is_nse_session(t) for t in times]
    assert [r.s for r in df.orderBy("ts").collect()] == expected == [False, True, True, False, False]


# ------------------------------------------------------------------ gold
def _daily(spark, closes, symbol="TCS.NS"):
    start = date(2026, 1, 1)
    rows = [(symbol, start + timedelta(days=i), c, c, c, c, 1000 + i) for i, c in enumerate(closes)]
    return spark.createDataFrame(
        rows, "symbol STRING, trade_date DATE, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume LONG")


def test_daily_return_and_drawdown(spark):
    m = gold.daily_metrics(_daily(spark, [100.0, 110.0, 99.0])).orderBy("trade_date").collect()
    assert m[0].daily_return is None  # no previous day
    assert m[1].daily_return == pytest.approx(0.10)
    assert m[2].daily_return == pytest.approx(-0.10)
    assert m[2].drawdown == pytest.approx(99 / 110 - 1)  # 10% below the running peak


def test_moving_average_has_warm_up_period(spark):
    m = gold.daily_metrics(_daily(spark, [float(i) for i in range(1, 26)])).orderBy("trade_date").collect()
    assert m[18].ma_20 is None  # day 19: window not full yet
    assert m[19].ma_20 == pytest.approx(sum(range(1, 21)) / 20)  # day 20: avg of 1..20
    assert m[24].ma_50 is None


def test_windows_do_not_mix_symbols(spark):
    df = _daily(spark, [100.0, 200.0], "A").unionByName(_daily(spark, [10.0, 11.0], "B"))
    rets = {(r.symbol, r.trade_date.day): r.daily_return for r in gold.daily_metrics(df).collect()}
    assert rets[("B", 2)] == pytest.approx(0.10)  # B compared with B, never with A


def test_intraday_summary_ohlc_and_vwap(spark):
    rows = [("TCS.NS", date(2026, 11, 18), datetime(2026, 11, 18, 3, 45), 10.0, 12.0, 9.0, 11.0, 100, True, False),
            ("TCS.NS", date(2026, 11, 18), datetime(2026, 11, 18, 3, 46), 11.0, 13.0, 10.0, 12.0, 300, True, False),
            ("TCS.NS", date(2026, 11, 18), datetime(2026, 11, 18, 2, 0), 1.0, 1.0, 1.0, 1.0, 999, False, False)]
    m1 = spark.createDataFrame(rows, "symbol STRING, trade_date DATE, ts TIMESTAMP, open DOUBLE, high DOUBLE, "
                                     "low DOUBLE, close DOUBLE, volume LONG, in_session BOOLEAN, is_zero_volume BOOLEAN")
    r = gold.intraday_summary(m1).collect()[0]
    assert (r.open, r.high, r.low, r.close, r.volume, r.bar_count) == (10.0, 13.0, 9.0, 12.0, 400, 2)
    vwap = ((12 + 9 + 11) / 3 * 100 + (13 + 10 + 12) / 3 * 300) / 400
    assert r.vwap == pytest.approx(vwap)  # out-of-session bar ignored
