"""Unit tests for the event contract and the Yahoo frame parser. No network, no Docker."""

from datetime import UTC, datetime, timedelta, timezone

import pandas as pd
import pytest
from pydantic import ValidationError

from pulse.schemas import Bar
from pulse.sources import yahoo

IST = timezone(timedelta(hours=5, minutes=30))


def make_bar(**over) -> Bar:
    base = {"symbol": "TCS.NS", "interval": "1m", "ts": datetime(2026, 11, 18, 10, 0, tzinfo=IST),
            "open": 100, "high": 105, "low": 99, "close": 104, "volume": 1000}
    return Bar(**{**base, **over})


def test_timestamps_are_normalised_to_utc():
    assert make_bar().ts == datetime(2026, 11, 18, 4, 30, tzinfo=UTC)


def test_naive_timestamp_rejected():
    with pytest.raises(ValidationError, match="timezone-aware"):
        make_bar(ts=datetime(2026, 11, 18, 10, 0))


@pytest.mark.parametrize("over", [
    {"high": 98},          # high < low
    {"open": 110},         # open above high
    {"close": 0},          # non-positive price
    {"volume": -1},        # negative volume
])
def test_invalid_bars_rejected(over):
    with pytest.raises(ValidationError):
        make_bar(**over)


def test_zero_volume_is_valid():
    # Opening-auction minute on NSE reports volume 0: unusual but REAL, must not be rejected.
    assert make_bar(volume=0).volume == 0


def test_event_id_is_deterministic_and_ignores_processing_time():
    a = make_bar()
    b = make_bar(produced_at=datetime.now(UTC) + timedelta(hours=3), source="replay")
    assert a.event_id == b.event_id                       # same bar -> same idempotency key
    assert a.event_id != make_bar(symbol="INFY.NS").event_id


def _yahoo_frame():
    idx = pd.DatetimeIndex(["2026-11-18 09:15", "2026-11-18 09:16"], tz="Asia/Kolkata")
    cols = pd.MultiIndex.from_product([["TCS.NS"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]])
    return pd.DataFrame([[100, 101, 99, 100.5, 100.5, 0],
                         [100.5, 100, 101, 100.7, 100.7, 500]],   # 2nd row: high < low -> reject
                        index=idx, columns=cols)


def test_frame_to_bars_splits_valid_and_rejects():
    res = yahoo.frame_to_bars(_yahoo_frame(), ["TCS.NS", "MISSING.NS"], "1m")
    assert len(res.bars) == 1 and len(res.rejects) == 1
    assert res.rejects[0]["raw"]["symbol"] == "TCS.NS" and "high" in res.rejects[0]["error"]


def test_fetch_intraday_drops_the_still_open_bar(monkeypatch):
    monkeypatch.setattr(yahoo, "_download", lambda *a, **k: _yahoo_frame())
    # 09:15:30 IST: the 09:15 bar is still forming, so nothing may be published yet.
    now = datetime(2026, 11, 18, 9, 15, 30, tzinfo=IST)
    assert yahoo.fetch_intraday(["TCS.NS"], now=now).bars == []
    later = datetime(2026, 11, 18, 9, 16, 0, tzinfo=IST)
    assert len(yahoo.fetch_intraday(["TCS.NS"], now=later).bars) == 1


def test_naive_daily_dates_get_exchange_timezone():
    # yf.download() returns naive daily dates: the adapter must attach India time,
    # otherwise the Bar contract (rightly) rejects every row.
    idx = pd.DatetimeIndex(["2026-11-20"])
    cols = pd.MultiIndex.from_product([["TCS.NS"], ["Open", "High", "Low", "Close", "Adj Close", "Volume"]])
    df = pd.DataFrame([[100, 101, 99, 100.5, 100.5, 10]], index=idx, columns=cols)
    res = yahoo.frame_to_bars(df, ["TCS.NS"], "1d")
    assert res.rejects == []
    assert res.bars[0].ts == datetime(2026, 11, 19, 18, 30, tzinfo=UTC)
