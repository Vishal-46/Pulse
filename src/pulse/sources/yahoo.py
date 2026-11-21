"""Yahoo Finance source (via yfinance).

yfinance is an UNOFFICIAL API: it gets rate limited and breaks when Yahoo changes things
(see v1's producer.log: 147 x "possibly delisted"). So this module:
  * fetches all symbols in ONE request (fewer calls -> fewer rate-limit hits),
  * retries transient failures with exponential backoff,
  * turns Yahoo's wide pandas frame into validated Bar objects (raw rows kept for Bronze),
  * never lets one bad row kill the whole batch: bad rows go to a rejects list.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta

import pandas as pd
import yfinance as yf
from pydantic import ValidationError
from tenacity import before_sleep_log, retry, stop_after_attempt, wait_exponential

from pulse.schemas import Bar

log = logging.getLogger(__name__)

INTERVAL_DELTA = {"1m": timedelta(minutes=1), "1d": timedelta(days=1)}


class SourceEmptyError(RuntimeError):
    """Raised when Yahoo returns nothing, usually throttling, so it is worth a retry."""


@dataclass
class FetchResult:
    bars: list[Bar] = field(default_factory=list)
    rejects: list[dict] = field(default_factory=list)  # {"raw": ..., "error": ...}


@retry(
    stop=stop_after_attempt(4),
    wait=wait_exponential(multiplier=2, min=2, max=30),  # 2s, 4s, 8s ... capped at 30s
    before_sleep=before_sleep_log(log, logging.WARNING),
    reraise=True,
)
def _download(symbols: list[str], interval: str, **kwargs) -> pd.DataFrame:
    df = yf.download(
        symbols,
        interval=interval,
        group_by="ticker",
        auto_adjust=False,  # keep raw prices; adjustments are an analytics decision
        progress=False,
        threads=True,
        **kwargs,
    )
    if df is None or df.empty:
        raise SourceEmptyError(f"empty response for {symbols} interval={interval}")
    return df


def frame_to_bars(
    df: pd.DataFrame, symbols: list[str], interval: str, source: str = "yahoo",
    market_tz: str = "Asia/Kolkata",
) -> FetchResult:
    """Convert yfinance's (symbol, field) column frame into validated Bars. Pure function: easy to test."""
    result = FetchResult()
    if df.index.tz is None:
        # yf.download() returns NAIVE dates for daily bars ("2026-11-02 00:00", but where?).
        # A daily NSE bar's date is an India date, so we attach the exchange timezone HERE,
        # in the adapter, instead of weakening the Bar contract to accept naive times.
        df = df.copy()
        df.index = df.index.tz_localize(market_tz)
    for sym in symbols:
        if sym not in df.columns.get_level_values(0):
            log.warning("no data returned for %s", sym)
            continue
        sub = df[sym].dropna(how="all")
        for ts, row in sub.iterrows():
            raw = {"symbol": sym, "ts": ts.isoformat(), **{k: _py(v) for k, v in row.items()}}
            try:
                bar = Bar(
                    symbol=sym,
                    interval=interval,
                    ts=ts.to_pydatetime(),
                    open=row["Open"],
                    high=row["High"],
                    low=row["Low"],
                    close=row["Close"],
                    volume=int(row["Volume"]) if pd.notna(row["Volume"]) else -1,
                    source=source,
                )
                result.bars.append(bar)
            except (ValidationError, ValueError, TypeError) as exc:
                result.rejects.append({"raw": raw, "error": str(exc)[:500]})
    return result


def _py(v):
    """numpy/pandas scalars -> plain Python so they serialise to JSON."""
    if pd.isna(v):
        return None
    return v.item() if hasattr(v, "item") else v


def fetch_intraday(symbols: list[str], now: datetime | None = None) -> FetchResult:
    """Today's 1-minute bars, keeping only CLOSED bars.

    The bar for the current minute keeps changing until the minute ends. Sending it would
    publish a value that is later 'corrected', so we keep only bars with ts + 1m <= now.
    """
    now = now or datetime.now(UTC)
    df = _download(symbols, "1m", period="1d")
    res = frame_to_bars(df, symbols, "1m")
    res.bars = [b for b in res.bars if b.ts + INTERVAL_DELTA["1m"] <= now]
    return res


def fetch_daily(symbols: list[str], start: date, end: date) -> tuple[pd.DataFrame, FetchResult]:
    """Daily bars for [start, end). Returns the raw frame too, because Bronze keeps raw data."""
    df = _download(symbols, "1d", start=start.isoformat(), end=end.isoformat())
    return df, frame_to_bars(df, symbols, "1d")
