"""The event contract for a market bar.

Producers and consumers never share code at runtime; they share THIS schema.
If the producer changes a field silently, every downstream job breaks, so the contract
is explicit, validated at the edge, and versioned (schema_version).
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

SCHEMA_VERSION = 1


class Bar(BaseModel):
    """One OHLCV bar (open/high/low/close/volume) for one symbol over one interval."""

    schema_version: int = SCHEMA_VERSION
    symbol: str = Field(min_length=1, max_length=32)
    interval: Literal["1m", "1d"]
    ts: datetime  # EVENT time: when the bar started on the exchange, always UTC
    open: float = Field(gt=0)
    high: float = Field(gt=0)
    low: float = Field(gt=0)
    close: float = Field(gt=0)
    volume: int = Field(ge=0)
    source: Literal["yahoo", "replay"] = "yahoo"
    produced_at: datetime = Field(default_factory=lambda: datetime.now(UTC))  # PROCESSING time

    @field_validator("ts", "produced_at")
    @classmethod
    def _must_be_utc(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must be timezone-aware")  # naive times cause silent bugs
        return v.astimezone(UTC)

    @model_validator(mode="after")
    def _ohlc_consistent(self) -> Bar:
        if self.high < self.low:
            raise ValueError(f"high {self.high} < low {self.low}")
        if not (self.low <= self.open <= self.high and self.low <= self.close <= self.high):
            raise ValueError("open/close outside [low, high]")
        return self

    @property
    def event_id(self) -> str:
        """Deterministic id: same bar -> same id, no matter how many times it is sent.

        This is the idempotency key Silver uses to deduplicate.
        """
        raw = f"{self.symbol}|{self.interval}|{self.ts.isoformat()}"
        return hashlib.sha1(raw.encode()).hexdigest()

    def to_message(self) -> dict:
        return {"event_id": self.event_id, **self.model_dump(mode="json")}
