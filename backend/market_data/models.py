from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from math import isfinite

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class VolumeSource(str, Enum):
    TICK = "TICK"
    REAL = "REAL"
    UNKNOWN = "UNKNOWN"


class CandleQuality(str, Enum):
    VERIFIED = "VERIFIED"
    INCOMPLETE = "INCOMPLETE"
    INVALID = "INVALID"


class MarketCandle(BaseModel):
    """Normalized OHLC candle with explicit timestamp and volume provenance.

    timestamp represents the candle opening time in UTC.
    Tick volume and real volume are stored separately and never conflated.
    """

    model_config = ConfigDict(frozen=True)

    symbol: str = Field(min_length=1)
    timeframe: str = Field(min_length=2)

    timestamp: datetime

    open: float
    high: float
    low: float
    close: float

    tick_volume: int = Field(ge=0)
    real_volume: int | None = Field(default=None, ge=0)
    volume_source: VolumeSource = VolumeSource.UNKNOWN

    is_closed: bool
    quality: CandleQuality = CandleQuality.VERIFIED

    @field_validator("symbol", "timeframe")
    @classmethod
    def normalize_required_text(cls, value: str) -> str:
        normalized = value.strip()

        if not normalized:
            raise ValueError("Value cannot be empty.")

        return normalized

    @field_validator("timestamp")
    @classmethod
    def require_utc_timestamp(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Candle timestamp must be timezone-aware.")

        if value.utcoffset().total_seconds() != 0:
            raise ValueError("Candle timestamp must already be UTC.")

        return value.astimezone(timezone.utc)

    @field_validator("open", "high", "low", "close")
    @classmethod
    def validate_finite_prices(cls, value: float) -> float:
        if not isfinite(value):
            raise ValueError("Candle prices must be finite numbers.")

        if value <= 0:
            raise ValueError("Candle prices must be greater than zero.")

        return value

    @model_validator(mode="after")
    def validate_ohlc_relationships(self) -> MarketCandle:
        if self.high < self.low:
            raise ValueError("Candle high cannot be lower than candle low.")

        if self.high < max(self.open, self.close):
            raise ValueError("Candle high must cover open and close.")

        if self.low > min(self.open, self.close):
            raise ValueError("Candle low must cover open and close.")

        if self.volume_source == VolumeSource.REAL:
            if self.real_volume is None:
                raise ValueError(
                    "REAL volume source requires a real_volume value."
                )

        if self.volume_source == VolumeSource.TICK:
            if self.tick_volume < 0:
                raise ValueError("Tick volume cannot be negative.")

        if self.quality == CandleQuality.VERIFIED and not self.is_closed:
            raise ValueError(
                "An unclosed candle cannot be marked VERIFIED."
            )

        return self


class CandleBatch(BaseModel):
    """Chronologically ordered candles for one symbol and timeframe."""

    model_config = ConfigDict(frozen=True)

    symbol: str = Field(min_length=1)
    timeframe: str = Field(min_length=2)
    candles: tuple[MarketCandle, ...] = Field(min_length=1)

    received_at: datetime
    source: str = Field(min_length=1)

    @field_validator("received_at")
    @classmethod
    def require_utc_received_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware.")

        if value.utcoffset().total_seconds() != 0:
            raise ValueError("received_at must already be UTC.")

        return value.astimezone(timezone.utc)

    @model_validator(mode="after")
    def validate_batch(self) -> CandleBatch:
        timestamps = []

        for candle in self.candles:
            if candle.symbol != self.symbol:
                raise ValueError(
                    "All candles must belong to the batch symbol."
                )

            if candle.timeframe != self.timeframe:
                raise ValueError(
                    "All candles must use the batch timeframe."
                )

            timestamps.append(candle.timestamp)

        if timestamps != sorted(timestamps):
            raise ValueError(
                "Candles must be ordered from oldest to newest."
            )

        if len(timestamps) != len(set(timestamps)):
            raise ValueError(
                "Duplicate candle timestamps are not allowed."
            )

        return self
