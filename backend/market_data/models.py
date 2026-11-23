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
    """Normalized OHLC candle with explicit timestamp and volume provenance."""

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

    @field_validator("tick_volume", "real_volume", mode="before")
    @classmethod
    def reject_boolean_volume(cls, value):
        if isinstance(value, bool):
            raise ValueError("Volume values must not be boolean.")

        return value

    @field_validator("is_closed", mode="before")
    @classmethod
    def require_strict_boolean(cls, value):
        if not isinstance(value, bool):
            raise ValueError("is_closed must be a boolean.")

        return value

    @field_validator("open", "high", "low", "close")
    @classmethod
    def validate_finite_prices(cls, value: float) -> float:
        if not isfinite(value):
            raise ValueError("Candle prices must be finite numbers.")

        if value <= 0:
            raise ValueError("Candle prices must be greater than zero.")

        return value

    @model_validator(mode="after")
    def validate_candle(self) -> MarketCandle:
        if self.high < self.low:
            raise ValueError("Candle high cannot be lower than candle low.")

        if self.high < max(self.open, self.close):
            raise ValueError("Candle high must cover open and close.")

        if self.low > min(self.open, self.close):
            raise ValueError("Candle low must cover open and close.")

        if self.volume_source == VolumeSource.REAL:
            if self.real_volume is None:
                raise ValueError("REAL volume source requires real_volume.")

        elif self.volume_source == VolumeSource.TICK:
            if self.real_volume is not None:
                raise ValueError(
                    "TICK volume source must not contain real_volume."
                )

        if self.quality == CandleQuality.VERIFIED and not self.is_closed:
            raise ValueError("An unclosed candle cannot be marked VERIFIED.")

        return self


class MarketDataClockEvidence(BaseModel):
    """Current live clock-health evidence bound to one MT5 candle batch.

    ``candle_offset_seconds`` describes the live M1/M5 calibration used to
    verify the current terminal clock. It is not the historical offset used
    to convert each candle; historical conversion is resolved per raw MT5
    timestamp through BrokerOffsetRegistry before a MarketCandle is built.
    """

    model_config = ConfigDict(frozen=True)

    verified: bool

    account_login: int = Field(gt=0)
    broker_server: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    terminal_build: int = Field(gt=0)

    checked_at: datetime
    ntp_offset_seconds: float
    ntp_uncertainty_seconds: float = Field(ge=0)
    ntp_sample_count: int = Field(ge=3)
    ntp_servers: tuple[str, ...] = Field(min_length=3)

    candle_offset_seconds: float
    candle_uncertainty_seconds: float = Field(ge=0)
    calibration_sample_count: int = Field(ge=3)

    @field_validator("checked_at")
    @classmethod
    def require_utc_checked_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Clock evidence time must be timezone-aware.")

        if value.utcoffset().total_seconds() != 0:
            raise ValueError("Clock evidence time must already be UTC.")

        return value.astimezone(timezone.utc)

    @field_validator("verified", mode="before")
    @classmethod
    def require_strict_verified_boolean(cls, value):
        if not isinstance(value, bool):
            raise ValueError("verified must be a boolean.")
        return value

    @field_validator("ntp_offset_seconds", "ntp_uncertainty_seconds",
                     "candle_offset_seconds", "candle_uncertainty_seconds")
    @classmethod
    def require_finite_clock_values(cls, value: float) -> float:
        if not isfinite(value):
            raise ValueError("Clock evidence values must be finite.")
        return value

    @field_validator("broker_server", "symbol")
    @classmethod
    def normalize_clock_identity(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Clock evidence identity cannot be empty.")
        return normalized

    @field_validator("ntp_servers")
    @classmethod
    def normalize_ntp_servers(
        cls,
        value: tuple[str, ...],
    ) -> tuple[str, ...]:
        if any(
            not isinstance(server, str) or not server.strip()
            for server in value
        ):
            raise ValueError(
                "Every NTP server must be a non-empty string."
            )

        servers = tuple(server.strip().lower() for server in value)

        if len(servers) < 3:
            raise ValueError(
                "At least three non-empty NTP servers are required."
            )

        if len(set(servers)) != len(servers):
            raise ValueError("Duplicate NTP servers are not allowed.")

        return servers


class CandleBatch(BaseModel):
    """Chronologically ordered candles for one symbol and timeframe."""

    model_config = ConfigDict(frozen=True)

    symbol: str = Field(min_length=1)
    timeframe: str = Field(min_length=2)
    candles: tuple[MarketCandle, ...] = Field(min_length=1)

    received_at: datetime
    source: str = Field(min_length=1)
    clock_evidence: MarketDataClockEvidence | None = None

    @field_validator("received_at")
    @classmethod
    def require_utc_received_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware.")

        if value.utcoffset().total_seconds() != 0:
            raise ValueError("received_at must already be UTC.")

        return value.astimezone(timezone.utc)

    @field_validator("source")
    @classmethod
    def normalize_source(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("Batch source cannot be empty.")
        return normalized

    @model_validator(mode="after")
    def validate_batch(self) -> CandleBatch:
        timestamps = []

        for candle in self.candles:
            if candle.symbol != self.symbol:
                raise ValueError("All candles must belong to the batch symbol.")

            if candle.timeframe != self.timeframe:
                raise ValueError("All candles must use the batch timeframe.")

            timestamps.append(candle.timestamp)

        if timestamps != sorted(timestamps):
            raise ValueError("Candles must be ordered from oldest to newest.")

        if len(timestamps) != len(set(timestamps)):
            raise ValueError("Duplicate candle timestamps are not allowed.")

        return self
