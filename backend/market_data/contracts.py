from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class MarketCandleDataContract(BaseModel):
    """Versioned, auditable contract for one persisted market candle.

    event_time is when the market candle occurred; available_at is when the
    collector received its batch; recorded_at is when the database stored it.
    Validation and source-clock evidence remain nullable for legacy records.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    contract_version: Literal["1.0"] = "1.0"
    record_type: Literal["market_candle"] = "market_candle"

    candle_id: int = Field(gt=0)
    batch_id: int = Field(gt=0)
    instrument: str = Field(min_length=1)
    timeframe: str = Field(min_length=2)

    event_time: datetime
    available_at: datetime
    recorded_at: datetime

    open: float
    high: float
    low: float
    close: float
    tick_volume: int = Field(ge=0)
    real_volume: int | None = Field(default=None, ge=0)
    volume_source: str
    candle_quality: str
    is_closed: bool

    source: str = Field(min_length=1)
    source_account_login: int | None = None
    source_broker_server: str | None = None
    clock_verified: bool | None = None

    analysis_eligible: bool | None = None
    validation_report: dict[str, Any] | None = None

    @field_validator("event_time", "available_at", "recorded_at")
    @classmethod
    def require_utc_timestamps(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Contract timestamps must be timezone-aware.")
        if value.utcoffset().total_seconds() != 0:
            raise ValueError("Contract timestamps must already be UTC.")
        return value
