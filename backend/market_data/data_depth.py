from __future__ import annotations

from datetime import datetime, timezone

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.market_data.repository import MarketDataRepository
from backend.mt5.timeframes import SUPPORTED_TIMEFRAMES


class TimeframeDataDepth(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    timeframe: str = Field(min_length=2)
    status: str
    candle_count: int = Field(ge=0)
    first_candle_at: datetime | None = None
    last_candle_at: datetime | None = None
    history_span_seconds: float | None = Field(default=None, ge=0)
    batch_count: int = Field(ge=0)
    clock_verified_batch_count: int = Field(ge=0)
    last_received_at: datetime | None = None

    @field_validator(
        "first_candle_at", "last_candle_at", "last_received_at"
    )
    @classmethod
    def require_utc_datetimes(cls, value: datetime | None):
        if value is None:
            return value
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Depth timestamps must be timezone-aware.")
        if value.utcoffset().total_seconds() != 0:
            raise ValueError("Depth timestamps must already be UTC.")
        return value


class MarketDataDepthReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: str = Field(min_length=1)
    generated_at: datetime
    timeframes: tuple[TimeframeDataDepth, ...]

    @field_validator("generated_at")
    @classmethod
    def require_utc_generated_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Report generation time must be timezone-aware.")
        if value.utcoffset().total_seconds() != 0:
            raise ValueError("Report generation time must already be UTC.")
        return value


class MarketDataDepthAuditor:
    """Build a factual depth report from candles already persisted in DB."""

    def __init__(self, repository: MarketDataRepository) -> None:
        self._repository = repository

    def audit(self, symbol: str) -> MarketDataDepthReport:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must be a non-empty string.")

        summaries = self._repository.get_data_depth(
            symbol.strip(), tuple(SUPPORTED_TIMEFRAMES)
        )
        timeframe_reports: list[TimeframeDataDepth] = []
        for summary in summaries:
            span = None
            if (
                summary.first_candle_at is not None
                and summary.last_candle_at is not None
            ):
                span = max(
                    0.0,
                    (summary.last_candle_at - summary.first_candle_at).total_seconds(),
                )

            timeframe_reports.append(
                TimeframeDataDepth(
                    timeframe=summary.timeframe,
                    status="AVAILABLE" if summary.candle_count else "NO_DATA",
                    candle_count=summary.candle_count,
                    first_candle_at=summary.first_candle_at,
                    last_candle_at=summary.last_candle_at,
                    history_span_seconds=span,
                    batch_count=summary.batch_count,
                    clock_verified_batch_count=summary.clock_verified_batch_count,
                    last_received_at=summary.last_received_at,
                )
            )

        return MarketDataDepthReport(
            symbol=symbol.strip(),
            generated_at=datetime.now(timezone.utc),
            timeframes=tuple(timeframe_reports),
        )
