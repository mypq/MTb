from __future__ import annotations

from datetime import datetime, timezone
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.market_data.repository import MarketDataRepository
from backend.mt5.timeframes import SUPPORTED_TIMEFRAMES


class TimeframeDataDepth(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    timeframe: str = Field(min_length=2)
    status: Literal["AVAILABLE", "NO_DATA"]
    ingestion_status: Literal["VALIDATED", "LIMITED", "NO_DATA"]
    candle_count: int = Field(ge=0)
    first_candle_at: datetime | None = None
    last_candle_at: datetime | None = None
    history_span_seconds: float | None = Field(default=None, ge=0)
    batch_count: int = Field(ge=0)
    clock_verified_batch_count: int = Field(ge=0)
    validation_report_batch_count: int = Field(ge=0)
    analysis_eligible_batch_count: int = Field(ge=0)
    freshness_verified_batch_count: int = Field(ge=0)
    validation_coverage_percent: float = Field(ge=0, le=100)
    validation_issue_count: int = Field(ge=0)
    unconfirmed_gap_count: int = Field(ge=0)
    verified_candle_count: int = Field(ge=0)
    closed_candle_count: int = Field(ge=0)
    unknown_volume_candle_count: int = Field(ge=0)
    last_received_at: datetime | None = None
    last_validation_at: datetime | None = None
    history_sufficiency: Literal["NOT_ASSESSED"] = "NOT_ASSESSED"
    readiness_limitations: tuple[str, ...] = ()

    @field_validator(
        "first_candle_at",
        "last_candle_at",
        "last_received_at",
        "last_validation_at",
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


class EvaluationHorizonRequirement(BaseModel):
    """Frozen minimum independent-trade sample policy for one evaluation horizon."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    horizon_minutes: Literal[5, 15, 30, 60]
    minimum_completed_non_overlapping_trades: int = Field(default=300, ge=1)
    independent_period_count: Literal[3] = 3
    minimum_trades_per_period: int = Field(default=75, ge=1)
    insufficient_sample_status: Literal["NEEDS_MORE_DATA"] = "NEEDS_MORE_DATA"


class MarketDataDepthReport(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    symbol: str = Field(min_length=1)
    generated_at: datetime
    candle_data_status: Literal["NO_DATA", "LIMITED", "VALIDATED"]
    data_foundation_status: Literal["PARTIAL"] = "PARTIAL"
    history_sufficiency_assessed: bool = False
    evaluation_sample_policy: tuple[EvaluationHorizonRequirement, ...] = (
        EvaluationHorizonRequirement(horizon_minutes=5),
        EvaluationHorizonRequirement(horizon_minutes=15),
        EvaluationHorizonRequirement(horizon_minutes=30),
        EvaluationHorizonRequirement(horizon_minutes=60),
    )
    limitations: tuple[str, ...]
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
    """Audit persisted candle depth and the evidence attached at ingestion."""

    REPORT_LIMITATIONS = (
        "This audit covers persisted OHLC candles only.",
        "Historical Tick and Quote archive readiness is not assessed here.",
        "Historical spread distributions and shadow slippage are not assessed here.",
        "Evaluation sample requirements are defined per horizon, but completed-trade outcomes are not stored or assessed by this candle-depth audit.",
        "History sufficiency remains NOT_ASSESSED until each horizon has an independently verified evaluation sample.",
    )

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
                    (
                        summary.last_candle_at - summary.first_candle_at
                    ).total_seconds(),
                )

            coverage = (
                summary.validation_report_batch_count
                / summary.batch_count * 100.0
                if summary.batch_count
                else 0.0
            )

            limitations: list[str] = []
            if summary.candle_count == 0:
                ingestion_status = "NO_DATA"
                limitations.append("No persisted candles for this timeframe.")
            else:
                if summary.clock_verified_batch_count != summary.batch_count:
                    limitations.append("One or more ingestion batches lack verified clock evidence.")
                if summary.validation_report_batch_count != summary.batch_count:
                    limitations.append("One or more batches have no persisted validation report.")
                if summary.analysis_eligible_batch_count != summary.batch_count:
                    limitations.append("One or more batches are not recorded as analysis-eligible.")
                if summary.freshness_verified_batch_count != summary.batch_count:
                    limitations.append("Freshness was not verified for every batch.")
                if summary.verified_candle_count != summary.candle_count:
                    limitations.append("One or more candles are not marked VERIFIED.")
                if summary.closed_candle_count != summary.candle_count:
                    limitations.append("One or more candles are not closed.")
                if summary.unconfirmed_gap_count:
                    limitations.append("The ingestion reports contain unconfirmed candle gaps.")
                if summary.unknown_volume_candle_count:
                    limitations.append("Some candles have an unknown volume source.")

                blocking_limitations = [
                    item for item in limitations
                    if not item.startswith("Some candles have an unknown volume source.")
                ]
                ingestion_status = (
                    "VALIDATED" if not blocking_limitations else "LIMITED"
                )

            timeframe_reports.append(
                TimeframeDataDepth(
                    timeframe=summary.timeframe,
                    status=(
                        "AVAILABLE" if summary.candle_count else "NO_DATA"
                    ),
                    ingestion_status=ingestion_status,
                    candle_count=summary.candle_count,
                    first_candle_at=summary.first_candle_at,
                    last_candle_at=summary.last_candle_at,
                    history_span_seconds=span,
                    batch_count=summary.batch_count,
                    clock_verified_batch_count=summary.clock_verified_batch_count,
                    validation_report_batch_count=summary.validation_report_batch_count,
                    analysis_eligible_batch_count=summary.analysis_eligible_batch_count,
                    freshness_verified_batch_count=summary.freshness_verified_batch_count,
                    validation_coverage_percent=round(coverage, 2),
                    validation_issue_count=summary.validation_issue_count,
                    unconfirmed_gap_count=summary.unconfirmed_gap_count,
                    verified_candle_count=summary.verified_candle_count,
                    closed_candle_count=summary.closed_candle_count,
                    unknown_volume_candle_count=summary.unknown_volume_candle_count,
                    last_received_at=summary.last_received_at,
                    last_validation_at=summary.last_validation_at,
                    readiness_limitations=tuple(limitations),
                )
            )

        if all(item.candle_count == 0 for item in timeframe_reports):
            candle_data_status = "NO_DATA"
        elif all(item.ingestion_status == "VALIDATED" for item in timeframe_reports):
            candle_data_status = "VALIDATED"
        else:
            candle_data_status = "LIMITED"

        return MarketDataDepthReport(
            symbol=symbol.strip(),
            generated_at=datetime.now(timezone.utc),
            candle_data_status=candle_data_status,
            limitations=self.REPORT_LIMITATIONS,
            timeframes=tuple(timeframe_reports),
        )
