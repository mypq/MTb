from __future__ import annotations

from datetime import datetime
from enum import Enum
from math import isfinite
from typing import Literal
from uuid import UUID

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)


EvaluationHorizon = Literal[5, 15, 30, 60]


class EvaluationDirection(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class FilterDecision(str, Enum):
    ALLOWED = "ALLOWED"
    BLOCKED = "BLOCKED"
    NOT_APPLICABLE = "NOT_APPLICABLE"


class EvaluationOutcomeStatus(str, Enum):
    COMPLETED = "COMPLETED"
    NEEDS_MORE_DATA = "NEEDS_MORE_DATA"
    INVALID = "INVALID"


class OutcomeMeasurementBasis(str, Enum):
    BID_ASK_QUOTES = "BID_ASK_QUOTES"
    OHLC_APPROXIMATION = "OHLC_APPROXIMATION"
    UNAVAILABLE = "UNAVAILABLE"


class SpreadAccounting(str, Enum):
    EMBEDDED_IN_BID_ASK_PRICES = "EMBEDDED_IN_BID_ASK_PRICES"
    SEPARATE_COST = "SEPARATE_COST"
    UNKNOWN = "UNKNOWN"


class EvaluationContract(BaseModel):
    """Shared strict rules for persisted evaluation records."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    @staticmethod
    def _require_utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Evaluation timestamps must be timezone-aware.")
        if value.utcoffset().total_seconds() != 0:
            raise ValueError("Evaluation timestamps must already be UTC.")
        return value

    @field_validator("created_at", check_fields=False)
    @classmethod
    def validate_created_at_utc(cls, value: datetime) -> datetime:
        return cls._require_utc(value)


class EvaluationRunContract(EvaluationContract):
    """Immutable definition and reproducibility metadata for one evaluation."""

    contract_version: Literal["1.0"] = "1.0"
    record_type: Literal["evaluation_run"] = "evaluation_run"

    run_id: UUID
    instrument: str = Field(min_length=1, max_length=64)
    created_at: datetime
    data_start_at: datetime
    data_end_at: datetime
    code_revision: str = Field(min_length=1, max_length=128)
    configuration_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    configuration_snapshot: dict
    sampling_rule: Literal["FIRST_ELIGIBLE_NON_OVERLAPPING_V1"] = (
        "FIRST_ELIGIBLE_NON_OVERLAPPING_V1"
    )
    period_partition: Literal["THREE_EQUAL_DURATION_CHRONOLOGICAL"] = (
        "THREE_EQUAL_DURATION_CHRONOLOGICAL"
    )
    horizons: tuple[EvaluationHorizon, ...] = (5, 15, 30, 60)
    minimum_completed_outcomes_per_horizon: int = Field(default=300, ge=1)
    minimum_outcomes_per_period: int = Field(default=75, ge=1)
    status: Literal["CREATED", "RUNNING", "COMPLETED", "FAILED"] = "CREATED"

    @field_validator("created_at", "data_start_at", "data_end_at")
    @classmethod
    def validate_run_timestamps(cls, value: datetime) -> datetime:
        return cls._require_utc(value)

    @field_validator("instrument")
    @classmethod
    def normalize_instrument(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("Instrument cannot be empty.")
        return normalized

    @model_validator(mode="after")
    def validate_run_definition(self) -> EvaluationRunContract:
        if self.data_start_at >= self.data_end_at:
            raise ValueError("Evaluation data range must have positive duration.")
        if not self.horizons or len(set(self.horizons)) != len(self.horizons):
            raise ValueError("Evaluation horizons must be non-empty and unique.")
        if self.minimum_outcomes_per_period * 3 > self.minimum_completed_outcomes_per_horizon:
            raise ValueError("Per-period minimum cannot exceed the total sample minimum.")
        return self


class EvaluationPeriodContract(EvaluationContract):
    """One of three chronological, equal-duration partitions of a run."""

    contract_version: Literal["1.0"] = "1.0"
    record_type: Literal["evaluation_period"] = "evaluation_period"

    period_id: UUID
    run_id: UUID
    period_index: Literal[1, 2, 3]
    start_at: datetime
    end_at: datetime
    created_at: datetime
    partition_method: Literal["THREE_EQUAL_DURATION_CHRONOLOGICAL"] = (
        "THREE_EQUAL_DURATION_CHRONOLOGICAL"
    )

    @field_validator("start_at", "end_at", "created_at")
    @classmethod
    def validate_period_timestamps(cls, value: datetime) -> datetime:
        return cls._require_utc(value)

    @model_validator(mode="after")
    def validate_period_range(self) -> EvaluationPeriodContract:
        if self.start_at >= self.end_at:
            raise ValueError("Evaluation period must have positive duration.")
        return self


class EvaluationSignalContract(EvaluationContract):
    """A reference signal and its filter decision, including blocked signals."""

    contract_version: Literal["1.0"] = "1.0"
    record_type: Literal["evaluation_signal"] = "evaluation_signal"

    signal_id: UUID
    run_id: UUID
    period_id: UUID
    instrument: str = Field(min_length=1, max_length=64)
    signal_at: datetime
    input_available_at: datetime
    direction: EvaluationDirection
    reference_eligible: bool
    filter_decision: FilterDecision
    filter_reason: str | None = Field(default=None, max_length=512)
    source_candle_ids: tuple[int, ...] = Field(min_length=1)
    source_batch_ids: tuple[int, ...] = Field(min_length=1)
    input_snapshot: dict | None = None
    created_at: datetime

    @field_validator("signal_at", "input_available_at", "created_at")
    @classmethod
    def validate_signal_timestamps(cls, value: datetime) -> datetime:
        return cls._require_utc(value)

    @field_validator("instrument")
    @classmethod
    def normalize_signal_instrument(cls, value: str) -> str:
        normalized = value.strip().upper()
        if not normalized:
            raise ValueError("Instrument cannot be empty.")
        return normalized

    @field_validator("source_candle_ids", "source_batch_ids")
    @classmethod
    def validate_source_ids(cls, values: tuple[int, ...]) -> tuple[int, ...]:
        if any(isinstance(value, bool) or value <= 0 for value in values):
            raise ValueError("Source record IDs must be positive integers.")
        if len(set(values)) != len(values):
            raise ValueError("Source record IDs must be unique.")
        return values

    @model_validator(mode="after")
    def validate_signal_evidence(self) -> EvaluationSignalContract:
        if self.input_available_at > self.signal_at:
            raise ValueError("Signal cannot use input unavailable at decision time.")
        if self.filter_decision == FilterDecision.BLOCKED and not self.filter_reason:
            raise ValueError("Blocked signals require a filter reason.")
        return self


class EvaluationOutcomeContract(EvaluationContract):
    """Counterfactual outcome for one signal and one independent horizon."""

    contract_version: Literal["1.0"] = "1.0"
    record_type: Literal["evaluation_outcome"] = "evaluation_outcome"

    outcome_id: UUID
    run_id: UUID
    signal_id: UUID
    period_id: UUID
    horizon_minutes: EvaluationHorizon
    outcome_type: Literal["COUNTERFACTUAL"] = "COUNTERFACTUAL"
    status: EvaluationOutcomeStatus
    measurement_basis: OutcomeMeasurementBasis
    entry_at: datetime | None = None
    exit_at: datetime | None = None
    entry_price: float | None = Field(default=None, gt=0)
    exit_price: float | None = Field(default=None, gt=0)
    gross_pnl: float | None = None
    spread_accounting: SpreadAccounting = SpreadAccounting.UNKNOWN
    spread_cost: float | None = Field(default=None, ge=0)
    slippage_cost: float | None = Field(default=None, ge=0)
    commission_cost: float | None = Field(default=None, ge=0)
    swap_cost: float | None = Field(default=None, ge=0)
    costs_complete: bool = False
    net_pnl: float | None = None
    created_at: datetime

    @field_validator("entry_at", "exit_at", "created_at")
    @classmethod
    def validate_outcome_timestamps(cls, value: datetime | None):
        if value is None:
            return value
        return cls._require_utc(value)

    @field_validator("entry_price", "exit_price", "gross_pnl", "spread_cost",
                     "slippage_cost", "commission_cost", "swap_cost", "net_pnl")
    @classmethod
    def validate_finite_outcome_values(cls, value: float | None):
        if value is not None and not isfinite(value):
            raise ValueError("Outcome numeric values must be finite.")
        return value

    @model_validator(mode="after")
    def validate_outcome_integrity(self) -> EvaluationOutcomeContract:
        if self.status == EvaluationOutcomeStatus.COMPLETED:
            required = (
                self.entry_at,
                self.exit_at,
                self.entry_price,
                self.exit_price,
                self.gross_pnl,
            )
            if any(value is None for value in required):
                raise ValueError("Completed outcomes require entry, exit, and gross PnL.")
            if self.measurement_basis == OutcomeMeasurementBasis.UNAVAILABLE:
                raise ValueError("Completed outcomes require a measurable price basis.")
            if self.exit_at <= self.entry_at:
                raise ValueError("Outcome exit must occur after entry.")
        elif any(value is not None for value in (
            self.entry_at, self.exit_at, self.entry_price, self.exit_price,
            self.gross_pnl, self.net_pnl,
        )):
            raise ValueError("Incomplete outcomes cannot contain calculated prices or PnL.")

        if self.measurement_basis == OutcomeMeasurementBasis.BID_ASK_QUOTES:
            if self.spread_accounting != SpreadAccounting.EMBEDDED_IN_BID_ASK_PRICES:
                raise ValueError("Bid/Ask execution prices must record spread as embedded.")
            if self.spread_cost is not None:
                raise ValueError("Do not subtract spread again when Bid/Ask prices include it.")
        elif self.measurement_basis == OutcomeMeasurementBasis.OHLC_APPROXIMATION:
            if self.spread_accounting != SpreadAccounting.UNKNOWN:
                raise ValueError("OHLC-only outcomes cannot claim measured spread accounting.")
            if self.spread_cost is not None:
                raise ValueError("OHLC-only outcomes cannot invent a spread cost.")

        if self.costs_complete:
            if self.status != EvaluationOutcomeStatus.COMPLETED:
                raise ValueError("Costs can be complete only for completed outcomes.")
            if self.measurement_basis != OutcomeMeasurementBasis.BID_ASK_QUOTES:
                raise ValueError("Complete trading costs require measured Bid/Ask quotes.")
            if any(value is None for value in (
                self.slippage_cost, self.commission_cost, self.swap_cost,
            )):
                raise ValueError("Complete cost model requires slippage, commission, and swap values.")
            if self.net_pnl is None:
                raise ValueError("Complete cost model requires net PnL.")
            expected_net_pnl = (
                self.gross_pnl
                - self.slippage_cost
                - self.commission_cost
                - self.swap_cost
            )
            if abs(self.net_pnl - expected_net_pnl) > 1e-9:
                raise ValueError("Net PnL does not match gross PnL minus recorded costs.")
        elif self.net_pnl is not None:
            raise ValueError("Net PnL must remain unset until all applicable costs are known.")
        return self
