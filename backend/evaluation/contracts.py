from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from enum import Enum
from math import isfinite
from typing import Any, Literal, Mapping
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


class ExecutionPriceSide(str, Enum):
    BID = "BID"
    ASK = "ASK"
    UNKNOWN = "UNKNOWN"


class CostKnowledge(str, Enum):
    MEASURED = "MEASURED"
    NOT_APPLICABLE = "NOT_APPLICABLE"
    UNKNOWN = "UNKNOWN"


def canonical_snapshot_json(snapshot: Mapping[str, Any]) -> str:
    """Stable JSON representation used for evaluation configuration identity."""
    try:
        return json.dumps(
            snapshot,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("Configuration snapshot must contain finite JSON values.") from exc


def configuration_snapshot_sha256(snapshot: Mapping[str, Any]) -> str:
    return hashlib.sha256(canonical_snapshot_json(snapshot).encode("utf-8")).hexdigest()


class FrozenDict(dict):
    """A JSON-serializable dict that rejects mutation, including nested values."""

    def _immutable(self, *_args, **_kwargs):
        raise TypeError("Evaluation snapshots are immutable.")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable

    def __deepcopy__(self, memo):
        return self


def _deep_freeze(value: Any) -> Any:
    if isinstance(value, dict):
        frozen = dict.__new__(FrozenDict)
        dict.__init__(frozen, {key: _deep_freeze(item) for key, item in value.items()})
        return frozen
    if isinstance(value, (list, tuple)):
        return tuple(_deep_freeze(item) for item in value)
    return value


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

    @field_validator("configuration_snapshot", mode="before")
    @classmethod
    def freeze_configuration_snapshot(cls, value: dict) -> dict:
        if not isinstance(value, dict):
            raise ValueError("Configuration snapshot must be a JSON object.")
        canonical_snapshot_json(value)
        return _deep_freeze(value)

    @model_validator(mode="after")
    def validate_run_definition(self) -> EvaluationRunContract:
        if self.data_start_at >= self.data_end_at:
            raise ValueError("Evaluation data range must have positive duration.")
        if not self.horizons or len(set(self.horizons)) != len(self.horizons):
            raise ValueError("Evaluation horizons must be non-empty and unique.")
        if self.minimum_outcomes_per_period * 3 > self.minimum_completed_outcomes_per_horizon:
            raise ValueError("Per-period minimum cannot exceed the total sample minimum.")
        expected_hash = configuration_snapshot_sha256(self.configuration_snapshot)
        if self.configuration_sha256 != expected_hash:
            raise ValueError("Configuration SHA-256 does not match the canonical snapshot.")
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


def validate_evaluation_period_set(
    run: EvaluationRunContract,
    periods: tuple[EvaluationPeriodContract, ...] | list[EvaluationPeriodContract],
) -> tuple[EvaluationPeriodContract, ...]:
    """Validate all three periods together against the immutable run range."""
    ordered = tuple(sorted(periods, key=lambda item: item.period_index))
    if len(ordered) != 3 or tuple(p.period_index for p in ordered) != (1, 2, 3):
        raise ValueError("An evaluation run requires exactly periods 1, 2, and 3.")
    if len({p.period_id for p in ordered}) != 3:
        raise ValueError("Evaluation period IDs must be unique.")
    if any(p.run_id != run.run_id for p in ordered):
        raise ValueError("Every evaluation period must belong to the supplied run.")

    duration = run.data_end_at - run.data_start_at
    first_boundary = run.data_start_at + duration / 3
    second_boundary = run.data_start_at + duration * 2 / 3
    expected = (
        (run.data_start_at, first_boundary),
        (first_boundary, second_boundary),
        (second_boundary, run.data_end_at),
    )
    for period, (expected_start, expected_end) in zip(ordered, expected):
        if period.start_at != expected_start or period.end_at != expected_end:
            raise ValueError(
                "Evaluation periods must be contiguous, equal-duration, and cover the full run range."
            )
    return ordered


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

    @field_validator("input_snapshot", mode="before")
    @classmethod
    def freeze_input_snapshot(cls, value: dict | None) -> dict | None:
        if value is None:
            return None
        if not isinstance(value, dict):
            raise ValueError("Input snapshot must be a JSON object.")
        canonical_snapshot_json(value)
        return _deep_freeze(value)

    @model_validator(mode="after")
    def validate_signal_evidence(self) -> EvaluationSignalContract:
        if self.input_available_at > self.signal_at:
            raise ValueError("Signal cannot use input unavailable at decision time.")
        if self.filter_decision == FilterDecision.BLOCKED and not self.filter_reason:
            raise ValueError("Blocked signals require a filter reason.")
        return self


class EvaluationOutcomeContract(EvaluationContract):
    """Counterfactual outcome for one reference signal and one independent horizon."""

    contract_version: Literal["1.0"] = "1.0"
    record_type: Literal["evaluation_outcome"] = "evaluation_outcome"

    outcome_id: UUID
    run_id: UUID
    signal_id: UUID
    period_id: UUID
    horizon_minutes: EvaluationHorizon
    outcome_type: Literal["COUNTERFACTUAL"] = "COUNTERFACTUAL"
    direction: EvaluationDirection
    status: EvaluationOutcomeStatus
    measurement_basis: OutcomeMeasurementBasis
    entry_at: datetime | None = None
    exit_at: datetime | None = None
    entry_price: float | None = Field(default=None, gt=0)
    exit_price: float | None = Field(default=None, gt=0)
    entry_price_side: ExecutionPriceSide = ExecutionPriceSide.UNKNOWN
    exit_price_side: ExecutionPriceSide = ExecutionPriceSide.UNKNOWN
    gross_pnl: float | None = None
    spread_accounting: SpreadAccounting = SpreadAccounting.UNKNOWN
    spread_cost: float | None = Field(default=None, ge=0)
    slippage_cost: float | None = Field(default=None, ge=0)
    slippage_knowledge: CostKnowledge = CostKnowledge.UNKNOWN
    commission_cost: float | None = Field(default=None, ge=0)
    commission_knowledge: CostKnowledge = CostKnowledge.UNKNOWN
    # Signed adjustment: positive is a debit; negative is a swap credit.
    swap_adjustment: float | None = None
    swap_knowledge: CostKnowledge = CostKnowledge.UNKNOWN
    costs_complete: bool = False
    net_pnl: float | None = None
    created_at: datetime

    @field_validator("entry_at", "exit_at", "created_at")
    @classmethod
    def validate_outcome_timestamps(cls, value: datetime | None):
        if value is None:
            return value
        return cls._require_utc(value)

    @field_validator(
        "entry_price", "exit_price", "gross_pnl", "spread_cost",
        "slippage_cost", "commission_cost", "swap_adjustment", "net_pnl",
    )
    @classmethod
    def validate_finite_outcome_values(cls, value: float | None):
        if value is not None and not isfinite(value):
            raise ValueError("Outcome numeric values must be finite.")
        return value

    @staticmethod
    def _validate_cost_component(
        knowledge: CostKnowledge, amount: float | None, name: str
    ) -> None:
        if knowledge == CostKnowledge.MEASURED and amount is None:
            raise ValueError(f"Measured {name} requires a recorded amount.")
        if knowledge != CostKnowledge.MEASURED and amount is not None:
            raise ValueError(f"{name} amount is allowed only when measured.")

    @model_validator(mode="after")
    def validate_outcome_integrity(self) -> EvaluationOutcomeContract:
        if self.status == EvaluationOutcomeStatus.COMPLETED:
            required = (
                self.entry_at, self.exit_at, self.entry_price,
                self.exit_price, self.gross_pnl,
            )
            if any(value is None for value in required):
                raise ValueError("Completed outcomes require entry, exit, and gross PnL.")
            if self.measurement_basis == OutcomeMeasurementBasis.UNAVAILABLE:
                raise ValueError("Completed outcomes require a measurable price basis.")
            if self.exit_at <= self.entry_at:
                raise ValueError("Outcome exit must occur after entry.")

            expected_gross = (
                self.exit_price - self.entry_price
                if self.direction == EvaluationDirection.BUY
                else self.entry_price - self.exit_price
            )
            if abs(self.gross_pnl - expected_gross) > 1e-9:
                raise ValueError("Gross PnL does not match direction and entry/exit prices.")
        else:
            calculated_values = (
                self.entry_at, self.exit_at, self.entry_price, self.exit_price,
                self.gross_pnl, self.spread_cost, self.slippage_cost,
                self.commission_cost, self.swap_adjustment, self.net_pnl,
            )
            if any(value is not None for value in calculated_values):
                raise ValueError("Incomplete outcomes cannot contain calculated prices, costs, or PnL.")
            if self.costs_complete:
                raise ValueError("Incomplete outcomes cannot have complete costs.")

        if self.measurement_basis == OutcomeMeasurementBasis.BID_ASK_QUOTES:
            if self.spread_accounting != SpreadAccounting.EMBEDDED_IN_BID_ASK_PRICES:
                raise ValueError("Bid/Ask execution prices must record spread as embedded.")
            if self.spread_cost is not None:
                raise ValueError("Do not subtract spread again when Bid/Ask prices include it.")
            expected_sides = (
                (ExecutionPriceSide.ASK, ExecutionPriceSide.BID)
                if self.direction == EvaluationDirection.BUY
                else (ExecutionPriceSide.BID, ExecutionPriceSide.ASK)
            )
            if (self.entry_price_side, self.exit_price_side) != expected_sides:
                raise ValueError("Bid/Ask entry and exit sides do not match the signal direction.")
        elif self.measurement_basis == OutcomeMeasurementBasis.OHLC_APPROXIMATION:
            if self.spread_accounting != SpreadAccounting.UNKNOWN:
                raise ValueError("OHLC-only outcomes cannot claim measured spread accounting.")
            if self.spread_cost is not None:
                raise ValueError("OHLC-only outcomes cannot invent a spread cost.")
            if (self.entry_price_side, self.exit_price_side) != (
                ExecutionPriceSide.UNKNOWN, ExecutionPriceSide.UNKNOWN
            ):
                raise ValueError("OHLC approximations cannot claim executable Bid/Ask sides.")

        self._validate_cost_component(
            self.slippage_knowledge, self.slippage_cost, "slippage"
        )
        self._validate_cost_component(
            self.commission_knowledge, self.commission_cost, "commission"
        )
        self._validate_cost_component(
            self.swap_knowledge, self.swap_adjustment, "swap"
        )

        if self.costs_complete:
            if self.status != EvaluationOutcomeStatus.COMPLETED:
                raise ValueError("Costs can be complete only for completed outcomes.")
            if self.measurement_basis != OutcomeMeasurementBasis.BID_ASK_QUOTES:
                raise ValueError("Complete trading costs require measured Bid/Ask quotes.")
            knowledge = (
                self.slippage_knowledge, self.commission_knowledge, self.swap_knowledge
            )
            if any(value == CostKnowledge.UNKNOWN for value in knowledge):
                raise ValueError("Every applicable cost must be measured or explicitly not applicable.")
            if self.net_pnl is None:
                raise ValueError("Complete cost model requires net PnL.")
            expected_net = (
                self.gross_pnl
                - (self.slippage_cost or 0.0)
                - (self.commission_cost or 0.0)
                - (self.swap_adjustment or 0.0)
            )
            if abs(self.net_pnl - expected_net) > 1e-9:
                raise ValueError("Net PnL does not match gross PnL minus recorded cost adjustments.")
        elif self.net_pnl is not None:
            raise ValueError("Net PnL must remain unset until all applicable costs are known.")
        return self


def validate_evaluation_relationships(
    run: EvaluationRunContract,
    periods: tuple[EvaluationPeriodContract, ...] | list[EvaluationPeriodContract],
    signals: tuple[EvaluationSignalContract, ...] | list[EvaluationSignalContract],
    outcomes: tuple[EvaluationOutcomeContract, ...] | list[EvaluationOutcomeContract],
) -> None:
    """Validate cross-record identity and uniqueness before persistence."""
    ordered_periods = validate_evaluation_period_set(run, periods)
    periods_by_id = {period.period_id: period for period in ordered_periods}
    signals_by_id: dict[UUID, EvaluationSignalContract] = {}

    for signal in signals:
        if signal.signal_id in signals_by_id:
            raise ValueError("Evaluation signal IDs must be unique.")
        if signal.run_id != run.run_id:
            raise ValueError("Evaluation signal run_id does not match its run.")
        period = periods_by_id.get(signal.period_id)
        if period is None or period.run_id != signal.run_id:
            raise ValueError("Evaluation signal period_id does not belong to its run.")
        if signal.instrument != run.instrument:
            raise ValueError("Evaluation signal instrument does not match its run.")
        if not (period.start_at <= signal.signal_at < period.end_at):
            raise ValueError("Evaluation signal timestamp is outside its assigned period.")
        signals_by_id[signal.signal_id] = signal

    seen_outcomes: set[tuple[UUID, int]] = set()
    for outcome in outcomes:
        signal = signals_by_id.get(outcome.signal_id)
        if signal is None:
            raise ValueError("Evaluation outcome references an unknown signal.")
        if outcome.run_id != signal.run_id or outcome.period_id != signal.period_id:
            raise ValueError("Evaluation outcome run/period does not match its signal.")
        if outcome.direction != signal.direction:
            raise ValueError("Evaluation outcome direction does not match its signal.")
        key = (outcome.signal_id, outcome.horizon_minutes)
        if key in seen_outcomes:
            raise ValueError("A signal can have only one outcome per horizon.")
        if outcome.horizon_minutes not in run.horizons:
            raise ValueError("Outcome horizon is not enabled for its evaluation run.")
        seen_outcomes.add(key)
