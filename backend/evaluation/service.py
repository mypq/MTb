from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Iterable
from uuid import uuid4

from sqlalchemy import select

from backend.database import Database
from backend.database_models import (
    EvaluationPeriodRecord,
    EvaluationRunRecord,
)
from backend.evaluation.contracts import (
    EvaluationHorizon,
    EvaluationPeriodContract,
    EvaluationRunContract,
    EvaluationSignalContract,
    configuration_snapshot_sha256,
    validate_evaluation_period_set,
)


@dataclass(frozen=True, slots=True)
class HorizonSampleReadiness:
    """Evidence that one horizon meets the pre-registered sample thresholds."""

    horizon_minutes: EvaluationHorizon
    selected_completed_total: int
    selected_completed_by_period: tuple[int, int, int]
    required_total: int
    required_per_period: int
    ready: bool
    status: str


class EvaluationService:
    """Creates reproducible evaluation runs and applies fixed sample rules.

    This first service layer deliberately does not calculate market outcomes.
    Outcome measurement belongs to a separate component with explicit Bid/Ask
    or OHLC evidence; it must never infer execution costs from missing data.
    """

    def __init__(self, database: Database) -> None:
        if not isinstance(database, Database):
            raise TypeError("database must be an initialized Database instance.")
        self._database = database

    def create_run(
        self,
        *,
        instrument: str,
        data_start_at: datetime,
        data_end_at: datetime,
        code_revision: str,
        configuration_snapshot: dict,
        created_at: datetime | None = None,
        horizons: tuple[EvaluationHorizon, ...] = (5, 15, 30, 60),
        minimum_completed_outcomes_per_horizon: int = 300,
        minimum_outcomes_per_period: int = 75,
    ) -> tuple[EvaluationRunContract, tuple[EvaluationPeriodContract, ...]]:
        """Persist one immutable run definition and its three equal periods."""
        created = created_at or datetime.now(timezone.utc)
        snapshot_json = json.dumps(
            configuration_snapshot,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        )
        snapshot = json.loads(snapshot_json)

        run = EvaluationRunContract(
            run_id=uuid4(),
            instrument=instrument,
            created_at=created,
            data_start_at=data_start_at,
            data_end_at=data_end_at,
            code_revision=code_revision,
            configuration_snapshot=snapshot,
            configuration_sha256=configuration_snapshot_sha256(snapshot),
            horizons=horizons,
            minimum_completed_outcomes_per_horizon=(
                minimum_completed_outcomes_per_horizon
            ),
            minimum_outcomes_per_period=minimum_outcomes_per_period,
        )

        duration = run.data_end_at - run.data_start_at
        boundaries = (
            run.data_start_at,
            run.data_start_at + duration / 3,
            run.data_start_at + duration * 2 / 3,
            run.data_end_at,
        )
        periods = tuple(
            EvaluationPeriodContract(
                period_id=uuid4(),
                run_id=run.run_id,
                period_index=index,
                start_at=boundaries[index - 1],
                end_at=boundaries[index],
                created_at=created,
            )
            for index in (1, 2, 3)
        )
        periods = validate_evaluation_period_set(run, periods)

        run_record = EvaluationRunRecord(
            id=str(run.run_id),
            contract_version=run.contract_version,
            instrument=run.instrument,
            created_at=run.created_at,
            data_start_at=run.data_start_at,
            data_end_at=run.data_end_at,
            code_revision=run.code_revision,
            configuration_sha256=run.configuration_sha256,
            configuration_snapshot=json.loads(snapshot_json),
            sampling_rule=run.sampling_rule,
            period_partition=run.period_partition,
            horizons=list(run.horizons),
            minimum_completed_outcomes_per_horizon=(
                run.minimum_completed_outcomes_per_horizon
            ),
            minimum_outcomes_per_period=run.minimum_outcomes_per_period,
            status=run.status,
        )
        period_records = [
            EvaluationPeriodRecord(
                id=str(period.period_id),
                run_id=str(period.run_id),
                contract_version=period.contract_version,
                period_index=period.period_index,
                start_at=period.start_at,
                end_at=period.end_at,
                created_at=period.created_at,
                partition_method=period.partition_method,
            )
            for period in periods
        ]

        with self._database.session() as session:
            session.add(run_record)
            session.add_all(period_records)

        return run, periods

    @staticmethod
    def select_non_overlapping_signals(
        signals: Iterable[EvaluationSignalContract],
        horizon_minutes: EvaluationHorizon,
    ) -> tuple[EvaluationSignalContract, ...]:
        """Select first eligible reference signals, independently per horizon.

        Filter decisions are intentionally ignored: a blocked signal remains
        in the reference sample so filters cannot alter the comparison set.
        """
        if horizon_minutes not in (5, 15, 30, 60):
            raise ValueError("Unsupported evaluation horizon.")

        candidates = tuple(signals)
        if any(not isinstance(item, EvaluationSignalContract) for item in candidates):
            raise TypeError("signals must contain EvaluationSignalContract values.")

        eligible = sorted(
            (item for item in candidates if item.reference_eligible),
            key=lambda item: (item.signal_at, str(item.signal_id)),
        )
        selected: list[EvaluationSignalContract] = []
        next_available_at: datetime | None = None
        holding_period = timedelta(minutes=horizon_minutes)

        for signal in eligible:
            if next_available_at is None or signal.signal_at >= next_available_at:
                selected.append(signal)
                next_available_at = signal.signal_at + holding_period

        return tuple(selected)

    @staticmethod
    def assess_horizon_sample(
        *,
        horizon_minutes: EvaluationHorizon,
        selected_completed_by_period: tuple[int, int, int],
        required_total: int = 300,
        required_per_period: int = 75,
    ) -> HorizonSampleReadiness:
        """Return NEEDS_MORE_DATA unless total and every period pass."""
        if horizon_minutes not in (5, 15, 30, 60):
            raise ValueError("Unsupported evaluation horizon.")
        if (
            not isinstance(selected_completed_by_period, tuple)
            or len(selected_completed_by_period) != 3
            or any(
                isinstance(count, bool) or not isinstance(count, int) or count < 0
                for count in selected_completed_by_period
            )
        ):
            raise ValueError("Exactly three non-negative integer period counts are required.")
        if (
            isinstance(required_total, bool)
            or not isinstance(required_total, int)
            or required_total < 1
            or isinstance(required_per_period, bool)
            or not isinstance(required_per_period, int)
            or required_per_period < 1
        ):
            raise ValueError("Sample thresholds must be positive integers.")

        total = sum(selected_completed_by_period)
        ready = (
            total >= required_total
            and all(count >= required_per_period for count in selected_completed_by_period)
        )
        return HorizonSampleReadiness(
            horizon_minutes=horizon_minutes,
            selected_completed_total=total,
            selected_completed_by_period=selected_completed_by_period,
            required_total=required_total,
            required_per_period=required_per_period,
            ready=ready,
            status="READY" if ready else "NEEDS_MORE_DATA",
        )

    def get_run_periods(
        self,
        run_id: str,
    ) -> tuple[EvaluationPeriodRecord, ...]:
        """Read the persisted periods in deterministic chronological order."""
        with self._database.session() as session:
            return tuple(
                session.scalars(
                    select(EvaluationPeriodRecord)
                    .where(EvaluationPeriodRecord.run_id == run_id)
                    .order_by(EvaluationPeriodRecord.period_index)
                ).all()
            )
