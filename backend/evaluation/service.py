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
    EvaluationOutcomeRecord,
    EvaluationSampleSelectionRecord,
    EvaluationSignalRecord,
)
from backend.evaluation.contracts import (
    EvaluationHorizon,
    EvaluationOutcomeContract,
    EvaluationPeriodContract,
    EvaluationRunContract,
    EvaluationSignalContract,
    configuration_snapshot_sha256,
    validate_evaluation_period_set,
    validate_evaluation_relationships,
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
    """Creates reproducible evaluation runs, ledgers, and outcome persistence.

    Outcome measurement is performed by dedicated calculators using explicit
    Bid/Ask or OHLC evidence; this service validates lineage and persists the
    resulting counterfactual records without inventing execution costs.
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

    def persist_signals(
        self,
        run: EvaluationRunContract,
        periods: tuple[EvaluationPeriodContract, ...],
        signals: Iterable[EvaluationSignalContract],
    ) -> tuple[EvaluationSignalRecord, ...]:
        """Persist source-linked signal contracts after validating their run scope."""
        signal_items = tuple(signals)
        if not signal_items:
            return ()
        validate_evaluation_relationships(run, periods, signal_items, ())
        records = [
            EvaluationSignalRecord(
                id=str(signal.signal_id),
                run_id=str(signal.run_id),
                period_id=str(signal.period_id),
                contract_version=signal.contract_version,
                instrument=signal.instrument,
                signal_at=signal.signal_at,
                input_available_at=signal.input_available_at,
                direction=signal.direction.value if hasattr(signal.direction, "value") else signal.direction,
                reference_eligible=signal.reference_eligible,
                filter_decision=signal.filter_decision.value if hasattr(signal.filter_decision, "value") else signal.filter_decision,
                filter_reason=signal.filter_reason,
                source_candle_ids=list(signal.source_candle_ids),
                source_batch_ids=list(signal.source_batch_ids),
                input_snapshot=(
                    json.loads(json.dumps(signal.input_snapshot, ensure_ascii=False))
                    if signal.input_snapshot is not None
                    else None
                ),
                created_at=signal.created_at,
            )
            for signal in signal_items
        ]
        with self._database.session() as session:
            session.add_all(records)
        return tuple(records)

    def record_sample_selections(
        self,
        *,
        run_id: str,
        signals: Iterable[EvaluationSignalContract],
        horizons: tuple[EvaluationHorizon, ...] | None = None,
    ) -> tuple[EvaluationSampleSelectionRecord, ...]:
        """Persist one auditable decision for every candidate and enabled horizon.

        Every signal is retained in the decision ledger, including reference-
        ineligible candidates. Filter decisions never affect the reference sample.
        """
        signal_items = tuple(signals)
        if len({item.signal_id for item in signal_items}) != len(signal_items):
            raise ValueError("Signal IDs in a selection batch must be unique.")
        if any(str(item.run_id) != run_id for item in signal_items):
            raise ValueError("Every selection candidate must belong to the supplied run.")

        with self._database.session() as session:
            run_record = session.get(EvaluationRunRecord, run_id)
            if run_record is None:
                raise ValueError("Evaluation run does not exist.")
            enabled_horizons = tuple(run_record.horizons)
            selected_horizons = enabled_horizons if horizons is None else tuple(horizons)
            if (
                not selected_horizons
                or len(set(selected_horizons)) != len(selected_horizons)
                or any(value not in enabled_horizons for value in selected_horizons)
            ):
                raise ValueError("Requested horizons must be unique horizons enabled for this run.")

            if signal_items:
                supplied_ids = {str(item.signal_id) for item in signal_items}
                persisted_ids = set(
                    session.scalars(
                        select(EvaluationSignalRecord.id).where(
                            EvaluationSignalRecord.run_id == run_id,
                            EvaluationSignalRecord.id.in_(supplied_ids),
                        )
                    ).all()
                )
                if persisted_ids != supplied_ids:
                    raise ValueError("All selection candidates must already be persisted for this run.")

            records: list[EvaluationSampleSelectionRecord] = []
            ordered_candidates = sorted(
                signal_items,
                key=lambda item: (item.signal_at, str(item.signal_id)),
            )
            for horizon in selected_horizons:
                if horizon not in (5, 15, 30, 60):
                    raise ValueError("Unsupported evaluation horizon.")
                next_available_at: datetime | None = None
                holding_period = timedelta(minutes=horizon)
                for candidate_order, signal in enumerate(ordered_candidates, start=1):
                    if not signal.reference_eligible:
                        reason = "INELIGIBLE_REFERENCE"
                        selected = False
                    elif (
                        next_available_at is not None
                        and signal.signal_at < next_available_at
                    ):
                        reason = "OVERLAPPING_HORIZON"
                        selected = False
                    else:
                        reason = "SELECTED"
                        selected = True
                        next_available_at = signal.signal_at + holding_period

                    records.append(
                        EvaluationSampleSelectionRecord(
                            id=str(uuid4()),
                            run_id=run_id,
                            signal_id=str(signal.signal_id),
                            period_id=str(signal.period_id),
                            horizon_minutes=horizon,
                            candidate_order=candidate_order,
                            selected=selected,
                            decision_reason=reason,
                        )
                    )
            session.add_all(records)
        return tuple(records)

    def get_sample_selections(
        self,
        *,
        run_id: str,
        horizon_minutes: EvaluationHorizon,
    ) -> tuple[EvaluationSampleSelectionRecord, ...]:
        """Read a stored horizon ledger in the exact deterministic candidate order."""
        with self._database.session() as session:
            return tuple(
                session.scalars(
                    select(EvaluationSampleSelectionRecord)
                    .where(
                        EvaluationSampleSelectionRecord.run_id == run_id,
                        EvaluationSampleSelectionRecord.horizon_minutes == horizon_minutes,
                    )
                    .order_by(EvaluationSampleSelectionRecord.candidate_order)
                ).all()
            )

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

    def persist_outcomes(
        self,
        run: EvaluationRunContract,
        periods: tuple[EvaluationPeriodContract, ...],
        signals: Iterable[EvaluationSignalContract],
        outcomes: Iterable[EvaluationOutcomeContract],
    ) -> tuple[EvaluationOutcomeRecord, ...]:
        """Persist validated counterfactual outcomes for already-persisted signals."""
        signal_items = tuple(signals)
        outcome_items = tuple(outcomes)
        validate_evaluation_relationships(
            run, periods, signal_items, outcome_items
        )
        if not outcome_items:
            return ()

        signal_ids = {str(item.signal_id) for item in signal_items}
        if any(str(item.signal_id) not in signal_ids for item in outcome_items):
            raise ValueError("Every outcome must reference a supplied signal.")

        with self._database.session() as session:
            stored_run = session.get(EvaluationRunRecord, str(run.run_id))
            if stored_run is None:
                raise ValueError("Evaluation run must be persisted before outcomes.")
            stored_signal_ids = set(session.scalars(
                select(EvaluationSignalRecord.id).where(
                    EvaluationSignalRecord.run_id == str(run.run_id),
                    EvaluationSignalRecord.id.in_(signal_ids),
                )
            ).all())
            if stored_signal_ids != signal_ids:
                raise ValueError("Every outcome signal must already be persisted for this run.")

            records = [
                EvaluationOutcomeRecord(
                    id=str(item.outcome_id),
                    run_id=str(item.run_id),
                    signal_id=str(item.signal_id),
                    period_id=str(item.period_id),
                    contract_version=item.contract_version,
                    horizon_minutes=item.horizon_minutes,
                    outcome_type=item.outcome_type,
                    direction=item.direction.value,
                    status=item.status.value,
                    measurement_basis=item.measurement_basis.value,
                    entry_at=item.entry_at,
                    exit_at=item.exit_at,
                    entry_price=item.entry_price,
                    exit_price=item.exit_price,
                    entry_price_side=item.entry_price_side.value,
                    exit_price_side=item.exit_price_side.value,
                    gross_pnl=item.gross_pnl,
                    spread_accounting=item.spread_accounting.value,
                    spread_cost=item.spread_cost,
                    slippage_cost=item.slippage_cost,
                    slippage_knowledge=item.slippage_knowledge.value,
                    commission_cost=item.commission_cost,
                    commission_knowledge=item.commission_knowledge.value,
                    swap_adjustment=item.swap_adjustment,
                    swap_knowledge=item.swap_knowledge.value,
                    costs_complete=item.costs_complete,
                    net_pnl=item.net_pnl,
                    created_at=item.created_at,
                )
                for item in outcome_items
            ]
            session.add_all(records)
        return tuple(records)

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
