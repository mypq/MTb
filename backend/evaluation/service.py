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
    EvaluationOutcomeEvidenceRecord,
    EvaluationSampleSelectionRecord,
    EvaluationSignalRecord,
)
from backend.evaluation.market_data_series import (
    EvaluationCandleSeriesReader,
    EvaluationMarketDataRepository,
)
from backend.evaluation.ohlc_outcomes import EvaluationOHLCOutcomeCalculator
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



def _serialize_outcome_source_evidence(evidence) -> dict:
    """Create a JSON-safe, immutable-in-practice snapshot of the full M1 input."""

    def timestamp(value):
        return value.isoformat() if value is not None else None

    candles = []
    for candle in evidence.candles:
        candles.append({
            "candle_id": candle.candle_id,
            "batch_id": candle.batch_id,
            "instrument": candle.instrument,
            "timeframe": candle.timeframe,
            "event_time": timestamp(candle.event_time),
            "available_at": timestamp(candle.available_at),
            "recorded_at": timestamp(candle.recorded_at),
            "open": candle.open,
            "high": candle.high,
            "low": candle.low,
            "close": candle.close,
            "tick_volume": candle.tick_volume,
            "real_volume": candle.real_volume,
            "volume_source": candle.volume_source,
            "candle_quality": candle.candle_quality,
            "is_closed": candle.is_closed,
            "source": candle.source,
            "source_account_login": candle.source_account_login,
            "source_broker_server": candle.source_broker_server,
            "clock_verified": candle.clock_verified,
            "analysis_eligible": candle.analysis_eligible,
            "validation_report": candle.validation_report,
        })

    return {
        "instrument": evidence.instrument,
        "timeframe": evidence.timeframe,
        "requested_start_at": timestamp(evidence.start_at),
        "requested_end_at": timestamp(evidence.end_at),
        "available_at_cutoff": timestamp(evidence.available_at_cutoff),
        "latest_available_at": timestamp(evidence.latest_available_at),
        "source_candles": candles,
        "observed_gaps": [
            {
                "previous_event_time": timestamp(gap.previous_event_time),
                "next_event_time": timestamp(gap.next_event_time),
                "expected_interval_seconds": gap.expected_interval_seconds,
                "actual_interval_seconds": gap.actual_interval_seconds,
                "missing_intervals": gap.missing_intervals,
            }
            for gap in evidence.gaps
        ],
        "duplicate_event_times": [
            timestamp(value) for value in evidence.duplicate_event_times
        ],
        "is_contiguous": evidence.is_contiguous,
        "evidence_schema_version": 1,
    }


def _parse_evidence_timestamp(value, field: str) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"Source evidence {field} must be an ISO timestamp.")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Source evidence {field} is not a valid timestamp.") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"Source evidence {field} must be UTC-aware.")
    return parsed


def _validate_ohlc_evidence_snapshot(
    snapshot: dict,
    run: EvaluationRunContract,
    signal: EvaluationSignalContract,
    outcome: EvaluationOutcomeContract,
) -> None:
    """Validate that an M1 snapshot is complete and belongs to this outcome."""
    if not isinstance(snapshot, dict):
        raise ValueError("OHLC source evidence must be a mapping.")
    required = {
        "instrument", "timeframe", "requested_start_at", "requested_end_at",
        "available_at_cutoff", "latest_available_at", "source_candles",
        "observed_gaps", "duplicate_event_times", "is_contiguous",
        "evidence_schema_version",
    }
    if set(snapshot) != required:
        raise ValueError("OHLC source evidence has missing or unexpected fields.")
    if snapshot["evidence_schema_version"] != 1:
        raise ValueError("Unsupported OHLC source evidence schema version.")
    if snapshot["instrument"] != signal.instrument or snapshot["instrument"] != run.instrument:
        raise ValueError("OHLC source evidence instrument does not match its run and signal.")
    if snapshot["timeframe"] != "M1":
        raise ValueError("OHLC source evidence must use the M1 timeframe.")

    start = _parse_evidence_timestamp(snapshot["requested_start_at"], "requested_start_at")
    end = _parse_evidence_timestamp(snapshot["requested_end_at"], "requested_end_at")
    cutoff = _parse_evidence_timestamp(snapshot["available_at_cutoff"], "available_at_cutoff")
    if start != signal.signal_at:
        raise ValueError("OHLC source evidence start does not match the signal timestamp.")
    if cutoff != run.created_at:
        raise ValueError("OHLC source evidence cutoff does not match the frozen run cutoff.")
    if start >= end or end > signal.signal_at + timedelta(minutes=max(run.horizons) + 1):
        raise ValueError("OHLC source evidence range is outside the permitted outcome window.")
    if outcome.exit_at is not None and not (start <= outcome.entry_at < outcome.exit_at <= end):
        raise ValueError("OHLC source evidence does not cover the outcome measurement window.")

    candles = snapshot["source_candles"]
    gaps = snapshot["observed_gaps"]
    duplicate_times = snapshot["duplicate_event_times"]
    if not isinstance(candles, list) or not isinstance(gaps, list) or not isinstance(duplicate_times, list):
        raise ValueError("OHLC source evidence collections must be lists.")
    candle_ids = set()
    batch_ids = set()
    event_times = []
    available_times = []
    for candle in candles:
        if not isinstance(candle, dict):
            raise ValueError("Each source candle evidence item must be a mapping.")
        required_candle_fields = {
            "candle_id", "batch_id", "instrument", "timeframe", "event_time",
            "available_at", "recorded_at", "open", "high", "low", "close",
            "tick_volume", "real_volume", "volume_source", "candle_quality",
            "is_closed", "source", "source_account_login", "source_broker_server",
            "clock_verified", "analysis_eligible", "validation_report",
        }
        if set(candle) != required_candle_fields:
            raise ValueError("Source candle evidence has missing or unexpected fields.")
        candle_id, batch_id = candle["candle_id"], candle["batch_id"]
        if isinstance(candle_id, bool) or not isinstance(candle_id, int) or candle_id <= 0:
            raise ValueError("Source candle IDs must be positive integers.")
        if isinstance(batch_id, bool) or not isinstance(batch_id, int) or batch_id <= 0:
            raise ValueError("Source batch IDs must be positive integers.")
        if candle_id in candle_ids:
            raise ValueError("Source candle IDs must not be duplicated.")
        candle_ids.add(candle_id)
        batch_ids.add(batch_id)
        if candle["instrument"] != signal.instrument or candle["timeframe"] != "M1":
            raise ValueError("A source candle does not match the signal instrument and M1 timeframe.")
        event_time = _parse_evidence_timestamp(candle["event_time"], "candle.event_time")
        available_at = _parse_evidence_timestamp(candle["available_at"], "candle.available_at")
        _parse_evidence_timestamp(candle["recorded_at"], "candle.recorded_at")
        if not (start <= event_time < end):
            raise ValueError("A source candle falls outside the requested evidence range.")
        if available_at > cutoff:
            raise ValueError("A source candle became available after the frozen run cutoff.")
        if candle["is_closed"] is not True or candle["candle_quality"] != "VERIFIED":
            raise ValueError("Source candles must be closed and verified.")
        if candle["clock_verified"] is not True or candle["analysis_eligible"] is not True:
            raise ValueError("Source candles must have verified clock and analysis eligibility.")
        if not isinstance(candle["validation_report"], dict) or candle["validation_report"].get("analysis_eligible") is not True:
            raise ValueError("Source candles require persisted positive validation evidence.")
        event_times.append(event_time)
        available_times.append(available_at)

    if event_times != sorted(event_times):
        raise ValueError("Source candles must be stored in chronological order.")
    expected_duplicates = sorted({
        value.isoformat() for value in event_times
        if event_times.count(value) > 1
    })
    normalized_duplicates = sorted(
        _parse_evidence_timestamp(value, "duplicate_event_times").isoformat()
        for value in duplicate_times
    )
    if normalized_duplicates != expected_duplicates:
        raise ValueError("Duplicate event-time evidence does not match the source candles.")
    if any(not isinstance(gap, dict) or set(gap) != {
        "previous_event_time", "next_event_time", "expected_interval_seconds",
        "actual_interval_seconds", "missing_intervals",
    } for gap in gaps):
        raise ValueError("Observed gap evidence has an invalid structure.")
    for gap in gaps:
        previous = _parse_evidence_timestamp(gap["previous_event_time"], "gap.previous_event_time")
        following = _parse_evidence_timestamp(gap["next_event_time"], "gap.next_event_time")
        if following <= previous or gap["actual_interval_seconds"] != int((following - previous).total_seconds()):
            raise ValueError("Observed gap timing is inconsistent.")
        if gap["expected_interval_seconds"] != 60:
            raise ValueError("M1 observed gaps must declare a 60-second expected interval.")
    expected_contiguous = bool(candles) and not gaps and not duplicate_times
    if snapshot["is_contiguous"] is not expected_contiguous:
        raise ValueError("Source evidence contiguity flag conflicts with its gaps or duplicates.")
    latest = snapshot["latest_available_at"]
    if available_times:
        if _parse_evidence_timestamp(latest, "latest_available_at") != max(available_times):
            raise ValueError("Latest source availability does not match the source candles.")
    elif latest is not None:
        raise ValueError("Latest source availability must be null when no candles are present.")


@dataclass(frozen=True, slots=True)
class HorizonSampleReadiness:
    """Persisted selected-sample counts and readiness for one horizon."""

    horizon_minutes: EvaluationHorizon
    candidate_total: int
    candidates_by_period: tuple[int, int, int]
    selected_total: int
    selected_by_period: tuple[int, int, int]
    overlapping_by_period: tuple[int, int, int]
    ineligible_by_period: tuple[int, int, int]
    selected_completed_total: int
    selected_completed_by_period: tuple[int, int, int]
    selected_not_completed_by_period: tuple[int, int, int]
    required_total: int
    required_per_period: int
    ready: bool
    status: str


@dataclass(frozen=True, slots=True)
class HorizonSampleAssessment:
    """Threshold assessment for completed selected counts, without ledger totals."""

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

            supplied_ids = {str(item.signal_id) for item in signal_items}
            persisted_ids = set(
                session.scalars(
                    select(EvaluationSignalRecord.id).where(
                        EvaluationSignalRecord.run_id == run_id
                    )
                ).all()
            )
            if persisted_ids != supplied_ids:
                raise ValueError(
                    "Selection must include every persisted signal for this run exactly once."
                )

            existing_ledger_horizons = set(
                session.scalars(
                    select(EvaluationSampleSelectionRecord.horizon_minutes).where(
                        EvaluationSampleSelectionRecord.run_id == run_id,
                        EvaluationSampleSelectionRecord.horizon_minutes.in_(selected_horizons),
                    )
                ).all()
            )
            if existing_ledger_horizons:
                raise ValueError(
                    "Sample selection ledger already exists for requested horizon(s): "
                    + ", ".join(str(value) for value in sorted(existing_ledger_horizons))
                )

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
    ) -> HorizonSampleAssessment:
        """Assess completed selected counts without implying ledger totals."""
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
        return HorizonSampleAssessment(
            horizon_minutes=horizon_minutes,
            selected_completed_total=total,
            selected_completed_by_period=selected_completed_by_period,
            required_total=required_total,
            required_per_period=required_per_period,
            ready=ready,
            status="READY" if ready else "NEEDS_MORE_DATA",
        )

    def get_run_readiness(
        self,
        *,
        run_id: str,
    ) -> tuple[HorizonSampleReadiness, ...]:
        """Aggregate selected ledger rows with COMPLETED outcomes per period.

        Only rows selected by the frozen sample ledger are candidates for the
        readiness count. Missing, NEEDS_MORE_DATA, and INVALID outcomes are
        reported as not completed and never contribute to the required sample.
        """
        with self._database.session() as session:
            run = session.get(EvaluationRunRecord, run_id)
            if run is None:
                raise ValueError("Evaluation run does not exist.")

            horizons = tuple(run.horizons)
            if not horizons or any(value not in (5, 15, 30, 60) for value in horizons):
                raise ValueError("Evaluation run contains unsupported horizons.")

            rows = session.execute(
                select(
                    EvaluationSampleSelectionRecord.horizon_minutes,
                    EvaluationPeriodRecord.period_index,
                    EvaluationSampleSelectionRecord.selected,
                    EvaluationSampleSelectionRecord.decision_reason,
                    EvaluationOutcomeRecord.status,
                )
                .select_from(EvaluationSampleSelectionRecord)
                .join(
                    EvaluationPeriodRecord,
                    EvaluationPeriodRecord.id == EvaluationSampleSelectionRecord.period_id,
                )
                .outerjoin(
                    EvaluationOutcomeRecord,
                    (EvaluationOutcomeRecord.run_id == EvaluationSampleSelectionRecord.run_id)
                    & (EvaluationOutcomeRecord.signal_id == EvaluationSampleSelectionRecord.signal_id)
                    & (EvaluationOutcomeRecord.horizon_minutes == EvaluationSampleSelectionRecord.horizon_minutes),
                )
                .where(
                    EvaluationSampleSelectionRecord.run_id == run_id,
                    EvaluationSampleSelectionRecord.horizon_minutes.in_(horizons),
                )
            ).all()

            counts = {
                horizon: {
                    "candidates": [0, 0, 0],
                    "selected": [0, 0, 0],
                    "overlapping": [0, 0, 0],
                    "ineligible": [0, 0, 0],
                    "completed": [0, 0, 0],
                    "not_completed": [0, 0, 0],
                }
                for horizon in horizons
            }
            for horizon, period_index, selected, decision_reason, outcome_status in rows:
                if period_index not in (1, 2, 3):
                    raise ValueError("Sample selection references an invalid evaluation period.")
                bucket = counts[horizon]
                index = period_index - 1
                bucket["candidates"][index] += 1
                if decision_reason == "OVERLAPPING_HORIZON":
                    bucket["overlapping"][index] += 1
                elif decision_reason == "INELIGIBLE_REFERENCE":
                    bucket["ineligible"][index] += 1
                if selected:
                    bucket["selected"][index] += 1
                    if outcome_status == "COMPLETED":
                        bucket["completed"][index] += 1
                    else:
                        bucket["not_completed"][index] += 1

            return tuple(
                HorizonSampleReadiness(
                    horizon_minutes=horizon,
                    candidate_total=sum(counts[horizon]["candidates"]),
                    candidates_by_period=tuple(counts[horizon]["candidates"]),
                    selected_total=sum(counts[horizon]["selected"]),
                    selected_by_period=tuple(counts[horizon]["selected"]),
                    overlapping_by_period=tuple(counts[horizon]["overlapping"]),
                    ineligible_by_period=tuple(counts[horizon]["ineligible"]),
                    selected_completed_total=sum(counts[horizon]["completed"]),
                    selected_completed_by_period=tuple(counts[horizon]["completed"]),
                    selected_not_completed_by_period=tuple(counts[horizon]["not_completed"]),
                    required_total=run.minimum_completed_outcomes_per_horizon,
                    required_per_period=run.minimum_outcomes_per_period,
                    ready=(
                        sum(counts[horizon]["completed"])
                        >= run.minimum_completed_outcomes_per_horizon
                        and all(
                            value >= run.minimum_outcomes_per_period
                            for value in counts[horizon]["completed"]
                        )
                    ),
                    status=(
                        "READY"
                        if (
                            sum(counts[horizon]["completed"])
                            >= run.minimum_completed_outcomes_per_horizon
                            and all(
                                value >= run.minimum_outcomes_per_period
                                for value in counts[horizon]["completed"]
                            )
                        )
                        else "NEEDS_MORE_DATA"
                    ),
                )
                for horizon in horizons
            )

    def persist_outcomes(
        self,
        run: EvaluationRunContract,
        periods: tuple[EvaluationPeriodContract, ...],
        signals: Iterable[EvaluationSignalContract],
        outcomes: Iterable[EvaluationOutcomeContract],
        *,
        evidence_by_outcome_id: dict[str, dict] | None = None,
    ) -> tuple[EvaluationOutcomeRecord, ...]:
        """Persist validated outcomes and optional immutable source evidence."""
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

        evidence_map = evidence_by_outcome_id or {}
        outcome_ids = {str(item.outcome_id) for item in outcome_items}
        if set(evidence_map) - outcome_ids:
            raise ValueError("Source evidence cannot reference an outcome outside this persistence batch.")
        signal_by_id = {str(item.signal_id): item for item in signal_items}
        for outcome in outcome_items:
            outcome_id = str(outcome.outcome_id)
            if outcome.measurement_basis.value == "OHLC_APPROXIMATION":
                if outcome_id not in evidence_map:
                    raise ValueError("Every OHLC outcome requires exactly one M1 source evidence snapshot.")
                _validate_ohlc_evidence_snapshot(
                    evidence_map[outcome_id],
                    run,
                    signal_by_id[str(outcome.signal_id)],
                    outcome,
                )
            elif outcome_id in evidence_map:
                raise ValueError("M1 OHLC evidence cannot be attached to a non-OHLC measurement basis.")

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
            if evidence_by_outcome_id is not None:
                session.add_all(
                    EvaluationOutcomeEvidenceRecord(
                        outcome_id=outcome_id,
                        **{
                            **snapshot,
                            "requested_start_at": datetime.fromisoformat(
                                snapshot["requested_start_at"]
                            ),
                            "requested_end_at": datetime.fromisoformat(
                                snapshot["requested_end_at"]
                            ),
                            "available_at_cutoff": datetime.fromisoformat(
                                snapshot["available_at_cutoff"]
                            ),
                            "latest_available_at": (
                                datetime.fromisoformat(snapshot["latest_available_at"])
                                if snapshot["latest_available_at"] is not None
                                else None
                            ),
                        },
                    )
                    for outcome_id, snapshot in evidence_map.items()
                )
        return tuple(records)

    def calculate_and_persist_ohlc_outcomes(
        self,
        run: EvaluationRunContract,
        periods: tuple[EvaluationPeriodContract, ...],
        signals: Iterable[EvaluationSignalContract],
    ) -> tuple[EvaluationOutcomeRecord, ...]:
        """Calculate and persist every enabled OHLC horizon for every signal.

        The run creation timestamp is the frozen data-availability cutoff.
        The range reader rejects candles received after that cutoff or whose
        source/validation evidence is not trustworthy.
        Filter decisions and sample-selection decisions never suppress
        counterfactual calculations.
        """
        signal_items = tuple(signals)
        validate_evaluation_relationships(run, periods, signal_items, ())
        if not signal_items:
            return ()

        horizons = tuple(run.horizons)
        if not horizons or any(value not in (5, 15, 30, 60) for value in horizons):
            raise ValueError("The evaluation run has no supported outcome horizons.")

        signal_ids = {str(item.signal_id) for item in signal_items}
        with self._database.session() as session:
            stored_run = session.get(EvaluationRunRecord, str(run.run_id))
            if stored_run is None:
                raise ValueError("Evaluation run must be persisted before outcomes.")
            stored_signal_ids = set(
                session.scalars(
                    select(EvaluationSignalRecord.id).where(
                        EvaluationSignalRecord.run_id == str(run.run_id),
                        EvaluationSignalRecord.id.in_(signal_ids),
                    )
                ).all()
            )
            if stored_signal_ids != signal_ids:
                raise ValueError(
                    "Every outcome signal must already be persisted for this run."
                )
            existing = session.execute(
                select(
                    EvaluationOutcomeRecord.signal_id,
                    EvaluationOutcomeRecord.horizon_minutes,
                ).where(
                    EvaluationOutcomeRecord.run_id == str(run.run_id),
                    EvaluationOutcomeRecord.signal_id.in_(signal_ids),
                    EvaluationOutcomeRecord.horizon_minutes.in_(horizons),
                )
            ).all()
            if existing:
                raise ValueError(
                    "Counterfactual outcomes already exist for one or more "
                    "supplied signals and enabled horizons; refusing to overwrite."
                )

        reader = EvaluationCandleSeriesReader(
            EvaluationMarketDataRepository(self._database)
        )
        calculator = EvaluationOHLCOutcomeCalculator()
        maximum_horizon = max(horizons)
        outcomes: list[EvaluationOutcomeContract] = []
        evidence_by_outcome_id: dict[str, dict] = {}

        for signal in sorted(
            signal_items,
            key=lambda item: (item.signal_at, str(item.signal_id)),
        ):
            # Include the last M1 candle needed when the signal falls inside
            # a minute and entry therefore starts at the following minute.
            series_end = signal.signal_at + timedelta(
                minutes=maximum_horizon + 1
            )
            evidence = reader.load_series(
                instrument=signal.instrument,
                timeframe="M1",
                start_at=signal.signal_at,
                end_at=series_end,
                available_at_cutoff=run.created_at,
            )
            evidence_snapshot = _serialize_outcome_source_evidence(evidence)
            for horizon in horizons:
                outcome = calculator.calculate(
                    signal,
                    horizon,
                    evidence.candles,
                    created_at=run.created_at,
                )
                outcomes.append(outcome)
                evidence_by_outcome_id[str(outcome.outcome_id)] = evidence_snapshot

        return self.persist_outcomes(
            run,
            periods,
            signal_items,
            tuple(outcomes),
            evidence_by_outcome_id=evidence_by_outcome_id,
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
