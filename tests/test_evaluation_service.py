from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

from backend.database import Database
from backend.database_backup import DatabaseBackupService
from backend.database_migrations import upgrade_database
from backend.database_models import (
    DatabaseBase,
    EvaluationPeriodRecord,
    EvaluationRunRecord,
    EvaluationSignalRecord,
    EvaluationSampleSelectionRecord,
    EvaluationOutcomeRecord,
    EvaluationOutcomeEvidenceRecord,
    MarketCandleRecord,
    MarketDataBatchRecord,
)
from backend.evaluation.contracts import (
    EvaluationSignalContract,
    FilterDecision,
)
from backend.evaluation.service import EvaluationService
from backend.evaluation.ohlc_outcomes import EvaluationOHLCOutcomeCalculator
from backend.market_data.contracts import MarketCandleDataContract


@pytest.fixture
def evaluation_service():
    database = Database("sqlite:///:memory:")
    database.initialize()
    DatabaseBase.metadata.create_all(database.engine)
    try:
        yield EvaluationService(database), database
    finally:
        database.dispose()


def make_signal(run, period, signal_at, *, eligible=True, decision="ALLOWED"):
    return EvaluationSignalContract(
        signal_id=uuid4(),
        run_id=run.run_id,
        period_id=period.period_id,
        instrument=run.instrument,
        signal_at=signal_at,
        input_available_at=signal_at - timedelta(seconds=1),
        direction="BUY",
        reference_eligible=eligible,
        filter_decision=decision,
        filter_reason="TEST_FILTER" if decision == "BLOCKED" else None,
        source_candle_ids=(1,),
        source_batch_ids=(1,),
        created_at=run.created_at,
    )


def test_create_run_persists_run_and_three_equal_periods(evaluation_service):
    service, database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)

    run, periods = service.create_run(
        instrument=" xauusd ",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="a" * 40,
        configuration_snapshot={"baseline": {"name": "last_closed_candle"}},
        created_at=now,
    )

    assert run.instrument == "XAUUSD"
    assert len(periods) == 3
    assert periods[0].start_at == run.data_start_at
    assert periods[-1].end_at == run.data_end_at
    assert periods[0].end_at == periods[1].start_at
    assert periods[1].end_at == periods[2].start_at

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(EvaluationRunRecord)) == 1
        assert session.scalar(select(func.count()).select_from(EvaluationPeriodRecord)) == 3
        stored = session.get(EvaluationRunRecord, str(run.run_id))
        assert stored.configuration_sha256 == run.configuration_sha256
        assert stored.horizons == [5, 15, 30, 60]
        assert stored.status == "CREATED"


def test_non_overlapping_selection_is_independent_and_keeps_blocked_signals(
    evaluation_service,
):
    service, _database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="b" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
    )
    base = periods[0].start_at + timedelta(hours=1)
    signals = (
        make_signal(run, periods[0], base + timedelta(minutes=10)),
        make_signal(
            run, periods[0], base,
            decision=FilterDecision.BLOCKED,
        ),
        make_signal(run, periods[0], base + timedelta(minutes=5)),
        make_signal(run, periods[0], base + timedelta(minutes=15)),
        make_signal(run, periods[0], base + timedelta(minutes=30)),
        make_signal(run, periods[0], base + timedelta(minutes=2), eligible=False),
    )

    selected_5 = service.select_non_overlapping_signals(signals, 5)
    selected_15 = service.select_non_overlapping_signals(signals, 15)

    assert [item.signal_at for item in selected_5] == [
        base, base + timedelta(minutes=5),
        base + timedelta(minutes=10), base + timedelta(minutes=15),
        base + timedelta(minutes=30),
    ]
    assert [item.signal_at for item in selected_15] == [
        base, base + timedelta(minutes=15),
        base + timedelta(minutes=30),
    ]
    assert selected_5[0].filter_decision == FilterDecision.BLOCKED


@pytest.mark.parametrize(
    ("counts", "expected"),
    [
        ((100, 100, 100), "READY"),
        ((150, 75, 75), "READY"),
        ((100, 100, 99), "NEEDS_MORE_DATA"),
        ((150, 75, 74), "NEEDS_MORE_DATA"),
        ((74, 74, 74), "NEEDS_MORE_DATA"),
    ],
)
def test_sample_readiness_requires_total_and_each_period(counts, expected):
    result = EvaluationService.assess_horizon_sample(
        horizon_minutes=5,
        selected_completed_by_period=counts,
    )
    assert result.status == expected
    assert result.ready is (expected == "READY")
    assert result.selected_completed_total == sum(counts)
    assert result.selected_completed_by_period == counts
    assert not hasattr(result, "candidate_total")
    assert not hasattr(result, "selected_total")


def test_sample_readiness_rejects_invalid_counts():
    with pytest.raises(ValueError, match="Exactly three"):
        EvaluationService.assess_horizon_sample(
            horizon_minutes=5,
            selected_completed_by_period=(300, 0),
        )
    with pytest.raises(ValueError, match="non-negative"):
        EvaluationService.assess_horizon_sample(
            horizon_minutes=5,
            selected_completed_by_period=(300, -1, 1),
        )




def test_run_readiness_counts_only_selected_completed_outcomes_by_period(evaluation_service):
    service, database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="g" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
        horizons=(5,),
        minimum_completed_outcomes_per_horizon=3,
        minimum_outcomes_per_period=1,
    )
    first_at = periods[0].start_at + timedelta(hours=1, seconds=30)
    signals = (
        make_signal(run, periods[0], first_at),
        make_signal(run, periods[0], first_at + timedelta(minutes=1)),
        make_signal(run, periods[1], periods[1].start_at + timedelta(hours=1, seconds=30)),
        make_signal(run, periods[2], periods[2].start_at + timedelta(hours=1, seconds=30)),
    )
    service.persist_signals(run, periods, signals)
    ledger = service.record_sample_selections(
        run_id=str(run.run_id), signals=signals, horizons=(5,)
    )
    assert sum(item.selected for item in ledger) == 3
    assert [item.decision_reason for item in sorted(ledger, key=lambda item: item.candidate_order)] == [
        "SELECTED", "OVERLAPPING_HORIZON", "SELECTED", "SELECTED"
    ]

    # Add source candles only for selected signals in periods 1 and 2.
    # The overlapping signal shares the same time range and is deliberately
    # left without separate duplicate market-candle rows; period 3 stays incomplete.
    with database.session() as session:
        for signal in (signals[0], signals[2]):
            first_minute = signal.signal_at.replace(second=0, microsecond=0) + timedelta(minutes=1)
            for offset in range(5):
                event_time = first_minute + timedelta(minutes=offset)
                available_at = event_time + timedelta(minutes=1, seconds=1)
                batch = MarketDataBatchRecord(
                    symbol="XAUUSD", timeframe="M1", source="MT5",
                    received_at=available_at, candle_count=1,
                    created_at=available_at + timedelta(seconds=1),
                    clock_verified=True, clock_account_login=123456,
                    clock_broker_server="Test-Broker",
                    validation_report={"analysis_eligible": True, "freshness_verified": True, "issues": [], "gaps": []},
                )
                session.add(batch)
                session.flush()
                session.add(MarketCandleRecord(
                    batch_id=batch.id, symbol="XAUUSD", timeframe="M1",
                    timestamp=event_time, open_price=2000.0 + offset,
                    high_price=2002.0 + offset, low_price=1999.0 + offset,
                    close_price=2001.0 + offset, tick_volume=10,
                    real_volume=None, volume_source="TICK", is_closed=True,
                    quality="VERIFIED",
                ))

    # Three selected ledger rows exist, but the third selected signal has no
    # source candles. The completed but overlapping second signal is excluded.
    service.calculate_and_persist_ohlc_outcomes(run, periods, signals)
    report = service.get_run_readiness(run_id=str(run.run_id))

    assert len(report) == 1
    readiness = report[0]
    assert readiness.horizon_minutes == 5
    assert readiness.candidate_total == 4
    assert readiness.candidates_by_period == (2, 1, 1)
    assert readiness.selected_total == 3
    assert readiness.selected_by_period == (1, 1, 1)
    assert readiness.overlapping_by_period == (1, 0, 0)
    assert readiness.ineligible_by_period == (0, 0, 0)
    assert readiness.selected_completed_total == 2
    assert readiness.selected_completed_by_period == (1, 1, 0)
    assert readiness.selected_not_completed_by_period == (0, 0, 1)
    assert readiness.required_total == 3
    assert readiness.required_per_period == 1
    assert readiness.ready is False
    assert readiness.status == "NEEDS_MORE_DATA"


def test_run_readiness_rejects_unknown_run(evaluation_service):
    service, _database = evaluation_service
    with pytest.raises(ValueError, match="does not exist"):
        service.get_run_readiness(run_id="missing-run")

def test_sample_selection_ledger_persists_every_candidate_per_horizon(evaluation_service):
    service, database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="c" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
    )
    base = periods[0].start_at + timedelta(hours=2)
    signals = (
        make_signal(run, periods[0], base + timedelta(minutes=10)),
        make_signal(run, periods[0], base, decision=FilterDecision.BLOCKED),
        make_signal(run, periods[0], base + timedelta(minutes=2), eligible=False),
        make_signal(run, periods[0], base + timedelta(minutes=5)),
        make_signal(run, periods[0], base + timedelta(minutes=15)),
    )

    service.persist_signals(run, periods, signals)
    ledger = service.record_sample_selections(
        run_id=str(run.run_id),
        signals=signals,
        horizons=(5, 15),
    )

    assert len(ledger) == 10
    five = service.get_sample_selections(run_id=str(run.run_id), horizon_minutes=5)
    fifteen = service.get_sample_selections(run_id=str(run.run_id), horizon_minutes=15)
    assert len(five) == len(fifteen) == 5
    assert [item.decision_reason for item in five] == [
        "SELECTED",
        "INELIGIBLE_REFERENCE",
        "SELECTED",
        "SELECTED",
        "SELECTED",
    ]
    assert [item.decision_reason for item in fifteen] == [
        "SELECTED",
        "INELIGIBLE_REFERENCE",
        "OVERLAPPING_HORIZON",
        "OVERLAPPING_HORIZON",
        "SELECTED",
    ]
    # The blocked signal remains in the reference sample; filter state is separate.
    assert five[0].selected is True
    assert five[0].signal_id == str(next(
        signal.signal_id for signal in signals
        if signal.filter_decision == FilterDecision.BLOCKED
    ))

    readiness = service.get_run_readiness(run_id=str(run.run_id))
    by_horizon = {item.horizon_minutes: item for item in readiness}
    five_report = by_horizon[5]
    assert five_report.candidate_total == 5
    assert five_report.candidates_by_period == (5, 0, 0)
    assert five_report.selected_total == 4
    assert five_report.selected_by_period == (4, 0, 0)
    assert five_report.overlapping_by_period == (0, 0, 0)
    assert five_report.ineligible_by_period == (1, 0, 0)
    assert five_report.selected_completed_total == 0
    assert five_report.selected_not_completed_by_period == (4, 0, 0)

    fifteen_report = by_horizon[15]
    assert fifteen_report.candidate_total == 5
    assert fifteen_report.candidates_by_period == (5, 0, 0)
    assert fifteen_report.selected_total == 2
    assert fifteen_report.selected_by_period == (2, 0, 0)
    assert fifteen_report.overlapping_by_period == (2, 0, 0)
    assert fifteen_report.ineligible_by_period == (1, 0, 0)
    assert fifteen_report.selected_completed_total == 0
    assert fifteen_report.selected_not_completed_by_period == (2, 0, 0)

    with database.session() as session:
        assert session.scalar(
            select(func.count()).select_from(EvaluationSampleSelectionRecord)
        ) == 10

    with pytest.raises(ValueError, match="already exists"):
        service.record_sample_selections(
            run_id=str(run.run_id), signals=signals, horizons=(5,)
        )


def test_sample_selection_requires_persisted_signals(evaluation_service):
    service, _database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="d" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
    )
    signal = make_signal(
        run, periods[0], periods[0].start_at + timedelta(hours=1)
    )
    with pytest.raises(ValueError, match="every persisted signal"):
        service.record_sample_selections(
            run_id=str(run.run_id),
            signals=(signal,),
            horizons=(5,),
        )



def test_sample_selection_rejects_partial_candidate_batch(evaluation_service):
    service, _database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="f" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
    )
    base = periods[0].start_at + timedelta(hours=1)
    signals = (
        make_signal(run, periods[0], base),
        make_signal(run, periods[0], base + timedelta(minutes=10)),
    )
    service.persist_signals(run, periods, signals)

    with pytest.raises(ValueError, match="every persisted signal"):
        service.record_sample_selections(
            run_id=str(run.run_id), signals=(signals[0],), horizons=(5,)
        )


def test_ohlc_outcome_cannot_be_persisted_without_source_lineage(
    evaluation_service,
):
    service, database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="e" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
    )
    signal_at = periods[0].start_at + timedelta(hours=1, seconds=30)
    signal = make_signal(run, periods[0], signal_at, decision="BLOCKED")
    service.persist_signals(run, periods, (signal,))

    candles = []
    for minute in range(1, 7):
        event_time = signal_at.replace(second=0, microsecond=0) + timedelta(minutes=minute)
        available_at = event_time + timedelta(minutes=1, seconds=1)
        candles.append(MarketCandleDataContract(
            candle_id=minute,
            batch_id=minute,
            instrument="XAUUSD",
            timeframe="M1",
            event_time=event_time,
            available_at=available_at,
            recorded_at=available_at + timedelta(seconds=1),
            open=2000.0 + minute,
            high=2003.0 + minute,
            low=1999.0 + minute,
            close=2000.5 + minute,
            tick_volume=10,
            real_volume=None,
            volume_source="TICK",
            candle_quality="VERIFIED",
            is_closed=True,
            source="MT5",
            clock_verified=True,
            analysis_eligible=True,
            validation_report={"analysis_eligible": True},
        ))

    outcome = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, tuple(candles))
    with pytest.raises(ValueError, match="Every OHLC outcome requires exactly one M1 source evidence snapshot"):
        service.persist_outcomes(run, periods, (signal,), (outcome,))

    with database.session() as session:
        assert session.get(EvaluationOutcomeRecord, str(outcome.outcome_id)) is None


def test_unavailable_outcome_validates_any_attached_m1_evidence(evaluation_service):
    service, database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="h" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
        horizons=(5,),
    )
    signal_at = periods[0].start_at + timedelta(hours=1, seconds=30)
    signal = make_signal(run, periods[0], signal_at)
    service.persist_signals(run, periods, (signal,))
    outcome = EvaluationOHLCOutcomeCalculator().calculate(
        signal, 5, (), created_at=run.created_at
    )
    assert outcome.measurement_basis.value == "UNAVAILABLE"

    snapshot = {
        "instrument": "EURUSD",
        "timeframe": "M1",
        "requested_start_at": signal_at.isoformat(),
        "requested_end_at": (signal_at + timedelta(minutes=6)).isoformat(),
        "available_at_cutoff": run.created_at.isoformat(),
        "latest_available_at": None,
        "source_candles": [],
        "observed_gaps": [],
        "duplicate_event_times": [],
        "is_contiguous": False,
        "evidence_schema_version": 1,
    }
    with pytest.raises(ValueError, match="instrument"):
        service.persist_outcomes(
            run,
            periods,
            (signal,),
            (outcome,),
            evidence_by_outcome_id={str(outcome.outcome_id): snapshot},
        )

    snapshot["instrument"] = "XAUUSD"
    snapshot["requested_end_at"] = (signal_at + timedelta(minutes=5)).isoformat()
    with pytest.raises(ValueError, match="required outcome horizon"):
        service.persist_outcomes(
            run,
            periods,
            (signal,),
            (outcome,),
            evidence_by_outcome_id={str(outcome.outcome_id): snapshot},
        )

    with database.session() as session:
        assert session.get(EvaluationOutcomeRecord, str(outcome.outcome_id)) is None
        assert session.get(
            EvaluationOutcomeEvidenceRecord, str(outcome.outcome_id)
        ) is None


def test_service_calculates_and_persists_every_enabled_horizon_for_all_signals(
    evaluation_service,
):
    service, database = evaluation_service
    now = datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="f" * 40,
        configuration_snapshot={"baseline": "fixed"},
        created_at=now,
    )
    first_at = periods[0].start_at + timedelta(hours=2, seconds=30)
    signals = (
        make_signal(run, periods[0], first_at, decision="BLOCKED"),
        make_signal(
            run,
            periods[0],
            first_at + timedelta(hours=2),
            eligible=False,
        ),
    )
    service.persist_signals(run, periods, signals)

    with database.session() as session:
        candle_id = 1
        for signal_index, signal in enumerate(signals):
            first_minute = signal.signal_at.replace(second=0, microsecond=0) + timedelta(minutes=1)
            for offset in range(61):
                if signal_index == 1 and offset == 3:
                    # Preserve a real M1 gap: every horizon for this signal
                    # must remain incomplete rather than interpolate a price.
                    continue
                event_time = first_minute + timedelta(minutes=offset)
                available_at = event_time + timedelta(minutes=1, seconds=1)
                batch = MarketDataBatchRecord(
                    symbol="XAUUSD",
                    timeframe="M1",
                    source="MT5",
                    received_at=available_at,
                    candle_count=1,
                    created_at=available_at + timedelta(seconds=1),
                    clock_verified=True,
                    clock_account_login=123456,
                    clock_broker_server="Test-Broker",
                    validation_report={
                        "analysis_eligible": True,
                        "freshness_verified": True,
                        "issues": [],
                        "gaps": [],
                    },
                )
                session.add(batch)
                session.flush()
                session.add(
                    MarketCandleRecord(
                        batch_id=batch.id,
                        symbol="XAUUSD",
                        timeframe="M1",
                        timestamp=event_time,
                        open_price=2000.0 + offset,
                        high_price=2002.0 + offset,
                        low_price=1999.0 + offset,
                        close_price=2001.0 + offset,
                        tick_volume=10,
                        real_volume=None,
                        volume_source="TICK",
                        is_closed=True,
                        quality="VERIFIED",
                    )
                )
                candle_id += 1

    records = service.calculate_and_persist_ohlc_outcomes(
        run,
        periods,
        signals,
    )

    assert len(records) == 8
    assert {
        (record.signal_id, record.horizon_minutes)
        for record in records
    } == {
        (str(signal.signal_id), horizon)
        for signal in signals
        for horizon in (5, 15, 30, 60)
    }
    assert all(
        record.status == "COMPLETED"
        for record in records
        if record.signal_id == str(signals[0].signal_id)
    )
    assert all(
        record.status == "NEEDS_MORE_DATA"
        for record in records
        if record.signal_id == str(signals[1].signal_id)
    )
    assert all(
        record.measurement_basis == (
            "OHLC_APPROXIMATION"
            if record.signal_id == str(signals[0].signal_id)
            else "UNAVAILABLE"
        )
        for record in records
    )
    assert all(record.net_pnl is None for record in records)
    # Filter-blocked and reference-ineligible signals both retain outcomes.
    assert {
        record.signal_id for record in records
    } == {str(signal.signal_id) for signal in signals}

    with database.session() as session:
        evidence_rows = session.scalars(
            select(EvaluationOutcomeEvidenceRecord).order_by(
                EvaluationOutcomeEvidenceRecord.outcome_id
            )
        ).all()
        assert len(evidence_rows) == 8
        by_outcome = {item.outcome_id: item for item in evidence_rows}
        for record in records:
            source = by_outcome[record.id]
            assert source.instrument == "XAUUSD"
            assert source.timeframe == "M1"
            assert source.requested_start_at.replace(tzinfo=timezone.utc) == signals[
                0 if record.signal_id == str(signals[0].signal_id) else 1
            ].signal_at
            assert source.available_at_cutoff.replace(tzinfo=timezone.utc) == run.created_at
            assert len(source.source_candles) in (60, 61)
            assert source.evidence_schema_version == 1
        complete_evidence = by_outcome[
            next(record.id for record in records if record.signal_id == str(signals[0].signal_id))
        ]
        assert complete_evidence.source_candles[0]["candle_id"] > 0
        assert complete_evidence.source_candles[0]["batch_id"] > 0
        assert complete_evidence.source_candles[0]["open"] > 0
        gap_evidence = by_outcome[
            next(record.id for record in records if record.signal_id == str(signals[1].signal_id))
        ]
        assert len(gap_evidence.observed_gaps) == 1
        assert gap_evidence.observed_gaps[0]["missing_intervals"] is None
        assert gap_evidence.is_contiguous is False
        assert len(complete_evidence.evidence_sha256) == 64
        assert service.get_outcome_evidence(complete_evidence.outcome_id).evidence_sha256 == (
            complete_evidence.evidence_sha256
        )

        # Simulate out-of-band tampering; the digest must detect it.
        with database.engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE evaluation_outcome_evidence "
                "SET is_contiguous = 0 WHERE outcome_id = ?",
                (complete_evidence.outcome_id,),
            )
        with pytest.raises(ValueError, match="integrity check failed"):
            service.get_outcome_evidence(complete_evidence.outcome_id)

    with pytest.raises(ValueError, match="already exist"):
        service.calculate_and_persist_ohlc_outcomes(
            run,
            periods,
            signals,
        )


def test_delete_run_cascades_only_evaluation_records(evaluation_service):
    service, database = evaluation_service
    now = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
    run, periods = service.create_run(
        instrument="XAUUSD",
        data_start_at=now - timedelta(days=30),
        data_end_at=now,
        code_revision="d" * 40,
        configuration_snapshot={"baseline": "delete-cascade"},
        created_at=now,
        horizons=(5,),
    )
    signal = make_signal(
        run,
        periods[0],
        periods[0].start_at + timedelta(hours=1),
    )
    service.persist_signals(run, periods, (signal,))
    service.record_sample_selections(
        run_id=str(run.run_id),
        signals=(signal,),
        horizons=(5,),
    )

    # An unrelated source-market record must survive deletion of the run.
    with database.session() as session:
        session.add(
            MarketDataBatchRecord(
                id=9001,
                symbol="EURUSD",
                timeframe="M1",
                source="TEST",
                received_at=now,
                candle_count=1,
                created_at=now,
            )
        )
        session.add(
            MarketCandleRecord(
                id=9001,
                batch_id=9001,
                symbol="EURUSD",
                timeframe="M1",
                timestamp=now - timedelta(minutes=1),
                open_price=1.1,
                high_price=1.2,
                low_price=1.0,
                close_price=1.15,
                tick_volume=10,
                real_volume=None,
                volume_source="TICK",
                is_closed=True,
                quality="VERIFIED",
            )
        )

    outcomes = service.calculate_and_persist_ohlc_outcomes(
        run,
        periods,
        (signal,),
    )
    assert len(outcomes) == 1
    outcome_id = str(outcomes[0].id)

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(EvaluationRunRecord)) == 1
        assert session.scalar(select(func.count()).select_from(EvaluationPeriodRecord)) == 3
        assert session.scalar(select(func.count()).select_from(EvaluationSignalRecord)) == 1
        assert session.scalar(select(func.count()).select_from(EvaluationOutcomeRecord)) == 1
        assert session.scalar(select(func.count()).select_from(EvaluationSampleSelectionRecord)) == 1
        assert session.scalar(select(func.count()).select_from(EvaluationOutcomeEvidenceRecord)) == 1

    assert service.delete_run(str(run.run_id)) is True

    with database.session() as session:
        assert session.scalar(select(func.count()).select_from(EvaluationRunRecord)) == 0
        assert session.scalar(select(func.count()).select_from(EvaluationPeriodRecord)) == 0
        assert session.scalar(select(func.count()).select_from(EvaluationSignalRecord)) == 0
        assert session.scalar(select(func.count()).select_from(EvaluationOutcomeRecord)) == 0
        assert session.scalar(select(func.count()).select_from(EvaluationSampleSelectionRecord)) == 0
        assert session.scalar(select(func.count()).select_from(EvaluationOutcomeEvidenceRecord)) == 0
        assert session.scalar(select(func.count()).select_from(MarketDataBatchRecord)) == 1
        assert session.scalar(select(func.count()).select_from(MarketCandleRecord)) == 1
        batch = session.get(MarketDataBatchRecord, 9001)
        assert batch is not None
        assert batch.symbol == "EURUSD"

    assert service.delete_run(str(run.run_id)) is False
    with pytest.raises(ValueError, match="does not exist"):
        service.get_run_readiness(run_id=str(run.run_id))
    with pytest.raises(ValueError, match="does not exist"):
        service.get_outcome_evidence(outcome_id)


def test_backup_restore_preserves_evaluation_evidence_and_market_data(tmp_path):
    database_path = tmp_path / "evaluation-backup.db"
    database = Database(f"sqlite:///{database_path.as_posix()}")
    database.initialize()
    upgrade_database(str(database.engine.url))
    service = EvaluationService(database)
    now = datetime(2026, 11, 22, 12, 0, tzinfo=timezone.utc)
    try:
        run, periods = service.create_run(
            instrument="XAUUSD",
            data_start_at=now - timedelta(days=30),
            data_end_at=now,
            code_revision="b" * 40,
            configuration_snapshot={"backup_roundtrip": True},
            created_at=now,
            horizons=(5,),
        )
        signal_at = periods[0].start_at + timedelta(hours=1, seconds=30)
        signal = make_signal(run, periods[0], signal_at)
        service.persist_signals(run, periods, (signal,))
        outcome = EvaluationOHLCOutcomeCalculator().calculate(
            signal, 5, (), created_at=run.created_at
        )
        evidence = {
            "instrument": "XAUUSD",
            "timeframe": "M1",
            "requested_start_at": signal_at.isoformat(),
            "requested_end_at": (signal_at + timedelta(minutes=6)).isoformat(),
            "available_at_cutoff": run.created_at.isoformat(),
            "latest_available_at": None,
            "source_candles": [],
            "observed_gaps": [],
            "duplicate_event_times": [],
            "is_contiguous": False,
            "evidence_schema_version": 1,
        }
        service.persist_outcomes(
            run, periods, (signal,), (outcome,),
            evidence_by_outcome_id={str(outcome.outcome_id): evidence},
        )
        original_digest = service.get_outcome_evidence(
            str(outcome.outcome_id)
        ).evidence_sha256

        with database.session() as session:
            batch = MarketDataBatchRecord(
                symbol="XAUUSD", timeframe="M1", source="MT5",
                received_at=now, candle_count=1, created_at=now,
                clock_verified=True, clock_broker_server="Backup-Test-Broker",
                validation_report={"analysis_eligible": True, "issues": []},
            )
            session.add(batch)
            session.flush()
            session.add(MarketCandleRecord(
                batch_id=batch.id, symbol="XAUUSD", timeframe="M1",
                timestamp=now - timedelta(minutes=1),
                open_price=2000.0, high_price=2002.0, low_price=1999.0,
                close_price=2001.0, tick_volume=10, real_volume=None,
                volume_source="TICK", is_closed=True, quality="VERIFIED",
            ))
            batch_id = batch.id

        backup_service = DatabaseBackupService(database, tmp_path / "backups")
        backup_path = backup_service.create_backup()
        assert service.delete_run(str(run.run_id)) is True
        with database.engine.begin() as connection:
            connection.exec_driver_sql(
                "UPDATE market_candles SET close_price = 1998.0 WHERE batch_id = ?",
                (batch_id,),
            )

        backup_service.restore_backup(backup_path)
        with database.session() as session:
            assert session.get(EvaluationRunRecord, str(run.run_id)) is not None
            assert session.get(EvaluationOutcomeRecord, str(outcome.outcome_id)) is not None
            assert session.get(EvaluationOutcomeEvidenceRecord, str(outcome.outcome_id)) is not None
            candle = session.scalar(select(MarketCandleRecord).where(
                MarketCandleRecord.batch_id == batch_id
            ))
            assert candle is not None and candle.close_price == 2001.0
            batch_record = session.get(MarketDataBatchRecord, batch_id)
            assert batch_record.validation_report == {
                "analysis_eligible": True, "issues": []
            }

        restored = service.get_outcome_evidence(str(outcome.outcome_id))
        assert restored.evidence_sha256 == original_digest
        with pytest.raises(IntegrityError, match="immutable"):
            with database.engine.begin() as connection:
                connection.exec_driver_sql(
                    "UPDATE evaluation_outcome_evidence SET is_contiguous = 1 "
                    "WHERE outcome_id = ?", (str(outcome.outcome_id),)
                )
        assert service.get_outcome_evidence(
            str(outcome.outcome_id)
        ).evidence_sha256 == original_digest
    finally:
        database.dispose()
