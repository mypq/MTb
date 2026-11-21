from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, func, select

from backend.database import Database
from backend.database_models import (
    DatabaseBase,
    EvaluationPeriodRecord,
    EvaluationRunRecord,
)
from backend.evaluation.contracts import (
    EvaluationSignalContract,
    FilterDecision,
)
from backend.evaluation.service import EvaluationService


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
