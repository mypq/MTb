from datetime import datetime, timedelta, timezone
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, event, insert, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.database_models import (
    DatabaseBase,
    EvaluationOutcomeRecord,
    EvaluationPeriodRecord,
    EvaluationRunRecord,
    EvaluationSignalRecord,
    MarketCandleRecord,
    MarketDataBatchRecord,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(connection, _record):
        cursor = connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    DatabaseBase.metadata.create_all(engine)
    with Session(engine) as db_session:
        yield db_session
    engine.dispose()


def make_batch():
    now = datetime.now(timezone.utc)
    return MarketDataBatchRecord(
        symbol="XAUUSD", timeframe="M5", source="TEST",
        received_at=now, candle_count=1, created_at=now,
    )


def make_candle(batch):
    return MarketCandleRecord(
        batch=batch, symbol="XAUUSD", timeframe="M5",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        open_price=2000.0, high_price=2010.0, low_price=1990.0,
        close_price=2005.0, tick_volume=100, real_volume=None,
        volume_source="TICK", is_closed=True, quality="VERIFIED",
    )


def make_run():
    now = datetime(2026, 11, 21, 10, 0, tzinfo=timezone.utc)
    return EvaluationRunRecord(
        id=str(uuid4()), contract_version="1.0", instrument="XAUUSD",
        created_at=now, data_start_at=now - timedelta(days=30), data_end_at=now,
        code_revision="abc123", configuration_sha256="a" * 64,
        configuration_snapshot={"baseline": {"name": "reference", "weights": [1, 2]}},
        sampling_rule="FIRST_ELIGIBLE_NON_OVERLAPPING_V1",
        period_partition="THREE_EQUAL_DURATION_CHRONOLOGICAL",
        horizons=[5, 15, 30, 60], minimum_completed_outcomes_per_horizon=300,
        minimum_outcomes_per_period=75, status="CREATED",
    )


def make_period(run, index=1):
    duration = run.data_end_at - run.data_start_at
    boundaries = (
        run.data_start_at,
        run.data_start_at + duration / 3,
        run.data_start_at + duration * 2 / 3,
        run.data_end_at,
    )
    return EvaluationPeriodRecord(
        id=str(uuid4()), run=run, contract_version="1.0", period_index=index,
        start_at=boundaries[index - 1], end_at=boundaries[index],
        created_at=run.created_at,
        partition_method="THREE_EQUAL_DURATION_CHRONOLOGICAL",
    )


def make_signal(run, period):
    signal_at = period.start_at + timedelta(seconds=1)
    return EvaluationSignalRecord(
        id=str(uuid4()), run=run, period=period, contract_version="1.0",
        instrument=run.instrument, signal_at=signal_at,
        input_available_at=signal_at - timedelta(seconds=1), direction="BUY",
        reference_eligible=True, filter_decision="BLOCKED",
        filter_reason="VOLATILITY_FILTER", source_candle_ids=[1, 2],
        source_batch_ids=[1], input_snapshot={"features": {"values": [1, 2]}},
        created_at=run.created_at,
    )


def make_outcome(signal, horizon=5):
    return EvaluationOutcomeRecord(
        id=str(uuid4()), signal=signal, contract_version="1.0",
        horizon_minutes=horizon, outcome_type="COUNTERFACTUAL",
        direction=signal.direction, status="COMPLETED",
        measurement_basis="BID_ASK_QUOTES",
        entry_at=signal.signal_at, exit_at=signal.signal_at + timedelta(minutes=horizon),
        entry_price=Decimal("2000.200000000000000000"),
        exit_price=Decimal("2001.000000000000000000"),
        entry_price_side="ASK", exit_price_side="BID",
        gross_pnl=Decimal("0.800000000000000000"),
        spread_accounting="EMBEDDED_IN_BID_ASK_PRICES",
        spread_cost=None, slippage_cost=None, slippage_knowledge="UNKNOWN",
        commission_cost=None, commission_knowledge="UNKNOWN",
        swap_adjustment=None, swap_knowledge="UNKNOWN", costs_complete=False,
        net_pnl=None, created_at=signal.created_at,
    )


def test_database_models_create_expected_tables(session):
    assert set(DatabaseBase.metadata.tables) == {
        "market_data_batches", "market_candles",
        "evaluation_runs", "evaluation_periods",
        "evaluation_signals", "evaluation_outcomes",
        "evaluation_outcome_evidence", "evaluation_sample_selections",
    }


def test_batch_and_candle_relationship_persist(session):
    batch = make_batch()
    candle = make_candle(batch)
    session.add(batch)
    session.commit()
    assert batch.id is not None
    assert candle.id is not None
    assert len(batch.candles) == 1
    assert batch.candles[0].symbol == "XAUUSD"


def test_duplicate_candle_identity_is_rejected(session):
    batch = make_batch()
    session.add(batch)
    session.flush()
    session.add(make_candle(batch))
    session.commit()

    duplicate_batch = make_batch()
    session.add(duplicate_batch)
    session.flush()
    session.add(make_candle(duplicate_batch))
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_invalid_ohlc_relationship_is_rejected_by_database(session):
    batch = make_batch()
    session.add(batch)
    session.flush()
    invalid_candle = make_candle(batch)
    invalid_candle.high_price = 1995.0
    session.add(invalid_candle)
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_negative_volume_is_rejected_by_database(session):
    batch = make_batch()
    session.add(batch)
    session.flush()
    invalid_candle = make_candle(batch)
    invalid_candle.tick_volume = -1
    session.add(invalid_candle)
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_evaluation_records_persist_with_exact_decimal_values(session):
    run = make_run()
    period = make_period(run)
    signal = make_signal(run, period)
    outcome = make_outcome(signal)
    session.add(run)
    session.add(outcome)
    session.commit()

    loaded = session.scalar(select(EvaluationOutcomeRecord))
    assert loaded is not None
    assert loaded.entry_price == Decimal("2000.200000000000000000")
    assert loaded.gross_pnl == Decimal("0.800000000000000000")
    assert loaded.signal.period.run_id == run.id


def test_evaluation_period_index_is_unique_per_run(session):
    run = make_run()
    session.add(run)
    session.flush()
    session.add_all([make_period(run, 1), make_period(run, 1)])
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_signal_cannot_reference_period_from_another_run(session):
    run_a, run_b = make_run(), make_run()
    period_b = make_period(run_b)
    session.add_all([run_a, run_b])
    session.flush()
    invalid_signal = make_signal(run_a, period_b)
    session.add(invalid_signal)
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_outcome_cannot_mismatch_signal_run_or_period(session):
    run_a, run_b = make_run(), make_run()
    period_a, period_b = make_period(run_a), make_period(run_b)
    signal = make_signal(run_a, period_a)
    session.add_all([run_a, run_b])
    session.flush()
    session.add(signal)
    session.flush()
    invalid = make_outcome(signal)
    values = {
        column.name: getattr(invalid, column.name)
        for column in EvaluationOutcomeRecord.__table__.columns
    }
    values.update(
        run_id=run_b.id,
        signal_id=signal.id,
        period_id=period_b.id,
    )
    with pytest.raises(IntegrityError):
        # This Core insert intentionally bypasses ORM relationship synchronization.
        # Suppress autoflush so the transient ORM object is not flushed indirectly
        # through signal.outcomes before the database-level composite FK is tested.
        with session.no_autoflush:
            session.execute(insert(EvaluationOutcomeRecord).values(**values))
    session.rollback()


def test_one_outcome_per_signal_and_horizon(session):
    run = make_run()
    period = make_period(run)
    signal = make_signal(run, period)
    session.add(run)
    session.flush()
    session.add(signal)
    session.flush()
    session.add_all([make_outcome(signal, 15), make_outcome(signal, 15)])
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


@pytest.mark.parametrize(
    "overrides",
    [
        {"outcome_type": "TRADE"},
        {"measurement_basis": "TICK_DATA"},
        {"entry_price_side": "UNKNOWN"},
        {"spread_accounting": "SEPARATE_COST"},
        {"slippage_knowledge": "ESTIMATED"},
        {"net_pnl": Decimal("0.5")},
        {"slippage_knowledge": "MEASURED"},
        {"measurement_basis": "OHLC_APPROXIMATION"},
        {"measurement_basis": "UNAVAILABLE"},
    ],
)
def test_evaluation_outcome_database_constraints_reject_invalid_semantics(session, overrides):
    run = make_run()
    period = make_period(run)
    signal = make_signal(run, period)
    session.add(run)
    session.flush()
    session.add(signal)
    session.flush()

    outcome = make_outcome(signal)
    values = {
        column.name: getattr(outcome, column.name)
        for column in EvaluationOutcomeRecord.__table__.columns
    }
    values.update(run_id=run.id, signal_id=signal.id, period_id=period.id)
    values.update(overrides)

    with pytest.raises(IntegrityError):
        with session.no_autoflush:
            session.execute(insert(EvaluationOutcomeRecord).values(**values))
    session.rollback()


@pytest.mark.parametrize(
    "overrides",
    [
        {"direction": "SIDEWAYS"},
        {"input_available_at": datetime(2026, 11, 21, 10, 0, 2, tzinfo=timezone.utc)},
        {"filter_reason": None},
    ],
)
def test_evaluation_signal_database_constraints_reject_invalid_semantics(session, overrides):
    run = make_run()
    period = make_period(run)
    signal = make_signal(run, period)
    for field, value in overrides.items():
        setattr(signal, field, value)
    session.add(run)
    session.add(signal)

    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_numeric_fields_use_decimal_storage_type():
    column = EvaluationOutcomeRecord.__table__.c.entry_price
    assert column.type.asdecimal is True
    assert column.type.precision == 38
    assert column.type.scale == 18


def test_run_delete_cascades_to_all_evaluation_records(session):
    run = make_run()
    period = make_period(run)
    signal = make_signal(run, period)
    outcome = make_outcome(signal)
    session.add(run)
    session.add(outcome)
    session.commit()
    run_id = run.id

    session.delete(run)
    session.commit()

    assert session.scalar(select(EvaluationRunRecord).where(EvaluationRunRecord.id == run_id)) is None
    assert session.scalar(select(EvaluationPeriodRecord)) is None
    assert session.scalar(select(EvaluationSignalRecord)) is None
    assert session.scalar(select(EvaluationOutcomeRecord)) is None


def test_signal_delete_cascades_to_its_outcomes(session):
    run = make_run()
    period = make_period(run)
    signal = make_signal(run, period)
    outcome = make_outcome(signal)
    session.add(run)
    session.add(outcome)
    session.commit()

    session.delete(signal)
    session.commit()
    assert session.scalar(select(EvaluationOutcomeRecord)) is None
