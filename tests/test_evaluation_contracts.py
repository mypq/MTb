from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from backend.evaluation.contracts import (
    CostKnowledge,
    EvaluationDirection,
    EvaluationOutcomeContract,
    EvaluationOutcomeStatus,
    EvaluationPeriodContract,
    EvaluationRunContract,
    EvaluationSignalContract,
    ExecutionPriceSide,
    FilterDecision,
    OutcomeMeasurementBasis,
    SpreadAccounting,
    configuration_snapshot_sha256,
    validate_evaluation_period_set,
    validate_evaluation_relationships,
)


UTC = timezone.utc
NOW = datetime(2026, 11, 21, 10, 0, tzinfo=UTC)
SNAPSHOT = {"baseline": "last_closed_candle_direction"}


def make_run(**overrides):
    values = {
        "run_id": uuid4(),
        "instrument": " xauusd ",
        "created_at": NOW,
        "data_start_at": NOW - timedelta(days=30),
        "data_end_at": NOW,
        "code_revision": "abc123",
        "configuration_snapshot": SNAPSHOT,
        "configuration_sha256": configuration_snapshot_sha256(SNAPSHOT),
    }
    values.update(overrides)
    return EvaluationRunContract(**values)


def make_signal(run_id, period_id, **overrides):
    values = {
        "signal_id": uuid4(),
        "run_id": run_id,
        "period_id": period_id,
        "instrument": "XAUUSD",
        "signal_at": NOW,
        "input_available_at": NOW - timedelta(seconds=1),
        "direction": EvaluationDirection.BUY,
        "reference_eligible": True,
        "filter_decision": FilterDecision.BLOCKED,
        "filter_reason": "VOLATILITY_FILTER",
        "source_candle_ids": (1, 2),
        "source_batch_ids": (1,),
        "created_at": NOW,
    }
    values.update(overrides)
    return EvaluationSignalContract(**values)


def make_periods(run):
    duration = run.data_end_at - run.data_start_at
    boundaries = (
        run.data_start_at,
        run.data_start_at + duration / 3,
        run.data_start_at + duration * 2 / 3,
        run.data_end_at,
    )
    return [
        EvaluationPeriodContract(
            period_id=uuid4(),
            run_id=run.run_id,
            period_index=index,
            start_at=boundaries[index - 1],
            end_at=boundaries[index],
            created_at=NOW,
        )
        for index in (1, 2, 3)
    ]


def make_outcome(signal, period_id, **overrides):
    values = {
        "outcome_id": uuid4(),
        "run_id": signal.run_id,
        "signal_id": signal.signal_id,
        "period_id": period_id,
        "horizon_minutes": 5,
        "direction": signal.direction,
        "status": EvaluationOutcomeStatus.COMPLETED,
        "measurement_basis": OutcomeMeasurementBasis.BID_ASK_QUOTES,
        "entry_at": NOW,
        "exit_at": NOW + timedelta(minutes=5),
        "entry_price": 2000.2,
        "exit_price": 2001.0,
        "entry_price_side": ExecutionPriceSide.ASK,
        "exit_price_side": ExecutionPriceSide.BID,
        "gross_pnl": 0.8,
        "spread_accounting": SpreadAccounting.EMBEDDED_IN_BID_ASK_PRICES,
        "created_at": NOW,
    }
    values.update(overrides)
    return EvaluationOutcomeContract(**values)


def test_run_contract_freezes_horizons_and_sample_policy():
    run = make_run()

    assert run.instrument == "XAUUSD"
    assert run.horizons == (5, 15, 30, 60)
    assert run.minimum_completed_outcomes_per_horizon == 300
    assert run.minimum_outcomes_per_period == 75
    with pytest.raises(ValidationError):
        run.instrument = "EURUSD"


def test_run_contract_rejects_invalid_range_or_duplicate_horizons():
    with pytest.raises(ValidationError):
        make_run(data_end_at=NOW - timedelta(days=31))
    with pytest.raises(ValidationError):
        make_run(horizons=(5, 5))


def test_run_snapshot_is_deeply_immutable_and_sha_verified():
    snapshot = {"baseline": {"name": "reference", "weights": [1, 2]}}
    run = make_run(
        configuration_snapshot=snapshot,
        configuration_sha256=configuration_snapshot_sha256(snapshot),
    )

    with pytest.raises(TypeError):
        run.configuration_snapshot["baseline"]["name"] = "changed"
    with pytest.raises(AttributeError):
        run.configuration_snapshot["baseline"]["weights"].append(3)
    with pytest.raises(ValidationError):
        make_run(configuration_sha256="a" * 64)


def test_period_contract_requires_positive_utc_range():
    period = EvaluationPeriodContract(
        period_id=uuid4(),
        run_id=uuid4(),
        period_index=1,
        start_at=NOW - timedelta(days=10),
        end_at=NOW - timedelta(days=5),
        created_at=NOW,
    )
    assert period.start_at < period.end_at

    with pytest.raises(ValidationError):
        EvaluationPeriodContract(
            period_id=uuid4(), run_id=uuid4(), period_index=1,
            start_at=NOW, end_at=NOW, created_at=NOW,
        )


def test_period_set_requires_three_equal_contiguous_full_range_periods():
    run = make_run()
    periods = make_periods(run)

    assert validate_evaluation_period_set(run, periods) == tuple(periods)

    with pytest.raises(ValueError):
        validate_evaluation_period_set(run, periods[:2])

    broken = list(periods)
    broken[1] = broken[1].model_copy(update={
        "start_at": broken[1].start_at + timedelta(seconds=1)
    })
    with pytest.raises(ValueError):
        validate_evaluation_period_set(run, broken)


def test_signal_contract_preserves_blocked_reference_signals():
    run = make_run()
    signal = make_signal(run.run_id, uuid4())

    assert signal.reference_eligible is True
    assert signal.filter_decision == FilterDecision.BLOCKED
    assert signal.filter_reason == "VOLATILITY_FILTER"


def test_signal_contract_rejects_lookahead_and_block_without_reason():
    run = make_run()
    period_id = uuid4()
    with pytest.raises(ValidationError):
        make_signal(run.run_id, period_id, input_available_at=NOW + timedelta(seconds=1))
    with pytest.raises(ValidationError):
        make_signal(run.run_id, period_id, filter_reason=None)


def test_signal_snapshot_is_deeply_immutable():
    signal = make_signal(
        uuid4(), uuid4(), input_snapshot={"features": {"values": [1, 2]}}
    )
    with pytest.raises(TypeError):
        signal.input_snapshot["features"]["new"] = 3


def test_bid_ask_gross_pnl_uses_direction_and_correct_execution_sides():
    buy = make_outcome(make_signal(uuid4(), uuid4()), uuid4())
    assert buy.gross_pnl == pytest.approx(0.8)

    sell_signal = make_signal(uuid4(), uuid4(), direction=EvaluationDirection.SELL)
    sell = make_outcome(
        sell_signal,
        sell_signal.period_id,
        entry_price=2001.0,
        exit_price=2000.2,
        entry_price_side=ExecutionPriceSide.BID,
        exit_price_side=ExecutionPriceSide.ASK,
        gross_pnl=0.8,
    )
    assert sell.gross_pnl == pytest.approx(0.8)

    with pytest.raises(ValidationError):
        make_outcome(
            make_signal(uuid4(), uuid4()), uuid4(), gross_pnl=-0.8
        )

    with pytest.raises(ValidationError):
        make_outcome(
            make_signal(uuid4(), uuid4()), uuid4(),
            entry_price_side=ExecutionPriceSide.BID,
        )


def test_bid_ask_prices_do_not_double_count_spread():
    signal = make_signal(uuid4(), uuid4())
    outcome = make_outcome(signal, signal.period_id)
    assert outcome.outcome_type == "COUNTERFACTUAL"
    assert outcome.net_pnl is None

    with pytest.raises(ValidationError):
        make_outcome(signal, signal.period_id, spread_cost=0.2)


def test_ohlc_approximation_cannot_claim_spread_or_complete_costs():
    with pytest.raises(ValidationError):
        make_outcome(
            make_signal(uuid4(), uuid4()),
            uuid4(),
            measurement_basis=OutcomeMeasurementBasis.OHLC_APPROXIMATION,
            spread_accounting=SpreadAccounting.SEPARATE_COST,
            spread_cost=0.3,
        )

    with pytest.raises(ValidationError):
        make_outcome(
            make_signal(uuid4(), uuid4()),
            uuid4(),
            measurement_basis=OutcomeMeasurementBasis.OHLC_APPROXIMATION,
            spread_accounting=SpreadAccounting.UNKNOWN,
            entry_price_side=ExecutionPriceSide.UNKNOWN,
            exit_price_side=ExecutionPriceSide.UNKNOWN,
            costs_complete=True,
            slippage_cost=0.0,
            slippage_knowledge=CostKnowledge.MEASURED,
            commission_cost=0.0,
            commission_knowledge=CostKnowledge.MEASURED,
            swap_adjustment=0.0,
            swap_knowledge=CostKnowledge.MEASURED,
            net_pnl=0.8,
        )


def test_cost_completeness_distinguishes_unknown_from_not_applicable():
    signal = make_signal(uuid4(), uuid4())
    incomplete_costs = make_outcome(
        signal,
        signal.period_id,
        slippage_cost=0.1,
        slippage_knowledge=CostKnowledge.MEASURED,
        commission_knowledge=CostKnowledge.NOT_APPLICABLE,
        swap_knowledge=CostKnowledge.UNKNOWN,
    )
    assert incomplete_costs.costs_complete is False
    assert incomplete_costs.net_pnl is None

    complete = make_outcome(
        signal,
        signal.period_id,
        slippage_cost=0.1,
        slippage_knowledge=CostKnowledge.MEASURED,
        commission_knowledge=CostKnowledge.NOT_APPLICABLE,
        swap_adjustment=-0.05,
        swap_knowledge=CostKnowledge.MEASURED,
        costs_complete=True,
        net_pnl=0.75,
    )
    assert complete.net_pnl == pytest.approx(0.75)


def test_swap_credit_is_signed_and_increases_net_pnl():
    signal = make_signal(uuid4(), uuid4())
    outcome = make_outcome(
        signal,
        signal.period_id,
        slippage_cost=0.1,
        slippage_knowledge=CostKnowledge.MEASURED,
        commission_cost=0.05,
        commission_knowledge=CostKnowledge.MEASURED,
        swap_adjustment=-0.2,
        swap_knowledge=CostKnowledge.MEASURED,
        costs_complete=True,
        net_pnl=0.85,
    )
    assert outcome.net_pnl == pytest.approx(0.85)


def test_net_pnl_requires_all_applicable_costs_and_correct_equation():
    signal = make_signal(uuid4(), uuid4())
    with pytest.raises(ValidationError):
        make_outcome(
            signal,
            signal.period_id,
            slippage_cost=0.0,
            slippage_knowledge=CostKnowledge.MEASURED,
            commission_cost=0.0,
            commission_knowledge=CostKnowledge.MEASURED,
            swap_adjustment=0.0,
            swap_knowledge=CostKnowledge.UNKNOWN,
            costs_complete=True,
            net_pnl=0.8,
        )

    with pytest.raises(ValidationError):
        make_outcome(
            signal,
            signal.period_id,
            slippage_cost=0.1,
            slippage_knowledge=CostKnowledge.MEASURED,
            commission_cost=0.05,
            commission_knowledge=CostKnowledge.MEASURED,
            swap_adjustment=0.0,
            swap_knowledge=CostKnowledge.MEASURED,
            costs_complete=True,
            net_pnl=0.8,
        )


def test_incomplete_outcome_cannot_store_prices_costs_or_pnl():
    with pytest.raises(ValidationError):
        EvaluationOutcomeContract(
            outcome_id=uuid4(), run_id=uuid4(), signal_id=uuid4(), period_id=uuid4(),
            horizon_minutes=60, direction=EvaluationDirection.BUY,
            status=EvaluationOutcomeStatus.NEEDS_MORE_DATA,
            measurement_basis=OutcomeMeasurementBasis.UNAVAILABLE,
            entry_price=2000.0, created_at=NOW,
        )


def test_relationship_validation_enforces_run_period_signal_and_outcome_identity():
    run = make_run()
    periods = make_periods(run)
    signal = make_signal(
        run.run_id,
        periods[0].period_id,
        signal_at=periods[0].start_at + timedelta(seconds=1),
        input_available_at=periods[0].start_at,
    )
    outcome = make_outcome(signal, periods[0].period_id)

    validate_evaluation_relationships(run, periods, [signal], [outcome])

    wrong_direction = outcome.model_copy(update={"direction": EvaluationDirection.SELL})
    with pytest.raises(ValueError):
        validate_evaluation_relationships(run, periods, [signal], [wrong_direction])

    duplicate = outcome.model_copy(update={"outcome_id": uuid4()})
    with pytest.raises(ValueError):
        validate_evaluation_relationships(run, periods, [signal], [outcome, duplicate])


def test_contracts_reject_non_utc_timestamps():
    with pytest.raises(ValidationError):
        make_run(created_at=datetime(2026, 11, 21, 10, 0))
