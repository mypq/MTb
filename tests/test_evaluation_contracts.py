from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from backend.evaluation.contracts import (
    EvaluationDirection,
    EvaluationOutcomeContract,
    EvaluationOutcomeStatus,
    EvaluationPeriodContract,
    EvaluationRunContract,
    EvaluationSignalContract,
    FilterDecision,
    OutcomeMeasurementBasis,
    SpreadAccounting,
)


UTC = timezone.utc
NOW = datetime(2026, 11, 21, 10, 0, tzinfo=UTC)


def make_run(**overrides):
    values = {
        "run_id": uuid4(),
        "instrument": " xauusd ",
        "created_at": NOW,
        "data_start_at": NOW - timedelta(days=30),
        "data_end_at": NOW,
        "code_revision": "abc123",
        "configuration_sha256": "a" * 64,
        "configuration_snapshot": {"baseline": "last_closed_candle_direction"},
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


def test_signal_contract_preserves_blocked_reference_signals():
    run = make_run()
    period_id = uuid4()
    signal = make_signal(run.run_id, period_id)

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


def test_completed_counterfactual_with_bid_ask_does_not_double_count_spread():
    outcome = EvaluationOutcomeContract(
        outcome_id=uuid4(), run_id=uuid4(), signal_id=uuid4(), period_id=uuid4(),
        horizon_minutes=5, status=EvaluationOutcomeStatus.COMPLETED,
        measurement_basis=OutcomeMeasurementBasis.BID_ASK_QUOTES,
        entry_at=NOW, exit_at=NOW + timedelta(minutes=5),
        entry_price=2000.2, exit_price=2001.0, gross_pnl=0.8,
        spread_accounting=SpreadAccounting.EMBEDDED_IN_BID_ASK_PRICES,
        spread_cost=None, costs_complete=False, created_at=NOW,
    )
    assert outcome.outcome_type == "COUNTERFACTUAL"
    assert outcome.net_pnl is None

    with pytest.raises(ValidationError):
        EvaluationOutcomeContract(
            outcome_id=uuid4(), run_id=uuid4(), signal_id=uuid4(), period_id=uuid4(),
            horizon_minutes=5, status=EvaluationOutcomeStatus.COMPLETED,
            measurement_basis=OutcomeMeasurementBasis.BID_ASK_QUOTES,
            entry_at=NOW, exit_at=NOW + timedelta(minutes=5),
            entry_price=2000.2, exit_price=2001.0, gross_pnl=0.8,
            spread_accounting=SpreadAccounting.EMBEDDED_IN_BID_ASK_PRICES,
            spread_cost=0.2, costs_complete=False, created_at=NOW,
        )


def test_ohlc_approximation_cannot_claim_spread_or_complete_execution_costs():
    with pytest.raises(ValidationError):
        EvaluationOutcomeContract(
            outcome_id=uuid4(), run_id=uuid4(), signal_id=uuid4(), period_id=uuid4(),
            horizon_minutes=15, status=EvaluationOutcomeStatus.COMPLETED,
            measurement_basis=OutcomeMeasurementBasis.OHLC_APPROXIMATION,
            entry_at=NOW, exit_at=NOW + timedelta(minutes=15),
            entry_price=2000.0, exit_price=2001.0, gross_pnl=1.0,
            spread_accounting=SpreadAccounting.SEPARATE_COST,
            spread_cost=0.3, costs_complete=False, created_at=NOW,
        )


def test_incomplete_outcome_cannot_store_prices_or_pnl():
    with pytest.raises(ValidationError):
        EvaluationOutcomeContract(
            outcome_id=uuid4(), run_id=uuid4(), signal_id=uuid4(), period_id=uuid4(),
            horizon_minutes=60, status=EvaluationOutcomeStatus.NEEDS_MORE_DATA,
            measurement_basis=OutcomeMeasurementBasis.UNAVAILABLE,
            entry_price=2000.0, created_at=NOW,
        )


def test_net_pnl_requires_complete_cost_inputs():
    with pytest.raises(ValidationError):
        EvaluationOutcomeContract(
            outcome_id=uuid4(), run_id=uuid4(), signal_id=uuid4(), period_id=uuid4(),
            horizon_minutes=5, status=EvaluationOutcomeStatus.COMPLETED,
            measurement_basis=OutcomeMeasurementBasis.OHLC_APPROXIMATION,
            entry_at=NOW, exit_at=NOW + timedelta(minutes=5),
            entry_price=2000.0, exit_price=2001.0, gross_pnl=1.0,
            spread_accounting=SpreadAccounting.UNKNOWN,
            slippage_cost=0.0, commission_cost=0.0, swap_cost=0.0,
            costs_complete=False, net_pnl=1.0, created_at=NOW,
        )


def test_contracts_reject_non_utc_timestamps():
    with pytest.raises(ValidationError):
        make_run(created_at=datetime(2026, 11, 21, 10, 0))
