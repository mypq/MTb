from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from backend.evaluation.contracts import (
    EvaluationDirection,
    EvaluationOutcomeStatus,
    EvaluationSignalContract,
    FilterDecision,
    OutcomeMeasurementBasis,
)
from backend.evaluation.ohlc_outcomes import EvaluationOHLCOutcomeCalculator
from backend.market_data.contracts import MarketCandleDataContract


UTC = timezone.utc
BASE = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)


def make_signal(*, direction=EvaluationDirection.BUY, signal_at=BASE + timedelta(seconds=30)):
    return EvaluationSignalContract(
        signal_id=uuid4(),
        run_id=uuid4(),
        period_id=uuid4(),
        instrument="XAUUSD",
        signal_at=signal_at,
        input_available_at=signal_at,
        direction=direction,
        reference_eligible=True,
        filter_decision=FilterDecision.BLOCKED,
        filter_reason="TEST_FILTER",
        source_candle_ids=(1,),
        source_batch_ids=(1,),
        created_at=BASE,
    )


def candle(minute, *, open_price=2000.0, close_price=2001.0, timeframe="M1",
           instrument="XAUUSD", low=None, high=None, candle_id=None):
    event_time = BASE + timedelta(minutes=minute)
    available = event_time + timedelta(minutes=1, seconds=1)
    actual_low = min(open_price, close_price) - 1.0 if low is None else low
    actual_high = max(open_price, close_price) + 1.0 if high is None else high
    return MarketCandleDataContract(
        candle_id=candle_id or minute + 100,
        batch_id=candle_id or minute + 100,
        instrument=instrument,
        timeframe=timeframe,
        event_time=event_time,
        available_at=available,
        recorded_at=available + timedelta(seconds=1),
        open=open_price,
        high=actual_high,
        low=actual_low,
        close=close_price,
        tick_volume=10,
        real_volume=None,
        volume_source="TICK",
        candle_quality="VERIFIED",
        is_closed=True,
        source="MT5",
        clock_verified=True,
        analysis_eligible=True,
        validation_report={"analysis_eligible": True},
    )


def make_window():
    # Signal is at 12:00:30; entry is the 12:01 M1 open.
    # Five-minute horizon exits at 12:06, using the 12:05 candle close.
    return tuple(
        candle(
            minute,
            open_price=2000.0 + minute,
            close_price=2000.5 + minute,
        )
        for minute in range(1, 7)
    )


def test_buy_outcome_uses_next_m1_open_and_exact_horizon_close():
    signal = make_signal()
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, make_window())

    assert result.status == EvaluationOutcomeStatus.COMPLETED
    assert result.measurement_basis == OutcomeMeasurementBasis.OHLC_APPROXIMATION
    assert result.entry_at == BASE + timedelta(minutes=1)
    assert result.exit_at == BASE + timedelta(minutes=6)
    assert result.entry_price == pytest.approx(2001.0)
    assert result.exit_price == pytest.approx(2005.5)
    assert result.gross_pnl == pytest.approx(4.5)
    assert result.net_pnl is None
    assert result.spread_cost is None


def test_sell_outcome_reverses_gross_pnl_sign():
    signal = make_signal(direction=EvaluationDirection.SELL)
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, make_window())

    assert result.status == EvaluationOutcomeStatus.COMPLETED
    assert result.gross_pnl == pytest.approx(-4.5)


def test_blocked_signal_still_receives_counterfactual_outcome():
    signal = make_signal()
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, make_window())

    assert signal.filter_decision == FilterDecision.BLOCKED
    assert result.signal_id == signal.signal_id
    assert result.outcome_type == "COUNTERFACTUAL"


def test_missing_m1_inside_holding_window_requires_more_data():
    signal = make_signal()
    candles = tuple(item for item in make_window() if item.event_time != BASE + timedelta(minutes=3))
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, candles)

    assert result.status == EvaluationOutcomeStatus.NEEDS_MORE_DATA
    assert result.measurement_basis == OutcomeMeasurementBasis.UNAVAILABLE
    assert result.entry_price is None
    assert result.gross_pnl is None


def test_duplicate_m1_timestamp_marks_outcome_invalid():
    signal = make_signal()
    candles = make_window()
    duplicate = candle(3, candle_id=999)
    result = EvaluationOHLCOutcomeCalculator().calculate(
        signal, 5, candles[:3] + (duplicate,) + candles[3:]
    )

    assert result.status == EvaluationOutcomeStatus.INVALID
    assert result.entry_price is None


def test_missing_entry_or_exit_candle_requires_more_data():
    signal = make_signal()
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, ())
    assert result.status == EvaluationOutcomeStatus.NEEDS_MORE_DATA

    # The 12:05 candle supplies the close at the exact 12:06 horizon boundary.
    missing_exit = tuple(
        item for item in make_window()
        if item.event_time != BASE + timedelta(minutes=5)
    )
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, missing_exit)
    assert result.status == EvaluationOutcomeStatus.NEEDS_MORE_DATA


def test_candle_start_equal_to_signal_time_is_a_valid_entry_boundary():
    signal = make_signal(signal_at=BASE + timedelta(minutes=1))
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, make_window())
    assert result.status == EvaluationOutcomeStatus.COMPLETED
    assert result.entry_at == signal.signal_at


@pytest.mark.parametrize(
    "overrides",
    [
        {"timeframe": "M5"},
        {"instrument": "EURUSD"},
        {"low": 2001.5},
    ],
)
def test_invalid_candle_metadata_or_ohlc_is_rejected_or_marked_invalid(overrides):
    signal = make_signal()
    candles = list(make_window())
    candles[0] = candle(1, **overrides)
    calculator = EvaluationOHLCOutcomeCalculator()

    if "timeframe" in overrides or "instrument" in overrides:
        with pytest.raises(ValueError):
            calculator.calculate(signal, 5, tuple(candles))
    else:
        result = calculator.calculate(signal, 5, tuple(candles))
        assert result.status == EvaluationOutcomeStatus.INVALID


def test_non_chronological_m1_input_is_rejected():
    signal = make_signal()
    candles = make_window()
    with pytest.raises(ValueError, match="chronological"):
        EvaluationOHLCOutcomeCalculator().calculate(
            signal, 5, (candles[0], candles[2], candles[1], *candles[3:])
        )


@pytest.mark.parametrize(
    "update",
    [
        {"clock_verified": False},
        {"analysis_eligible": False},
        {"is_closed": False},
        {"event_time": BASE + timedelta(minutes=1, seconds=1)},
    ],
)
def test_unverified_or_unaligned_m1_candles_cannot_produce_outcomes(update):
    signal = make_signal()
    candles = list(make_window())
    candles[0] = candles[0].model_copy(update=update)
    result = EvaluationOHLCOutcomeCalculator().calculate(signal, 5, tuple(candles))

    assert result.status == EvaluationOutcomeStatus.INVALID
    assert result.entry_price is None
    assert result.gross_pnl is None
