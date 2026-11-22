from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from backend.evaluation.contracts import EvaluationSignalContract
from backend.evaluation.market_data import (
    EvaluationMarketDataError,
    EvaluationMarketDataReader,
)
from backend.market_data.contracts import MarketCandleDataContract


UTC = timezone.utc
SIGNAL_AT = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)


class FakeCandleRepository:
    def __init__(self, candles):
        self.candles = {item.candle_id: item for item in candles}

    def get_candle_contract(self, candle_id):
        return self.candles.get(candle_id)


def make_candle(
    candle_id=11,
    batch_id=21,
    *,
    event_time=None,
    available_at=None,
    instrument="XAUUSD",
    is_closed=True,
    candle_quality="VERIFIED",
    clock_verified=True,
    analysis_eligible=True,
    validation_eligible=True,
):
    return MarketCandleDataContract(
        candle_id=candle_id,
        batch_id=batch_id,
        instrument=instrument,
        timeframe="M5",
        event_time=event_time or SIGNAL_AT - timedelta(minutes=5),
        available_at=available_at or SIGNAL_AT - timedelta(minutes=4),
        recorded_at=SIGNAL_AT - timedelta(minutes=3),
        open=2000.0,
        high=2002.0,
        low=1999.0,
        close=2001.0,
        tick_volume=100,
        real_volume=None,
        volume_source="TICK",
        candle_quality=candle_quality,
        is_closed=is_closed,
        source="MT5",
        source_account_login=123456,
        source_broker_server="Test-Broker",
        clock_verified=clock_verified,
        analysis_eligible=analysis_eligible,
        validation_report={"analysis_eligible": validation_eligible},
    )


def make_signal(*, candle_ids=(11,), batch_ids=(21,), input_available_at=None):
    return EvaluationSignalContract(
        signal_id=uuid4(),
        run_id=uuid4(),
        period_id=uuid4(),
        instrument="XAUUSD",
        signal_at=SIGNAL_AT,
        input_available_at=input_available_at or SIGNAL_AT - timedelta(minutes=2),
        direction="BUY",
        reference_eligible=True,
        filter_decision="ALLOWED",
        source_candle_ids=candle_ids,
        source_batch_ids=batch_ids,
        created_at=SIGNAL_AT,
    )


def test_reader_resolves_verified_source_candles_and_lineage():
    candles = (
        make_candle(candle_id=11, batch_id=21),
        make_candle(
            candle_id=12,
            batch_id=22,
            event_time=SIGNAL_AT - timedelta(minutes=10),
            available_at=SIGNAL_AT - timedelta(minutes=6),
        ),
    )
    signal = make_signal(candle_ids=(11, 12), batch_ids=(21, 22))

    evidence = EvaluationMarketDataReader(
        FakeCandleRepository(candles)
    ).resolve_signal_inputs(signal)

    assert evidence.signal_id == str(signal.signal_id)
    assert evidence.source_candle_ids == (11, 12)
    assert evidence.source_batch_ids == (21, 22)
    assert evidence.latest_available_at == max(item.available_at for item in candles)


def test_reader_rejects_missing_source_candle():
    signal = make_signal()
    reader = EvaluationMarketDataReader(FakeCandleRepository(()))

    with pytest.raises(EvaluationMarketDataError, match="does not exist"):
        reader.resolve_signal_inputs(signal)


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"instrument": "EURUSD"}, "another instrument"),
        ({"event_time": SIGNAL_AT + timedelta(seconds=1)}, "occurs after"),
        ({"available_at": SIGNAL_AT - timedelta(minutes=1)}, "became available after"),
        ({"recorded_at": SIGNAL_AT - timedelta(seconds=1)}, "was recorded after"),
        ({"is_closed": False}, "verified closed"),
        ({"candle_quality": "INCOMPLETE"}, "verified closed"),
        ({"clock_verified": False}, "verified source clock"),
        ({"analysis_eligible": False}, "not approved"),
        ({"validation_report": {"analysis_eligible": False}}, "positive persisted"),
    ],
)
def test_reader_rejects_untrusted_or_lookahead_source_candles(changes, message):
    candle = make_candle().model_copy(update=changes)
    signal = make_signal()

    reader = EvaluationMarketDataReader(FakeCandleRepository((candle,)))

    with pytest.raises(EvaluationMarketDataError, match=message):
        reader.resolve_signal_inputs(signal)


def test_reader_rejects_batch_lineage_mismatch():
    signal = make_signal(batch_ids=(999,))
    reader = EvaluationMarketDataReader(
        FakeCandleRepository((make_candle(),))
    )

    with pytest.raises(EvaluationMarketDataError, match="source_batch_ids"):
        reader.resolve_signal_inputs(signal)


def test_reader_rejects_invalid_signal_type():
    reader = EvaluationMarketDataReader(FakeCandleRepository(()))

    with pytest.raises(TypeError, match="EvaluationSignalContract"):
        reader.resolve_signal_inputs(object())
