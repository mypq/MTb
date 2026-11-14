
from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.market_data import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)

NOW = datetime.now(timezone.utc)


def make_candle(
    *,
    timestamp=None,
    symbol="XAUUSD",
    timeframe="M5",
    quality=CandleQuality.VERIFIED,
    volume_source=VolumeSource.TICK,
    tick_volume=100,
    real_volume=None,
):
    timestamp = timestamp or NOW - timedelta(minutes=5)

    return MarketCandle(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=timestamp,
        open=2000.0,
        high=2005.0,
        low=1995.0,
        close=2002.0,
        tick_volume=tick_volume,
        real_volume=real_volume,
        volume_source=volume_source,
        quality=quality,
        is_closed=True,
    )


def make_batch(candles=None, **overrides):
    values = {
        "symbol": "XAUUSD",
        "timeframe": "M5",
        "source": "MT5",
        "received_at": NOW,
        "candles": candles if candles is not None else [make_candle()],
    }
    values.update(overrides)
    return CandleBatch(**values)


def test_valid_candle_is_accepted():
    candle = make_candle()
    assert candle.symbol == "XAUUSD"
    assert candle.close == 2002.0


def test_candle_timestamp_must_be_utc():
    with pytest.raises(ValidationError):
        make_candle(timestamp=datetime(2026, 1, 1, 10, 0))


def test_candle_timestamp_rejects_non_utc_timezone():
    non_utc = datetime(
        2026,
        1,
        1,
        10,
        0,
        tzinfo=timezone(timedelta(hours=3)),
    )

    with pytest.raises(ValidationError):
        make_candle(timestamp=non_utc)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open", float("nan")),
        ("high", float("inf")),
        ("low", float("-inf")),
        ("close", 0),
        ("open", -1),
    ],
)
def test_invalid_prices_are_rejected(field, value):
    values = {
        "symbol": "XAUUSD",
        "timeframe": "M5",
        "timestamp": NOW - timedelta(minutes=5),
        "open": 2000.0,
        "high": 2005.0,
        "low": 1995.0,
        "close": 2002.0,
        "tick_volume": 100,
        "real_volume": None,
        "volume_source": VolumeSource.TICK,
        "quality": CandleQuality.VERIFIED,
        "is_closed": True,
    }
    values[field] = value

    with pytest.raises(ValidationError):
        MarketCandle(**values)


@pytest.mark.parametrize(
    ("high", "low"),
    [
        (1999.0, 1995.0),
        (2005.0, 2006.0),
    ],
)
def test_invalid_ohlc_relationships_are_rejected(high, low):
    with pytest.raises(ValidationError):
        MarketCandle(
            symbol="XAUUSD",
            timeframe="M5",
            timestamp=NOW - timedelta(minutes=5),
            open=2000.0,
            high=high,
            low=low,
            close=2002.0,
            tick_volume=100,
            real_volume=None,
            volume_source=VolumeSource.TICK,
            quality=CandleQuality.VERIFIED,
            is_closed=True,
        )


def test_negative_tick_volume_is_rejected():
    with pytest.raises(ValidationError):
        make_candle(tick_volume=-1)


def test_real_volume_requires_real_volume_source():
    with pytest.raises(ValidationError):
        make_candle(
            volume_source=VolumeSource.TICK,
            tick_volume=100,
            real_volume=100,
        )


def test_real_volume_is_accepted_with_real_volume_source():
    candle = make_candle(
        volume_source=VolumeSource.REAL,
        tick_volume=100,
        real_volume=100,
    )

    assert candle.real_volume == 100


def test_real_volume_source_requires_real_volume():
    with pytest.raises(ValidationError):
        make_candle(
            volume_source=VolumeSource.REAL,
            real_volume=None,
        )


def test_unclosed_candle_cannot_be_verified():
    with pytest.raises(ValidationError):
        MarketCandle(
            symbol="XAUUSD",
            timeframe="M5",
            timestamp=NOW - timedelta(minutes=5),
            open=2000.0,
            high=2005.0,
            low=1995.0,
            close=2002.0,
            tick_volume=100,
            real_volume=None,
            volume_source=VolumeSource.TICK,
            quality=CandleQuality.VERIFIED,
            is_closed=False,
        )


def test_valid_batch_is_accepted():
    batch = make_batch()
    assert batch.source == "MT5"
    assert len(batch.candles) == 1


def test_batch_accepts_chronological_candles():
    older = make_candle(timestamp=NOW - timedelta(minutes=10))
    newer = make_candle(timestamp=NOW - timedelta(minutes=5))

    batch = make_batch(candles=[older, newer])

    assert len(batch.candles) == 2


def test_batch_rejects_reversed_candle_order():
    older = make_candle(timestamp=NOW - timedelta(minutes=10))
    newer = make_candle(timestamp=NOW - timedelta(minutes=5))

    with pytest.raises(ValidationError):
        make_batch(candles=[newer, older])


def test_batch_rejects_duplicate_timestamps():
    timestamp = NOW - timedelta(minutes=5)
    first = make_candle(timestamp=timestamp)
    second = make_candle(timestamp=timestamp)

    with pytest.raises(ValidationError):
        make_batch(candles=[first, second])


def test_batch_rejects_mixed_symbols():
    first = make_candle(symbol="XAUUSD")
    second = make_candle(
        symbol="EURUSD",
        timestamp=NOW,
    )

    with pytest.raises(ValidationError):
        make_batch(candles=[first, second])


def test_batch_rejects_mixed_timeframes():
    first = make_candle(timeframe="M5")
    second = make_candle(
        timeframe="M15",
        timestamp=NOW,
    )

    with pytest.raises(ValidationError):
        make_batch(candles=[first, second])


def test_batch_requires_source():
    with pytest.raises(ValidationError):
        make_batch(source=None)


def test_batch_rejects_empty_source():
    with pytest.raises(ValidationError):
        make_batch(source="   ")


def test_batch_rejects_naive_received_at():
    with pytest.raises(ValidationError):
        make_batch(received_at=datetime(2026, 1, 1, 10, 0))


def test_batch_rejects_non_utc_received_at():
    non_utc = datetime(
        2026,
        1,
        1,
        10,
        0,
        tzinfo=timezone(timedelta(hours=3)),
    )

    with pytest.raises(ValidationError):
        make_batch(received_at=non_utc)
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("open", True),
        ("open", False),
        ("high", True),
        ("low", False),
        ("close", True),
        ("tick_volume", True),
    ],
)
def test_boolean_numeric_candle_fields_are_rejected(field, value):
    values = {
        "symbol": "XAUUSD",
        "timeframe": "M5",
        "timestamp": NOW - timedelta(minutes=5),
        "open": 2000.0,
        "high": 2005.0,
        "low": 1995.0,
        "close": 2002.0,
        "tick_volume": 100,
        "real_volume": None,
        "volume_source": VolumeSource.TICK,
        "quality": CandleQuality.VERIFIED,
        "is_closed": True,
    }
    values[field] = value

    with pytest.raises(ValidationError):
        MarketCandle(**values)


def test_boolean_real_volume_is_rejected():
    with pytest.raises(ValidationError):
        make_candle(
            volume_source=VolumeSource.REAL,
            real_volume=True,
        )
