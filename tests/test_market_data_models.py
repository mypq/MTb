from datetime import datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from backend.market_data import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)


UTC = timezone.utc
BASE_TIME = datetime(2026, 11, 13, 10, 0, tzinfo=UTC)


def make_candle(**overrides: object) -> MarketCandle:
    values: dict[str, object] = {
        "symbol": "XAUUSD",
        "timeframe": "M5",
        "timestamp": BASE_TIME,
        "open": 2650.0,
        "high": 2655.0,
        "low": 2648.0,
        "close": 2653.0,
        "tick_volume": 120,
        "real_volume": None,
        "volume_source": VolumeSource.TICK,
        "is_closed": True,
        "quality": CandleQuality.VERIFIED,
    }
    values.update(overrides)
    return MarketCandle(**values)


def make_batch(*candles: MarketCandle) -> CandleBatch:
    return CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=candles,
        received_at=BASE_TIME + timedelta(minutes=15),
        source="MT5",
    )


def test_valid_candle_is_accepted() -> None:
    candle = make_candle()

    assert candle.symbol == "XAUUSD"
    assert candle.timeframe == "M5"
    assert candle.close == 2653.0


def test_candle_timestamp_is_utc() -> None:
    candle = make_candle()

    assert candle.timestamp.utcoffset() == timedelta(0)


def test_naive_timestamp_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_candle(timestamp=datetime(2026, 11, 13, 10, 0))


def test_non_utc_timestamp_is_rejected() -> None:
    non_utc = datetime(
        2026, 11, 13, 13, 0,
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
        ("close", 0.0),
    ],
)
def test_invalid_prices_are_rejected(field: str, value: float) -> None:
    with pytest.raises(ValidationError):
        make_candle(**{field: value})


def test_invalid_ohlc_relationship_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_candle(high=2649.0)


def test_negative_tick_volume_is_rejected() -> None:
    with pytest.raises(ValidationError):
        make_candle(tick_volume=-1)


def test_real_volume_requires_real_volume_value() -> None:
    with pytest.raises(ValidationError):
        make_candle(
            volume_source=VolumeSource.REAL,
            real_volume=None,
        )


def test_tick_and_real_volume_are_kept_separate() -> None:
    candle = make_candle(
        tick_volume=120,
        real_volume=75,
        volume_source=VolumeSource.REAL,
    )

    assert candle.tick_volume == 120
    assert candle.real_volume == 75
    assert candle.volume_source == VolumeSource.REAL


def test_unclosed_candle_cannot_be_verified() -> None:
    with pytest.raises(ValidationError):
        make_candle(is_closed=False)


def test_unclosed_candle_can_be_incomplete() -> None:
    candle = make_candle(
        is_closed=False,
        quality=CandleQuality.INCOMPLETE,
    )

    assert candle.quality == CandleQuality.INCOMPLETE


def test_batch_accepts_chronological_candles() -> None:
    first = make_candle(timestamp=BASE_TIME)
    second = make_candle(
        timestamp=BASE_TIME + timedelta(minutes=5)
    )

    batch = make_batch(first, second)

    assert len(batch.candles) == 2
    assert batch.candles[0].timestamp < batch.candles[1].timestamp


def test_batch_rejects_reversed_candles() -> None:
    first = make_candle(timestamp=BASE_TIME)
    second = make_candle(
        timestamp=BASE_TIME + timedelta(minutes=5)
    )

    with pytest.raises(ValidationError):
        make_batch(second, first)


def test_batch_rejects_duplicate_timestamps() -> None:
    first = make_candle()
    second = make_candle(close=2654.0)

    with pytest.raises(ValidationError):
        make_batch(first, second)


def test_batch_rejects_different_symbols() -> None:
    first = make_candle()
    second = make_candle(
        symbol="EURUSD",
        timestamp=BASE_TIME + timedelta(minutes=5),
    )

    with pytest.raises(ValidationError):
        make_batch(first, second)


def test_batch_rejects_different_timeframes() -> None:
    first = make_candle()
    second = make_candle(
        timeframe="M15",
        timestamp=BASE_TIME + timedelta(minutes=5),
    )

    with pytest.raises(ValidationError):
        make_batch(first, second)


def test_batch_rejects_naive_received_at() -> None:
    candle = make_candle()

    with pytest.raises(ValidationError):
        CandleBatch(
            symbol="XAUUSD",
            timeframe="M5",
            candles=(candle,),
            received_at=datetime(2026, 11, 13, 10, 15),
            source="MT5",
    )
