from datetime import datetime, timedelta, timezone

from pydantic import ValidationError
import pytest

from backend.market_data import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)

UTC = timezone.utc


def make_candle(
    *,
    timestamp: datetime | None = None,
    open_price: float = 2000.0,
    high: float = 2010.0,
    low: float = 1990.0,
    close: float = 2005.0,
    tick_volume: int = 100,
    real_volume: int | None = None,
    volume_source: VolumeSource = VolumeSource.TICK,
    quality: CandleQuality = CandleQuality.VERIFIED,
    symbol: str = "XAUUSD",
    timeframe: str = "M5",
) -> MarketCandle:
    candle_timestamp = timestamp or datetime(
        2026, 1, 1, 10, 0, tzinfo=UTC
    )

    return MarketCandle(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=candle_timestamp,
        open=open_price,
        high=high,
        low=low,
        close=close,
        tick_volume=tick_volume,
        real_volume=real_volume,
        volume_source=volume_source,
        quality=quality,
        is_closed=True,
    )


def test_valid_candle_is_created():
    candle = make_candle()

    assert candle.symbol == "XAUUSD"
    assert candle.timeframe == "M5"
    assert candle.close == 2005.0


def test_timestamp_must_be_utc():
    with pytest.raises(ValidationError):
        make_candle(
            timestamp=datetime(2026, 1, 1, 10, 0)
        )


def test_non_utc_timestamp_is_rejected():
    non_utc = timezone(timedelta(hours=3))

    with pytest.raises(ValidationError):
        make_candle(
            timestamp=datetime(
                2026, 1, 1, 10, 0, tzinfo=non_utc
            )
        )


@pytest.mark.parametrize(
    "price",
    [0, -1, float("inf"), float("-inf"), float("nan")],
)
def test_invalid_prices_are_rejected(price):
    with pytest.raises(ValidationError):
        make_candle(open_price=price)


def test_high_cannot_be_lower_than_open():
    with pytest.raises(ValidationError):
        make_candle(high=1999.0)


def test_high_cannot_be_lower_than_close():
    with pytest.raises(ValidationError):
        make_candle(high=2000.0, close=2005.0)


def test_low_cannot_be_higher_than_open():
    with pytest.raises(ValidationError):
        make_candle(low=2001.0)


def test_low_cannot_be_higher_than_close():
    with pytest.raises(ValidationError):
        make_candle(low=2006.0)


def test_negative_tick_volume_is_rejected():
    with pytest.raises(ValidationError):
        make_candle(tick_volume=-1)


def test_negative_real_volume_is_rejected():
    with pytest.raises(ValidationError):
        make_candle(
            real_volume=-1,
            volume_source=VolumeSource.REAL,
        )


def test_real_volume_requires_real_volume_source():
    with pytest.raises(ValidationError):
        make_candle(
            real_volume=100,
            volume_source=VolumeSource.TICK,
        )


def test_real_volume_source_requires_real_volume():
    with pytest.raises(ValidationError):
        make_candle(
            real_volume=None,
            volume_source=VolumeSource.REAL,
        )


def test_unclosed_candle_cannot_be_verified():
    with pytest.raises(ValidationError):
        MarketCandle(
            symbol="XAUUSD",
            timeframe="M5",
            timestamp=datetime(2026, 1, 1, 10, 0, tzinfo=UTC),
            open=2000.0,
            high=2010.0,
            low=1990.0,
            close=2005.0,
            tick_volume=100,
            real_volume=None,
            volume_source=VolumeSource.TICK,
            quality=CandleQuality.VERIFIED,
            is_closed=False,
        )


def test_batch_accepts_chronological_candles():
    first = make_candle(
        timestamp=datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    )
    second = make_candle(
        timestamp=datetime(2026, 1, 1, 10, 5, tzinfo=UTC)
    )

    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=[first, second],
        received_at=datetime(2026, 1, 1, 10, 10, tzinfo=UTC),
    )

    assert len(batch.candles) == 2


def test_batch_rejects_out_of_order_candles():
    first = make_candle(
        timestamp=datetime(2026, 1, 1, 10, 5, tzinfo=UTC)
    )
    second = make_candle(
        timestamp=datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    )

    with pytest.raises(ValidationError):
        CandleBatch(
            symbol="XAUUSD",
            timeframe="M5",
            candles=[first, second],
            received_at=datetime(2026, 1, 1, 10, 10, tzinfo=UTC),
        )


def test_batch_rejects_duplicate_timestamps():
    timestamp = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)

    first = make_candle(timestamp=timestamp)
    second = make_candle(timestamp=timestamp)

    with pytest.raises(ValidationError):
        CandleBatch(
            symbol="XAUUSD",
            timeframe="M5",
            candles=[first, second],
            received_at=datetime(2026, 1, 1, 10, 10, tzinfo=UTC),
        )


def test_batch_rejects_mixed_symbols():
    first = make_candle(symbol="XAUUSD")
    second = make_candle(
        timestamp=datetime(2026, 1, 1, 10, 5, tzinfo=UTC),
        symbol="EURUSD",
    )

    with pytest.raises(ValidationError):
        CandleBatch(
            symbol="XAUUSD",
            timeframe="M5",
            candles=[first, second],
            received_at=datetime(2026, 1, 1, 10, 10, tzinfo=UTC),
        )


def test_batch_rejects_mixed_timeframes():
    first = make_candle(timeframe="M5")
    second = make_candle(
        timestamp=datetime(2026, 1, 1, 10, 5, tzinfo=UTC),
        timeframe="M15",
    )

    with pytest.raises(ValidationError):
        CandleBatch(
            symbol="XAUUSD",
            timeframe="M5",
            candles=[first, second],
            received_at=datetime(2026, 1, 1, 10, 10, tzinfo=UTC),
        )


def test_batch_received_at_must_be_utc():
    candle = make_candle()

    with pytest.raises(ValidationError):
        CandleBatch(
            symbol="XAUUSD",
            timeframe="M5",
            candles=[candle],
            received_at=datetime(2026, 1, 1, 10, 10),
)
