from datetime import datetime, timedelta, timezone

import pytest

from backend.market_data.models import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)
from backend.market_data.validator import (
    MarketDataIssueCode,
    MarketDataValidator,
)


UTC = timezone.utc
CHECKED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def make_candle(
    timestamp: datetime,
    *,
    is_closed: bool = True,
    quality: CandleQuality = CandleQuality.VERIFIED,
    volume_source: VolumeSource = VolumeSource.TICK,
) -> MarketCandle:
    return MarketCandle(
        symbol="XAUUSD",
        timeframe="M5",
        timestamp=timestamp,
        open=2000.0,
        high=2005.0,
        low=1995.0,
        close=2002.0,
        tick_volume=100,
        real_volume=None,
        volume_source=volume_source,
        is_closed=is_closed,
        quality=quality,
    )


def make_batch(
    candles: tuple[MarketCandle, ...],
    *,
    received_at: datetime = CHECKED_AT,
) -> CandleBatch:
    return CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=candles,
        received_at=received_at,
        source="MT5",
    )


def test_valid_batch_is_eligible_for_analysis():
    candles = (
        make_candle(CHECKED_AT - timedelta(minutes=10)),
        make_candle(CHECKED_AT - timedelta(minutes=5)),
    )
    report = MarketDataValidator().validate(
        make_batch(candles),
        timeframe_seconds=300,
        checked_at=CHECKED_AT,
    )

    assert report.structurally_valid is True
    assert report.analysis_eligible is True
    assert report.volume_data_available is True
    assert report.candle_count == 2
    assert report.issues == ()
    assert report.gaps == ()


def test_exact_timeframe_interval_has_no_gap():
    candles = (
        make_candle(CHECKED_AT - timedelta(minutes=5)),
        make_candle(CHECKED_AT),
    )
    report = MarketDataValidator().validate(
        make_batch(candles),
        300,
        CHECKED_AT,
    )

    assert report.gaps == ()
    assert report.analysis_eligible is True


def test_time_gap_blocks_analysis():
    candles = (
        make_candle(CHECKED_AT - timedelta(minutes=15)),
        make_candle(CHECKED_AT),
    )
    report = MarketDataValidator().validate(
        make_batch(candles),
        300,
        CHECKED_AT,
    )

    assert report.analysis_eligible is False
    assert len(report.gaps) == 1
    assert report.gaps[0].missing_intervals == 2
    assert any(
        issue.code == MarketDataIssueCode.TIME_GAP
        for issue in report.issues
    )


def test_future_candle_blocks_analysis():
    candles = (
        make_candle(CHECKED_AT + timedelta(minutes=1)),
    )
    report = MarketDataValidator().validate(
        make_batch(candles),
        300,
        CHECKED_AT,
    )

    assert report.structurally_valid is False
    assert report.analysis_eligible is False
    assert any(
        issue.code == MarketDataIssueCode.FUTURE_CANDLE
        for issue in report.issues
    )


def test_future_receive_time_blocks_analysis():
    candles = (
        make_candle(CHECKED_AT - timedelta(minutes=5)),
    )
    batch = make_batch(
        candles,
        received_at=CHECKED_AT + timedelta(seconds=1),
    )

    report = MarketDataValidator().validate(
        batch,
        300,
        CHECKED_AT,
    )

    assert report.structurally_valid is False
    assert report.analysis_eligible is False
    assert any(
        issue.code == MarketDataIssueCode.FUTURE_RECEIVE_TIME
        for issue in report.issues
    )


def test_unclosed_candle_is_rejected():
    candle = make_candle(
        CHECKED_AT - timedelta(minutes=5),
        is_closed=False,
        quality=CandleQuality.INCOMPLETE,
    )
    report = MarketDataValidator().validate(
        make_batch((candle,)),
        300,
        CHECKED_AT,
    )

    assert report.structurally_valid is False
    assert report.analysis_eligible is False
    assert any(
        issue.code == MarketDataIssueCode.UNCLOSED_CANDLE
        for issue in report.issues
    )


def test_incomplete_quality_is_rejected():
    candle = make_candle(
        CHECKED_AT - timedelta(minutes=5),
        quality=CandleQuality.INCOMPLETE,
    )
    report = MarketDataValidator().validate(
        make_batch((candle,)),
        300,
        CHECKED_AT,
    )

    assert report.analysis_eligible is False
    assert any(
        issue.code == MarketDataIssueCode.UNVERIFIED_CANDLE
        for issue in report.issues
    )


def test_invalid_quality_is_rejected():
    candle = make_candle(
        CHECKED_AT - timedelta(minutes=5),
        quality=CandleQuality.INVALID,
    )
    report = MarketDataValidator().validate(
        make_batch((candle,)),
        300,
        CHECKED_AT,
    )

    assert report.analysis_eligible is False
    assert any(
        issue.code == MarketDataIssueCode.UNVERIFIED_CANDLE
        for issue in report.issues
    )


def test_unknown_volume_source_blocks_volume_data_only():
    candle = make_candle(
        CHECKED_AT - timedelta(minutes=5),
        volume_source=VolumeSource.UNKNOWN,
    )
    report = MarketDataValidator().validate(
        make_batch((candle,)),
        300,
        CHECKED_AT,
    )

    assert report.structurally_valid is True
    assert report.analysis_eligible is True
    assert report.volume_data_available is False
    assert any(
        issue.code == MarketDataIssueCode.UNKNOWN_VOLUME_SOURCE
        for issue in report.issues
    )


@pytest.mark.parametrize("timeframe_seconds", [0, -1])
def test_invalid_timeframe_duration_is_rejected(timeframe_seconds):
    candle = make_candle(CHECKED_AT - timedelta(minutes=5))

    with pytest.raises(ValueError):
        MarketDataValidator().validate(
            make_batch((candle,)),
            timeframe_seconds,
            CHECKED_AT,
        )


@pytest.mark.parametrize("timeframe_seconds", [True, 300.0, "300"])
def test_non_integer_timeframe_duration_is_rejected(timeframe_seconds):
    candle = make_candle(CHECKED_AT - timedelta(minutes=5))

    with pytest.raises(TypeError):
        MarketDataValidator().validate(
            make_batch((candle,)),
            timeframe_seconds,
            CHECKED_AT,
        )


def test_naive_checked_at_is_rejected():
    candle = make_candle(CHECKED_AT - timedelta(minutes=5))

    with pytest.raises(ValueError):
        MarketDataValidator().validate(
            make_batch((candle,)),
            300,
            datetime(2026, 1, 1, 12, 0),
        )


def test_non_utc_checked_at_is_rejected():
    candle = make_candle(CHECKED_AT - timedelta(minutes=5))
    non_utc = datetime(
        2026, 1, 1, 15, 0,
        tzinfo=timezone(timedelta(hours=3)),
    )

    with pytest.raises(ValueError):
        MarketDataValidator().validate(
            make_batch((candle,)),
            300,
            non_utc,
        )


def test_report_preserves_batch_identity():
    candle = make_candle(CHECKED_AT - timedelta(minutes=5))
    report = MarketDataValidator().validate(
        make_batch((candle,)),
        300,
        CHECKED_AT,
    )

    assert report.symbol == "XAUUSD"
    assert report.timeframe == "M5"
    assert report.source == "MT5"
    assert report.checked_at == CHECKED_AT
