from datetime import datetime, timedelta, timezone

import pytest

from backend.market_data.models import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)
from backend.market_data.service import (
    MarketDataContractError,
    MarketDataRejectedError,
    MarketDataService,
)

UTC = timezone.utc
CHECKED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def make_candle(
    timestamp: datetime,
    *,
    symbol: str = "XAUUSD",
    timeframe: str = "M5",
    volume_source: VolumeSource = VolumeSource.TICK,
) -> MarketCandle:
    return MarketCandle(
        symbol=symbol,
        timeframe=timeframe,
        timestamp=timestamp,
        open=2000.0,
        high=2005.0,
        low=1995.0,
        close=2002.0,
        tick_volume=100,
        real_volume=None,
        volume_source=volume_source,
        is_closed=True,
        quality=CandleQuality.VERIFIED,
    )


def make_batch(
    *,
    symbol: str = "XAUUSD",
    timeframe: str = "M5",
    count: int = 2,
    received_at: datetime = CHECKED_AT,
) -> CandleBatch:
    candles = tuple(
        make_candle(
            CHECKED_AT - timedelta(minutes=5 * offset),
            symbol=symbol,
            timeframe=timeframe,
        )
        for offset in reversed(range(1, count + 1))
    )

    return CandleBatch(
        symbol=symbol,
        timeframe=timeframe,
        candles=candles,
        received_at=received_at,
        source="MT5",
    )


class FakeMarketDataSource:
    def __init__(self, batch: CandleBatch):
        self.batch = batch
        self.last_request = None

    def get_closed_candles(
        self,
        symbol: str,
        timeframe: str,
        count: int,
    ) -> CandleBatch:
        self.last_request = {
            "symbol": symbol,
            "timeframe": timeframe,
            "count": count,
        }
        return self.batch


def make_service(batch: CandleBatch) -> tuple[MarketDataService, FakeMarketDataSource]:
    source = FakeMarketDataSource(batch)
    return MarketDataService(source), source


def test_valid_snapshot_matches_requested_data():
    batch = make_batch()
    service, source = make_service(batch)

    snapshot = service.get_snapshot(
        "XAUUSD",
        "M5",
        2,
        CHECKED_AT,
    )

    assert snapshot.batch == batch
    assert snapshot.analysis_eligible is True
    assert snapshot.validation.freshness_eligible is True
    assert source.last_request == {
        "symbol": "XAUUSD",
        "timeframe": "M5",
        "count": 2,
    }


def test_symbol_is_trimmed_before_source_request():
    batch = make_batch()
    service, source = make_service(batch)

    service.get_snapshot(
        " XAUUSD ",
        "M5",
        2,
        CHECKED_AT,
    )

    assert source.last_request["symbol"] == "XAUUSD"


def test_timeframe_is_normalized_before_source_request():
    batch = make_batch()
    service, source = make_service(batch)

    service.get_snapshot(
        "XAUUSD",
        "m5",
        2,
        CHECKED_AT,
    )

    assert source.last_request["timeframe"] == "M5"


def test_returned_symbol_mismatch_is_rejected():
    batch = make_batch(symbol="EURUSD")
    service, _ = make_service(batch)

    with pytest.raises(
        MarketDataContractError,
        match="Returned symbol",
    ):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )


def test_returned_timeframe_mismatch_is_rejected():
    batch = make_batch(
        timeframe="M15",
        count=2,
    )
    service, _ = make_service(batch)

    with pytest.raises(
        MarketDataContractError,
        match="Returned timeframe",
    ):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )


def test_returned_unsupported_timeframe_is_rejected():
    batch = make_batch(
        timeframe="M5",
        count=2,
    )
    invalid_batch = batch.model_copy(
        update={"timeframe": "W1"},
    )
    service, _ = make_service(invalid_batch)

    with pytest.raises(
        MarketDataContractError,
        match="unsupported timeframe",
    ):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )


def test_returned_candle_count_mismatch_is_rejected():
    batch = make_batch(count=1)
    service, _ = make_service(batch)

    with pytest.raises(
        MarketDataContractError,
        match="Requested 2 candles but received 1",
    ):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )


def test_non_batch_source_response_is_rejected():
    source = FakeMarketDataSource(make_batch())
    source.batch = None
    service = MarketDataService(source)

    with pytest.raises(
        MarketDataContractError,
        match="must return a CandleBatch",
    ):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )


@pytest.mark.parametrize("count", [0, -1])
def test_non_positive_count_is_rejected(count):
    service, _ = make_service(make_batch())

    with pytest.raises(ValueError, match="greater than zero"):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            count,
            CHECKED_AT,
        )


@pytest.mark.parametrize("count", [True, 2.0, "2"])
def test_non_integer_count_is_rejected(count):
    service, _ = make_service(make_batch())

    with pytest.raises(TypeError, match="must be an integer"):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            count,
            CHECKED_AT,
        )


def test_empty_symbol_is_rejected_before_source_request():
    service, source = make_service(make_batch())

    with pytest.raises(ValueError, match="non-empty"):
        service.get_snapshot(
            " ",
            "M5",
            2,
            CHECKED_AT,
        )

    assert source.last_request is None


def test_unsupported_requested_timeframe_is_rejected():
    service, source = make_service(make_batch())

    with pytest.raises(ValueError, match="Unsupported timeframe"):
        service.get_snapshot(
            "XAUUSD",
            "W1",
            2,
            CHECKED_AT,
        )

    assert source.last_request is None


def test_stale_snapshot_is_not_eligible_for_analysis():
    old_candle = make_candle(
        CHECKED_AT - timedelta(hours=2),
    )
    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=(old_candle,),
        received_at=CHECKED_AT,
        source="MT5",
    )
    service = MarketDataService(FakeMarketDataSource(batch))

    snapshot = service.get_snapshot(
        "XAUUSD",
        "M5",
        1,
        CHECKED_AT,
    )

    assert snapshot.validation.freshness_eligible is False
    assert snapshot.analysis_eligible is False


def test_stale_snapshot_is_rejected_from_analysis_batch():
    old_candle = make_candle(
        CHECKED_AT - timedelta(hours=2),
    )
    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=(old_candle,),
        received_at=CHECKED_AT,
        source="MT5",
    )
    service = MarketDataService(FakeMarketDataSource(batch))

    with pytest.raises(
        MarketDataRejectedError,
        match="STALE_DATA",
    ):
        service.get_analysis_batch(
            "XAUUSD",
            "M5",
            1,
            checked_at=CHECKED_AT,
        )


def test_analysis_batch_returns_valid_candles():
    batch = make_batch()
    service, _ = make_service(batch)

    result = service.get_analysis_batch(
        "XAUUSD",
        "M5",
        2,
        checked_at=CHECKED_AT,
    )

    assert result == batch


def test_unknown_volume_blocks_only_volume_required_analysis():
    candle = make_candle(
        CHECKED_AT - timedelta(minutes=5),
        volume_source=VolumeSource.UNKNOWN,
    )
    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=(candle,),
        received_at=CHECKED_AT,
        source="MT5",
    )
    service = MarketDataService(FakeMarketDataSource(batch))

    result = service.get_analysis_batch(
        "XAUUSD",
        "M5",
        1,
        require_volume=False,
        checked_at=CHECKED_AT,
    )

    assert result == batch

    with pytest.raises(MarketDataRejectedError):
        service.get_analysis_batch(
            "XAUUSD",
            "M5",
            1,
            require_volume=True,
            checked_at=CHECKED_AT,
)

@pytest.mark.parametrize("symbol", [None, 123, True, [], {}])
def test_non_string_symbol_is_rejected_before_source_request(symbol):
    service, source = make_service(make_batch())

    with pytest.raises(ValueError, match="non-empty string"):
        service.get_snapshot(
            symbol,
            "M5",
            2,
            CHECKED_AT,
        )

    assert source.last_request is None


@pytest.mark.parametrize("timeframe", [None, 123, True, [], {}])
def test_non_string_requested_timeframe_is_rejected_before_source_request(
    timeframe,
):
    service, source = make_service(make_batch())

    with pytest.raises(TypeError, match="must be a string"):
        service.get_snapshot(
            "XAUUSD",
            timeframe,
            2,
            CHECKED_AT,
        )

    assert source.last_request is None


def test_invalid_symbol_does_not_reach_source():
    service, source = make_service(make_batch())

    with pytest.raises(ValueError):
        service.get_snapshot(
            "   ",
            "M5",
            2,
            CHECKED_AT,
        )

    assert source.last_request is None


def test_invalid_count_does_not_reach_source():
    service, source = make_service(make_batch())

    with pytest.raises(ValueError):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            0,
            CHECKED_AT,
        )

    assert source.last_request is None


def test_source_exception_is_propagated_without_fabricating_data():
    class FailingSource:
        def get_closed_candles(self, symbol, timeframe, count):
            raise RuntimeError("MT5 source failure")

    service = MarketDataService(FailingSource())

    with pytest.raises(RuntimeError, match="MT5 source failure"):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )


def test_source_receives_normalized_symbol_timeframe_and_count():
    batch = make_batch()
    service, source = make_service(batch)

    service.get_snapshot(
        " XAUUSD ",
        "m5",
        2,
        CHECKED_AT,
    )

    assert source.last_request == {
        "symbol": "XAUUSD",
        "timeframe": "M5",
        "count": 2,
    }


def test_analysis_batch_rejects_time_gaps():
    candles = (
        make_candle(CHECKED_AT - timedelta(minutes=20)),
        make_candle(CHECKED_AT - timedelta(minutes=5)),
    )

    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=candles,
        received_at=CHECKED_AT,
        source="MT5",
    )

    service = MarketDataService(FakeMarketDataSource(batch))

    with pytest.raises(
        MarketDataRejectedError,
        match="TIME_GAP",
    ) as error:
        service.get_analysis_batch(
            "XAUUSD",
            "M5",
            2,
            checked_at=CHECKED_AT,
        )

    assert error.value.snapshot.analysis_eligible is False
    assert len(error.value.snapshot.validation.gaps) == 1


def test_volume_required_analysis_rejection_preserves_snapshot():
    candle = make_candle(
        CHECKED_AT - timedelta(minutes=5),
        volume_source=VolumeSource.UNKNOWN,
    )

    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=(candle,),
        received_at=CHECKED_AT,
        source="MT5",
    )

    service = MarketDataService(FakeMarketDataSource(batch))

    with pytest.raises(MarketDataRejectedError) as error:
        service.get_analysis_batch(
            "XAUUSD",
            "M5",
            1,
            require_volume=True,
            checked_at=CHECKED_AT,
        )

    assert error.value.snapshot.analysis_eligible is True
    assert error.value.snapshot.volume_data_available is False
    assert (
        error.value.snapshot.validation.issues[0].code.value
        == "UNKNOWN_VOLUME_SOURCE"
    )


def test_unknown_volume_does_not_block_non_volume_analysis():
    candle = make_candle(
        CHECKED_AT - timedelta(minutes=5),
        volume_source=VolumeSource.UNKNOWN,
    )

    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=(candle,),
        received_at=CHECKED_AT,
        source="MT5",
    )

    service = MarketDataService(FakeMarketDataSource(batch))

    result = service.get_analysis_batch(
        "XAUUSD",
        "M5",
        1,
        require_volume=False,
        checked_at=CHECKED_AT,
    )

    assert result == batch


def test_rejected_snapshot_contains_original_batch():
    candle = make_candle(
        CHECKED_AT - timedelta(hours=2),
    )

    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=(candle,),
        received_at=CHECKED_AT,
        source="MT5",
    )

    service = MarketDataService(FakeMarketDataSource(batch))

    with pytest.raises(MarketDataRejectedError) as error:
        service.get_analysis_batch(
            "XAUUSD",
            "M5",
            1,
            checked_at=CHECKED_AT,
        )

    assert error.value.snapshot.batch == batch
    assert error.value.snapshot.validation.candle_count == 1


def test_requested_symbol_case_is_preserved():
    batch = make_batch(symbol="xauusd")
    service, source = make_service(batch)

    with pytest.raises(MarketDataContractError, match="Returned symbol"):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )

    assert source.last_request["symbol"] == "XAUUSD"


def test_unsupported_returned_timeframe_is_rejected_without_fallback():
    batch = make_batch()
    invalid_batch = batch.model_copy(
        update={"timeframe": "W1"},
    )

    service = MarketDataService(
        FakeMarketDataSource(invalid_batch)
    )

    with pytest.raises(
        MarketDataContractError,
        match="unsupported timeframe",
    ):
        service.get_snapshot(
            "XAUUSD",
            "M5",
            2,
            CHECKED_AT,
        )
