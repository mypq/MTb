
from datetime import datetime, timedelta, timezone

import pytest
from fastapi import HTTPException
from sqlalchemy import func, select

import backend.main as main
from backend.database import Database
from backend.database_models import (
    DatabaseBase,
    MarketCandleRecord,
    MarketDataBatchRecord,
)
from backend.market_data.models import (
    CandleBatch,
    CandleQuality,
    MarketCandle,
    VolumeSource,
)
from backend.market_data.repository import MarketDataRepository
from backend.market_data.service import MarketDataService


UTC = timezone.utc


class FakeMarketDataSource:
    def __init__(self, batch: CandleBatch):
        self.batch = batch
        self.calls = 0

    def get_closed_candles(
        self,
        symbol: str,
        timeframe: str,
        count: int,
    ) -> CandleBatch:
        self.calls += 1
        assert symbol == self.batch.symbol
        assert timeframe == self.batch.timeframe
        assert count == len(self.batch.candles)
        return self.batch


def make_recent_batch() -> CandleBatch:
    now = datetime.now(UTC)

    boundary = now.replace(
        minute=(now.minute // 5) * 5,
        second=0,
        microsecond=0,
    )

    latest_timestamp = boundary - timedelta(minutes=5)

    candles = tuple(
        MarketCandle(
            symbol="XAUUSD",
            timeframe="M5",
            timestamp=latest_timestamp - timedelta(minutes=5 * offset),
            open=2000.0,
            high=2005.0,
            low=1995.0,
            close=2002.0,
            tick_volume=100,
            real_volume=None,
            volume_source=VolumeSource.TICK,
            is_closed=True,
            quality=CandleQuality.VERIFIED,
        )
        for offset in (1, 0)
    )

    return CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=candles,
        received_at=boundary,
        source="INTEGRATION_TEST",
    )


@pytest.fixture
def integration_environment(tmp_path, monkeypatch):
    database_path = tmp_path / "market_data_integration.db"
    database_url = f"sqlite:///{database_path.as_posix()}"

    database = Database(database_url)
    database.initialize()
    DatabaseBase.metadata.create_all(database.engine)

    settings = type(
        "TestSettings",
        (),
        {
            "APP_NAME": "GOLD_BOT_TEST",
            "DEFAULT_TIMEFRAME": "M5",
            "MT5_SYMBOL": "XAUUSD",
        },
    )()

    monkeypatch.setattr(main, "get_settings", lambda: settings)

    application = main.create_app()

    batch = make_recent_batch()
    source = FakeMarketDataSource(batch)

    application.state.market_data_service = MarketDataService(
        source=source,
    )
    application.state.market_data_repository = MarketDataRepository(
        database,
    )

    route = next(
        route
        for route in application.routes
        if getattr(route, "path", None)
        == "/api/v1/market-data/candles"
    )

    yield {
        "endpoint": route.endpoint,
        "database": database,
        "source": source,
        "batch": batch,
    }

    database.dispose()


def read_database_counts(database: Database) -> tuple[int, int]:
    with database.session() as session:
        batch_count = session.scalar(
            select(func.count()).select_from(MarketDataBatchRecord)
        )
        candle_count = session.scalar(
            select(func.count()).select_from(MarketCandleRecord)
        )

    return batch_count, candle_count


def test_api_endpoint_persists_real_market_data_to_sqlite(
    integration_environment,
):
    endpoint = integration_environment["endpoint"]
    database = integration_environment["database"]
    batch = integration_environment["batch"]

    result = endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=2,
    )

    assert result["batch"]["symbol"] == "XAUUSD"
    assert result["batch"]["timeframe"] == "M5"
    assert len(result["batch"]["candles"]) == 2
    assert result["validation"]["analysis_eligible"] is True

    assert read_database_counts(database) == (1, 2)

    with database.session() as session:
        records = session.scalars(
            select(MarketCandleRecord)
            .where(
                MarketCandleRecord.symbol == "XAUUSD",
                MarketCandleRecord.timeframe == "M5",
            )
            .order_by(MarketCandleRecord.timestamp)
        ).all()

        assert len(records) == 2

        for record, candle in zip(records, batch.candles):
            assert record.symbol == candle.symbol
            assert record.timeframe == candle.timeframe
            assert record.open_price == candle.open
            assert record.high_price == candle.high
            assert record.low_price == candle.low
            assert record.close_price == candle.close
            assert record.tick_volume == candle.tick_volume
            assert record.is_closed is True


def test_repeated_api_request_does_not_duplicate_persisted_candles(
    integration_environment,
):
    endpoint = integration_environment["endpoint"]
    database = integration_environment["database"]
    source = integration_environment["source"]

    first_result = endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=2,
    )

    second_result = endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=2,
    )

    assert first_result["validation"]["analysis_eligible"] is True
    assert second_result["validation"]["analysis_eligible"] is True
    assert source.calls == 2

    assert read_database_counts(database) == (1, 2)


def test_persisted_market_data_survives_session_reopening(
    integration_environment,
):
    endpoint = integration_environment["endpoint"]
    database = integration_environment["database"]

    endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=2,
    )

    with database.session() as session:
        records = session.scalars(
            select(MarketCandleRecord)
            .where(MarketCandleRecord.symbol == "XAUUSD")
            .order_by(MarketCandleRecord.timestamp)
        ).all()

        assert len(records) == 2
        assert all(record.symbol == "XAUUSD" for record in records)
        assert all(record.timeframe == "M5" for record in records)


def test_ineligible_market_data_is_not_persisted(
    integration_environment,
):
    endpoint = integration_environment["endpoint"]
    database = integration_environment["database"]
    source = integration_environment["source"]

    old_candles = tuple(
        candle.model_copy(
            update={
                "timestamp": candle.timestamp - timedelta(hours=2),
            }
        )
        for candle in source.batch.candles
    )

    source.batch = source.batch.model_copy(
        update={"candles": old_candles}
    )

    result = endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=2,
    )

    assert result["validation"]["analysis_eligible"] is False
    assert read_database_counts(database) == (0, 0)


def test_conflicting_candle_returns_http_409_and_preserves_original(
    integration_environment,
):
    endpoint = integration_environment["endpoint"]
    database = integration_environment["database"]
    source = integration_environment["source"]

    first_result = endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=2,
    )

    assert first_result["validation"]["analysis_eligible"] is True
    assert read_database_counts(database) == (1, 2)

    original_close = source.batch.candles[-1].close

    conflicting_candles = (
        source.batch.candles[0],
        source.batch.candles[1].model_copy(
            update={"close": original_close + 10.0}
        ),
    )

    source.batch = source.batch.model_copy(
        update={"candles": conflicting_candles}
    )

    with pytest.raises(HTTPException) as exc_info:
        endpoint(
            symbol="XAUUSD",
            timeframe="M5",
            count=2,
        )

    assert exc_info.value.status_code == 409
    assert read_database_counts(database) == (1, 2)

    with database.session() as session:
        records = session.scalars(
            select(MarketCandleRecord)
            .where(MarketCandleRecord.symbol == "XAUUSD")
            .order_by(MarketCandleRecord.timestamp)
        ).all()

        assert len(records) == 2
        assert records[-1].close_price == original_close
def test_persistence_preserves_complete_candle_and_batch_metadata(
    integration_environment,
):
    endpoint = integration_environment["endpoint"]
    database = integration_environment["database"]
    batch = integration_environment["batch"]

    endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=2,
    )

    with database.session() as session:
        batch_record = session.scalar(
            select(MarketDataBatchRecord)
        )

        records = session.scalars(
            select(MarketCandleRecord)
            .where(
                MarketCandleRecord.symbol == "XAUUSD",
                MarketCandleRecord.timeframe == "M5",
            )
            .order_by(MarketCandleRecord.timestamp)
        ).all()

        assert batch_record is not None
        assert batch_record.symbol == batch.symbol
        assert batch_record.timeframe == batch.timeframe
        assert batch_record.source == batch.source
        assert batch_record.candle_count == len(batch.candles)
        assert batch_record.created_at is not None

        assert len(records) == len(batch.candles)

        for record, candle in zip(records, batch.candles):
            assert record.symbol == candle.symbol
            assert record.timeframe == candle.timeframe

            assert (
                record.timestamp.replace(tzinfo=UTC)
                == candle.timestamp
            )

            assert record.open_price == candle.open
            assert record.high_price == candle.high
            assert record.low_price == candle.low
            assert record.close_price == candle.close

            assert record.tick_volume == candle.tick_volume
            assert record.real_volume == candle.real_volume
            assert record.volume_source == candle.volume_source.value

            assert record.is_closed == candle.is_closed
            assert record.quality == candle.quality.value
