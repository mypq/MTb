from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from backend.database_models import (
    DatabaseBase,
    MarketCandleRecord,
    MarketDataBatchRecord,
)


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")
    DatabaseBase.metadata.create_all(engine)

    with Session(engine) as db_session:
        yield db_session

    engine.dispose()


def make_batch() -> MarketDataBatchRecord:
    now = datetime.now(timezone.utc)
    return MarketDataBatchRecord(
        symbol="XAUUSD",
        timeframe="M5",
        source="TEST",
        received_at=now,
        candle_count=1,
        created_at=now,
    )


def make_candle(batch: MarketDataBatchRecord) -> MarketCandleRecord:
    return MarketCandleRecord(
        batch=batch,
        symbol="XAUUSD",
        timeframe="M5",
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        open_price=2000.0,
        high_price=2010.0,
        low_price=1990.0,
        close_price=2005.0,
        tick_volume=100,
        real_volume=None,
        volume_source="TICK",
        is_closed=True,
        quality="VERIFIED",
    )


def test_database_models_create_expected_tables(session):
    assert set(DatabaseBase.metadata.tables) == {
        "market_data_batches",
        "market_candles",
    }


def test_batch_and_candle_relationship_persist(session):
    batch = make_batch()
    candle = make_candle(batch)

    session.add(batch)
    session.commit()

    assert batch.id is not None
    assert candle.id is not None
    assert len(batch.candles) == 1
    assert batch.candles[0].symbol == "XAUUSD"


def test_duplicate_candle_identity_is_rejected(session):
    batch = make_batch()
    session.add(batch)
    session.flush()

    session.add(make_candle(batch))
    session.commit()

    duplicate_batch = make_batch()
    session.add(duplicate_batch)
    session.flush()
    session.add(make_candle(duplicate_batch))

    with pytest.raises(IntegrityError):
        session.commit()

    session.rollback()


def test_invalid_ohlc_relationship_is_rejected_by_database(session):
    batch = make_batch()
    session.add(batch)
    session.flush()

    invalid_candle = make_candle(batch)
    invalid_candle.high_price = 1995.0
    session.add(invalid_candle)

    with pytest.raises(IntegrityError):
        session.commit()

    session.rollback()


def test_negative_volume_is_rejected_by_database(session):
    batch = make_batch()
    session.add(batch)
    session.flush()

    invalid_candle = make_candle(batch)
    invalid_candle.tick_volume = -1
    session.add(invalid_candle)

    with pytest.raises(IntegrityError):
        session.commit()

    session.rollback()
