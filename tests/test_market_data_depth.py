from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backend.database import Database
from backend.database_models import (
    DatabaseBase,
    MarketCandleRecord,
    MarketDataBatchRecord,
)
from backend.market_data.data_depth import MarketDataDepthAuditor
from backend.market_data.repository import MarketDataRepository


UTC = timezone.utc


@pytest.fixture
def repository():
    database = Database("sqlite:///:memory:")
    database.initialize()
    DatabaseBase.metadata.create_all(database.engine)
    yield MarketDataRepository(database), database
    database.dispose()


def add_batch(session, *, timeframe, received_at, clock_verified, timestamps):
    batch = MarketDataBatchRecord(
        symbol="XAUUSD",
        timeframe=timeframe,
        source="MT5",
        received_at=received_at,
        candle_count=len(timestamps),
        created_at=received_at,
        clock_verified=clock_verified,
    )
    for timestamp in timestamps:
        batch.candles.append(
            MarketCandleRecord(
                symbol="XAUUSD",
                timeframe=timeframe,
                timestamp=timestamp,
                open_price=2000.0,
                high_price=2005.0,
                low_price=1995.0,
                close_price=2001.0,
                tick_volume=100,
                real_volume=None,
                volume_source="TICK",
                is_closed=True,
                quality="VERIFIED",
            )
        )
    session.add(batch)


def test_depth_audit_reports_empty_timeframes_without_claiming_readiness(
    repository,
):
    repo, _database = repository

    report = MarketDataDepthAuditor(repo).audit("XAUUSD")

    assert report.symbol == "XAUUSD"
    assert len(report.timeframes) == 7
    assert all(item.status == "NO_DATA" for item in report.timeframes)
    assert all(item.candle_count == 0 for item in report.timeframes)
    assert all(item.history_span_seconds is None for item in report.timeframes)


def test_depth_audit_reports_persisted_counts_ranges_and_clock_batches(
    repository,
):
    repo, database = repository
    first = datetime(2026, 1, 1, 10, 0, tzinfo=UTC)
    second = datetime(2026, 1, 1, 10, 5, tzinfo=UTC)
    received = datetime(2026, 1, 1, 10, 6, tzinfo=UTC)

    with database.session() as session:
        add_batch(
            session,
            timeframe="M5",
            received_at=received,
            clock_verified=True,
            timestamps=(first, second),
        )
        add_batch(
            session,
            timeframe="M15",
            received_at=received,
            clock_verified=False,
            timestamps=(first,),
        )

    report = MarketDataDepthAuditor(repo).audit("XAUUSD")
    by_timeframe = {item.timeframe: item for item in report.timeframes}

    assert by_timeframe["M5"].status == "AVAILABLE"
    assert by_timeframe["M5"].candle_count == 2
    assert by_timeframe["M5"].first_candle_at == first
    assert by_timeframe["M5"].last_candle_at == second
    assert by_timeframe["M5"].history_span_seconds == 300
    assert by_timeframe["M5"].batch_count == 1
    assert by_timeframe["M5"].clock_verified_batch_count == 1

    assert by_timeframe["M15"].candle_count == 1
    assert by_timeframe["M15"].clock_verified_batch_count == 0
    assert by_timeframe["M1"].status == "NO_DATA"


def test_depth_query_rejects_duplicate_or_invalid_timeframes(repository):
    repo, _database = repository

    with pytest.raises(ValueError):
        repo.get_data_depth("XAUUSD", ("M5", "m5"))

    with pytest.raises(ValueError):
        repo.get_data_depth("XAUUSD", ("W1",))

    with pytest.raises(ValueError):
        repo.get_data_depth("  ", ("M5",))
