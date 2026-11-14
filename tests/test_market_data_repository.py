
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

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
from backend.market_data.repository import (
    MarketDataPersistenceConflict,
    MarketDataPersistenceError,
    MarketDataRepository,
)
from backend.market_data.service import MarketDataService


UTC = timezone.utc
CHECKED_AT = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


class FakeMarketDataSource:
    def __init__(self, batch):
        self.batch = batch

    def get_closed_candles(self, symbol, timeframe, count):
        return self.batch


class ConcurrentInsertDatabase:
    """Simulates another request winning the insert race."""

    def __init__(self, database, snapshot):
        self.database = database
        self.snapshot = snapshot
        self.calls = 0

    @contextmanager
    def session(self):
        self.calls += 1

        if self.calls == 1:
            MarketDataRepository(self.database).save_snapshot(
                self.snapshot
            )
            raise IntegrityError(
                "Simulated concurrent unique constraint conflict",
                {},
                Exception("duplicate key"),
            )

        with self.database.session() as session:
            yield session


def make_snapshot(
    *,
    count=1,
    close=2002.0,
    stale=False,
):
    candles = []

    for index in range(count):
        timestamp = CHECKED_AT - timedelta(
            minutes=5 * (count - index)
        )

        if stale:
            timestamp -= timedelta(hours=2)

        candles.append(
            MarketCandle(
                symbol="XAUUSD",
                timeframe="M5",
                timestamp=timestamp,
                open=2000.0,
                high=2005.0,
                low=1995.0,
                close=close,
                tick_volume=100,
                real_volume=None,
                volume_source=VolumeSource.TICK,
                is_closed=True,
                quality=CandleQuality.VERIFIED,
            )
        )

    batch = CandleBatch(
        symbol="XAUUSD",
        timeframe="M5",
        candles=tuple(candles),
        received_at=CHECKED_AT,
        source="MT5",
    )

    service = MarketDataService(FakeMarketDataSource(batch))

    return service.get_snapshot(
        symbol="XAUUSD",
        timeframe="M5",
        count=count,
        checked_at=CHECKED_AT,
    )


@pytest.fixture
def repository():
    database = Database("sqlite:///:memory:")
    database.initialize()
    DatabaseBase.metadata.create_all(database.engine)

    yield MarketDataRepository(database), database

    database.dispose()


def database_counts(database):
    with database.session() as session:
        batches = session.scalar(
            select(func.count()).select_from(MarketDataBatchRecord)
        )
        candles = session.scalar(
            select(func.count()).select_from(MarketCandleRecord)
        )

    return batches, candles


def test_repository_persists_validated_snapshot(repository):
    repo, database = repository

    result = repo.save_snapshot(make_snapshot(count=2))

    assert result.batch_id is not None
    assert result.inserted_candles == 2
    assert result.duplicate_candles == 0

    assert database_counts(database) == (1, 2)


def test_exact_duplicate_snapshot_is_idempotent(repository):
    repo, database = repository
    snapshot = make_snapshot(count=2)

    first = repo.save_snapshot(snapshot)
    second = repo.save_snapshot(snapshot)

    assert first.inserted_candles == 2
    assert second.batch_id is None
    assert second.inserted_candles == 0
    assert second.duplicate_candles == 2

    assert database_counts(database) == (1, 2)


def test_partial_duplicates_only_insert_new_candles(repository):
    repo, database = repository

    first = repo.save_snapshot(make_snapshot(count=1))
    second = repo.save_snapshot(make_snapshot(count=2))

    assert first.inserted_candles == 1
    assert second.inserted_candles == 1
    assert second.duplicate_candles == 1

    assert database_counts(database) == (2, 2)


def test_conflicting_existing_candle_is_rejected(repository):
    repo, database = repository

    repo.save_snapshot(make_snapshot(count=1))

    with pytest.raises(MarketDataPersistenceConflict):
        repo.save_snapshot(
            make_snapshot(count=1, close=2003.0)
        )

    assert database_counts(database) == (1, 1)


def test_ineligible_snapshot_is_rejected(repository):
    repo, database = repository
    snapshot = make_snapshot(count=1, stale=True)

    assert snapshot.analysis_eligible is False

    with pytest.raises(MarketDataPersistenceError):
        repo.save_snapshot(snapshot)

    assert database_counts(database) == (0, 0)


def test_batch_candle_count_matches_inserted_records(repository):
    repo, database = repository

    result = repo.save_snapshot(make_snapshot(count=3))

    with database.session() as session:
        batch = session.get(
            MarketDataBatchRecord,
            result.batch_id,
        )

        assert batch is not None
        assert batch.candle_count == 3
        assert len(batch.candles) == 3


def test_integrity_error_retries_and_recognizes_concurrent_insert(
    repository,
):
    repo, database = repository
    snapshot = make_snapshot(count=2)

    simulated_database = ConcurrentInsertDatabase(
        database,
        snapshot,
    )
    repo._database = simulated_database

    result = repo.save_snapshot(snapshot)

    assert simulated_database.calls == 2
    assert result.batch_id is None
    assert result.inserted_candles == 0
    assert result.duplicate_candles == 2

    assert database_counts(database) == (1, 2)


def test_invalid_snapshot_argument_is_rejected(repository):
    repo, database = repository

    with pytest.raises(TypeError):
        repo.save_snapshot(None)

    assert database_counts(database) == (0, 0)
def test_conflicting_candle_rolls_back_new_candles_in_same_batch(
    repository,
):
    repo, database = repository

    # Store one candle first.
    first_result = repo.save_snapshot(
        make_snapshot(count=1)
    )

    assert first_result.inserted_candles == 1
    assert database_counts(database) == (1, 1)

    # This batch contains:
    # 1. A new older candle.
    # 2. An existing candle with conflicting close price.
    conflicting_snapshot = make_snapshot(
        count=2,
        close=2003.0,
    )

    with pytest.raises(MarketDataPersistenceConflict):
        repo.save_snapshot(conflicting_snapshot)

    # Nothing from the conflicting batch must be persisted.
    assert database_counts(database) == (1, 1)
