
import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from backend.market_data.collector import (
    MarketDataCollector,
    next_collection_time,
)
from backend.market_data.repository import MarketDataSaveResult


class FakeMarketDataService:
    def __init__(self, snapshot=None, error=None):
        self.snapshot = snapshot
        self.error = error
        self.requests = []

    def get_snapshot(self, symbol, timeframe, count):
        self.requests.append(
            {
                "symbol": symbol,
                "timeframe": timeframe,
                "count": count,
            }
        )

        if self.error is not None:
            raise self.error

        return self.snapshot


class FakeMarketDataRepository:
    def __init__(self, result=None, error=None):
        self.result = result or MarketDataSaveResult(
            batch_id=1,
            inserted_candles=2,
            duplicate_candles=0,
        )
        self.error = error
        self.saved_snapshots = []

    def save_snapshot(self, snapshot):
        self.saved_snapshots.append(snapshot)

        if self.error is not None:
            raise self.error

        return self.result


def make_snapshot(*, eligible=True):
    return SimpleNamespace(
        analysis_eligible=eligible,
        validation=SimpleNamespace(
            issues=[
                SimpleNamespace(
                    code=SimpleNamespace(value="TIME_GAP")
                )
            ]
        ),
    )


def make_collector(
    *,
    snapshot=None,
    service_error=None,
    repository_error=None,
    interval_seconds=None,
):
    service = FakeMarketDataService(
        snapshot=snapshot or make_snapshot(),
        error=service_error,
    )
    repository = FakeMarketDataRepository(
        error=repository_error,
    )

    collector = MarketDataCollector(
        service,
        repository,
        symbol=" XAUUSD ",
        timeframe="m5",
        count=300,
        interval_seconds=interval_seconds,
    )

    return collector, service, repository


def test_collector_normalizes_symbol_and_timeframe():
    collector, service, repository = make_collector()

    assert collector._symbol == "XAUUSD"
    assert collector._timeframe == "M5"
    assert collector.interval_seconds == 300.0


def test_collector_uses_explicit_interval():
    collector, service, repository = make_collector(
        interval_seconds=45,
    )

    assert collector.interval_seconds == 45.0


@pytest.mark.parametrize(
    "symbol",
    ["", "   ", None, 123, True],
)
def test_collector_rejects_invalid_symbol(symbol):
    with pytest.raises(ValueError, match="Symbol"):
        MarketDataCollector(
            FakeMarketDataService(make_snapshot()),
            FakeMarketDataRepository(),
            symbol=symbol,
            timeframe="M5",
        )


@pytest.mark.parametrize(
    ("count", "error"),
    [
        (True, TypeError),
        (False, TypeError),
        (0, ValueError),
        (-1, ValueError),
        (10001, ValueError),
        (1.5, TypeError),
    ],
)
def test_collector_rejects_invalid_count(count, error):
    with pytest.raises(error):
        MarketDataCollector(
            FakeMarketDataService(make_snapshot()),
            FakeMarketDataRepository(),
            symbol="XAUUSD",
            timeframe="M5",
            count=count,
        )


@pytest.mark.parametrize(
    "timeframe",
    ["", "   ", "W1", None, 123],
)
def test_collector_rejects_unsupported_timeframe(timeframe):
    with pytest.raises((TypeError, ValueError)):
        MarketDataCollector(
            FakeMarketDataService(make_snapshot()),
            FakeMarketDataRepository(),
            symbol="XAUUSD",
            timeframe=timeframe,
        )


@pytest.mark.parametrize(
    "interval",
    [
        0,
        -1,
        True,
        False,
        float("inf"),
        float("-inf"),
        float("nan"),
        "300",
        None,
    ],
)
def test_collector_rejects_invalid_explicit_interval(interval):
    if interval is None:
        return

    with pytest.raises(ValueError):
        MarketDataCollector(
            FakeMarketDataService(make_snapshot()),
            FakeMarketDataRepository(),
            symbol="XAUUSD",
            timeframe="M5",
            interval_seconds=interval,
        )


def test_collect_once_requests_configured_market_data():
    collector, service, repository = make_collector()

    collector.collect_once()

    assert service.requests == [
        {
            "symbol": "XAUUSD",
            "timeframe": "M5",
            "count": 300,
        }
    ]


def test_collect_once_persists_eligible_snapshot():
    snapshot = make_snapshot(eligible=True)
    collector, service, repository = make_collector(
        snapshot=snapshot,
    )

    result = collector.collect_once()

    assert result == repository.result
    assert repository.saved_snapshots == [snapshot]


def test_collect_once_does_not_persist_ineligible_snapshot():
    snapshot = make_snapshot(eligible=False)
    collector, service, repository = make_collector(
        snapshot=snapshot,
    )

    result = collector.collect_once()

    assert result is None
    assert repository.saved_snapshots == []


def test_collect_once_propagates_market_data_errors():
    collector, service, repository = make_collector(
        service_error=RuntimeError("Market data source failed"),
    )

    with pytest.raises(
        RuntimeError,
        match="Market data source failed",
    ):
        collector.collect_once()

    assert repository.saved_snapshots == []


def test_collect_once_propagates_persistence_errors():
    collector, service, repository = make_collector(
        repository_error=RuntimeError("Database write failed"),
    )

    with pytest.raises(
        RuntimeError,
        match="Database write failed",
    ):
        collector.collect_once()

    assert len(repository.saved_snapshots) == 1


def test_collector_run_requires_asyncio_event():
    collector, service, repository = make_collector()

    with pytest.raises(TypeError, match="asyncio.Event"):
        asyncio.run(collector.run(None))


def test_collector_run_collects_and_stops(monkeypatch):
    collector, service, repository = make_collector()
    stop_event = asyncio.Event()
    calls = []

    async def fake_to_thread(function, *args, **kwargs):
        calls.append(function)
        result = function(*args, **kwargs)
        stop_event.set()
        return result

    monkeypatch.setattr(
        "backend.market_data.collector.asyncio.to_thread",
        fake_to_thread,
    )

    asyncio.run(collector.run(stop_event))

    assert calls == [collector.collect_once]
    assert len(repository.saved_snapshots) == 1


def test_collector_run_does_not_collect_when_already_stopped():
    collector, service, repository = make_collector()
    stop_event = asyncio.Event()
    stop_event.set()

    asyncio.run(collector.run(stop_event))

    assert service.requests == []
    assert repository.saved_snapshots == []


def test_next_collection_time_aligns_with_next_m5_close():
    now = datetime(
        2026, 11, 15, 10, 2, 30,
        tzinfo=timezone.utc,
    )

    result = next_collection_time(
        timeframe_seconds=300,
        now=now,
    )

    assert result == datetime(
        2026, 11, 15, 10, 5, 2,
        tzinfo=timezone.utc,
    )


def test_next_collection_time_moves_to_following_close_at_boundary():
    now = datetime(
        2026, 11, 15, 10, 5, 0,
        tzinfo=timezone.utc,
    )

    result = next_collection_time(
        timeframe_seconds=300,
        now=now,
    )

    assert result == datetime(
        2026, 11, 15, 10, 10, 2,
        tzinfo=timezone.utc,
    )


def test_next_collection_time_supports_zero_delay():
    now = datetime(
        2026, 11, 15, 10, 2, 30,
        tzinfo=timezone.utc,
    )

    result = next_collection_time(
        timeframe_seconds=300,
        now=now,
        delay_seconds=0,
    )

    assert result == datetime(
        2026, 11, 15, 10, 5, 0,
        tzinfo=timezone.utc,
    )


def test_next_collection_time_rejects_naive_datetime():
    now = datetime(2026, 11, 15, 10, 2, 30)

    with pytest.raises(ValueError, match="timezone-aware"):
        next_collection_time(
            timeframe_seconds=300,
            now=now,
        )


@pytest.mark.parametrize(
    "timeframe_seconds",
    [0, -1, True, False, 300.5],
)
def test_next_collection_time_rejects_invalid_timeframe(
    timeframe_seconds,
):
    with pytest.raises(ValueError, match="positive integer"):
        next_collection_time(
            timeframe_seconds=timeframe_seconds,
        )


@pytest.mark.parametrize(
    "delay_seconds",
    [-1, True, False, float("inf"), float("nan"), "2"],
)
def test_next_collection_time_rejects_invalid_delay(delay_seconds):
    with pytest.raises(ValueError, match="delay_seconds"):
        next_collection_time(
            timeframe_seconds=300,
            delay_seconds=delay_seconds,
        )
