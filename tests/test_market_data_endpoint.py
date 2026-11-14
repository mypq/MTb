
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

import backend.main as main
from backend.market_data.repository import MarketDataPersistenceConflict


class FakeMarketDataService:
    def __init__(self, snapshot=None, error=None):
        self.snapshot = snapshot
        self.error = error
        self.calls = []

    def get_snapshot(self, **kwargs):
        self.calls.append(kwargs)

        if self.error is not None:
            raise self.error

        return self.snapshot


class FakeMarketDataRepository:
    def __init__(self, error=None):
        self.error = error
        self.saved = []

    def save_snapshot(self, snapshot):
        if self.error is not None:
            raise self.error

        self.saved.append(snapshot)


class FakeSnapshot:
    def __init__(self, eligible):
        self.analysis_eligible = eligible

    def model_dump(self, mode):
        return {
            "analysis_eligible": self.analysis_eligible,
        }


def create_endpoint(monkeypatch, snapshot=None, service_error=None,
                    repository_error=None):
    settings = SimpleNamespace(
        APP_NAME="GOLD_BOT",
        DEFAULT_TIMEFRAME="M5",
        MT5_SYMBOL="XAUUSD",
    )

    monkeypatch.setattr(main, "get_settings", lambda: settings)

    application = main.create_app()
    service = FakeMarketDataService(
        snapshot=snapshot,
        error=service_error,
    )
    repository = FakeMarketDataRepository(
        error=repository_error,
    )

    application.state.market_data_service = service
    application.state.market_data_repository = repository

    route = next(
        route
        for route in application.routes
        if getattr(route, "path", None)
        == "/api/v1/market-data/candles"
    )

    return route.endpoint, service, repository


def test_eligible_snapshot_is_persisted(monkeypatch):
    snapshot = FakeSnapshot(eligible=True)

    endpoint, service, repository = create_endpoint(
        monkeypatch,
        snapshot=snapshot,
    )

    result = endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=100,
    )

    assert result == {"analysis_eligible": True}
    assert repository.saved == [snapshot]
    assert service.calls == [
        {
            "symbol": "XAUUSD",
            "timeframe": "M5",
            "count": 100,
        }
    ]


def test_ineligible_snapshot_is_returned_without_persistence(
    monkeypatch,
):
    snapshot = FakeSnapshot(eligible=False)

    endpoint, service, repository = create_endpoint(
        monkeypatch,
        snapshot=snapshot,
    )

    result = endpoint(
        symbol="XAUUSD",
        timeframe="M5",
        count=100,
    )

    assert result == {"analysis_eligible": False}
    assert repository.saved == []


def test_persistence_conflict_returns_http_409(monkeypatch):
    snapshot = FakeSnapshot(eligible=True)

    endpoint, service, repository = create_endpoint(
        monkeypatch,
        snapshot=snapshot,
        repository_error=MarketDataPersistenceConflict(
            "Conflicting candle"
        ),
    )

    with pytest.raises(HTTPException) as exc_info:
        endpoint(
            symbol="XAUUSD",
            timeframe="M5",
            count=100,
        )

    assert exc_info.value.status_code == 409
    assert repository.saved == []


def test_empty_symbol_returns_http_400(monkeypatch):
    endpoint, service, repository = create_endpoint(
        monkeypatch,
        snapshot=FakeSnapshot(eligible=True),
    )

    with pytest.raises(HTTPException) as exc_info:
        endpoint(
            symbol="",
            timeframe="M5",
            count=100,
        )

    assert exc_info.value.status_code == 400
    assert service.calls == []
    assert repository.saved == []


def test_market_data_value_error_returns_http_400(monkeypatch):
    endpoint, service, repository = create_endpoint(
        monkeypatch,
        service_error=ValueError("Invalid timeframe"),
    )

    with pytest.raises(HTTPException) as exc_info:
        endpoint(
            symbol="XAUUSD",
            timeframe="INVALID",
            count=100,
        )

    assert exc_info.value.status_code == 400
    assert repository.saved == []
