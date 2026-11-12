
import asyncio
from types import SimpleNamespace

import pytest

import backend.main as main


class FakeAdapter:
    def __init__(self, settings, events, connect_error=None):
        self.settings = settings
        self.events = events
        self.connect_error = connect_error

    def connect(self):
        self.events.append("adapter.connect")
        if self.connect_error is not None:
            raise self.connect_error

    def shutdown(self):
        self.events.append("adapter.shutdown")


class FakeMonitor:
    def __init__(self, adapter, interval_seconds):
        self.adapter = adapter
        self.interval_seconds = interval_seconds
        self.events = adapter.events

    @property
    def is_running(self):
        return False

    def start(self):
        self.events.append("monitor.start")

    def stop(self):
        self.events.append("monitor.stop")


class FakeMarketDataService:
    def __init__(self, source):
        self.source = source


def configure_lifecycle(monkeypatch, *, mt5_enabled, connect_error=None):
    events = []
    settings = SimpleNamespace(MT5_ENABLED=mt5_enabled)

    def create_adapter(settings):
        return FakeAdapter(
            settings=settings,
            events=events,
            connect_error=connect_error,
        )

    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "MT5Adapter", create_adapter)
    monkeypatch.setattr(main, "MT5ConnectionMonitor", FakeMonitor)
    monkeypatch.setattr(
        main,
        "MarketDataService",
        FakeMarketDataService,
    )

    app = SimpleNamespace(state=SimpleNamespace())
    return app, events


def test_lifespan_starts_monitor_and_stops_it_before_adapter_shutdown(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    async def run():
        async with main.lifespan(app):
            assert events == [
                "adapter.connect",
                "monitor.start",
            ]

    asyncio.run(run())

    assert events == [
        "adapter.connect",
        "monitor.start",
        "monitor.stop",
        "adapter.shutdown",
    ]
    assert app.state.mt5_adapter is not None
    assert app.state.mt5_monitor is not None
    assert app.state.market_data_service.source is app.state.mt5_adapter


def test_lifespan_does_not_start_monitor_when_mt5_is_disabled(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
    )

    async def run():
        async with main.lifespan(app):
            assert events == []

    asyncio.run(run())

    assert events == [
        "monitor.stop",
        "adapter.shutdown",
    ]


def test_lifespan_cleans_up_when_initial_connection_raises(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        connect_error=RuntimeError("Simulated connection failure"),
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Simulated connection failure",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert events == [
        "adapter.connect",
        "monitor.stop",
        "adapter.shutdown",
    ]
