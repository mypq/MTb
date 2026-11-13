
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

    def connection_status(self):
        return SimpleNamespace(model_dump=lambda mode: {})


class FakeMonitor:
    def __init__(
        self,
        adapter,
        interval_seconds,
        stop_result=True,
        stop_error=None,
    ):
        self.adapter = adapter
        self.interval_seconds = interval_seconds
        self.events = adapter.events
        self.stop_result = stop_result
        self.stop_error = stop_error

    @property
    def is_running(self):
        return False

    def start(self):
        self.events.append("monitor.start")

    def stop(self):
        self.events.append("monitor.stop")
        if self.stop_error is not None:
            raise self.stop_error
        return self.stop_result


class FakeMarketDataService:
    def __init__(self, source):
        self.source = source


class FakeChronyStatusService:
    def __init__(self):
        self.snapshot_called = False

    def snapshot(self):
        self.snapshot_called = True
        return SimpleNamespace(
            available=True,
            synchronized=True,
            reference_id="A29FC87B",
            stratum=4,
            leap_status="Normal",
            last_offset_seconds=0.0001,
            root_dispersion_seconds=0.002,
            selected_source="time.cloudflare.com",
            error=None,
        )


def configure_lifecycle(
    monkeypatch,
    *,
    mt5_enabled,
    connect_error=None,
    monitor_stop_result=True,
    monitor_stop_error=None,
):
    events = []
    settings = SimpleNamespace(MT5_ENABLED=mt5_enabled)

    def create_adapter(settings):
        return FakeAdapter(
            settings=settings,
            events=events,
            connect_error=connect_error,
        )

    def create_monitor(adapter, interval_seconds):
        return FakeMonitor(
            adapter=adapter,
            interval_seconds=interval_seconds,
            stop_result=monitor_stop_result,
            stop_error=monitor_stop_error,
        )

    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "MT5Adapter", create_adapter)
    monkeypatch.setattr(main, "MT5ConnectionMonitor", create_monitor)
    monkeypatch.setattr(
        main,
        "MarketDataService",
        FakeMarketDataService,
    )
    monkeypatch.setattr(
        main,
        "ChronyStatusService",
        FakeChronyStatusService,
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
            assert app.state.chrony_status_service is not None

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
    assert isinstance(
        app.state.chrony_status_service,
        FakeChronyStatusService,
    )


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
            assert app.state.chrony_status_service is not None

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


def test_lifespan_skips_adapter_shutdown_when_monitor_does_not_stop(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        monitor_stop_result=False,
    )

    async def run():
        async with main.lifespan(app):
            pass

    asyncio.run(run())

    assert events == [
        "adapter.connect",
        "monitor.start",
        "monitor.stop",
    ]


def test_lifespan_skips_adapter_shutdown_when_monitor_stop_raises(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        monitor_stop_error=RuntimeError("Simulated stop failure"),
    )

    async def run():
        async with main.lifespan(app):
            pass

    asyncio.run(run())

    assert events == [
        "adapter.connect",
        "monitor.start",
        "monitor.stop",
    ]


def test_chrony_service_is_independent_of_mt5_connection(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
    )

    async def run():
        async with main.lifespan(app):
            chrony = app.state.chrony_status_service
            status = chrony.snapshot()

            assert status.available is True
            assert status.synchronized is True
            assert status.selected_source == "time.cloudflare.com"
            assert status.stratum == 4
            assert status.error is None

    asyncio.run(run())

    assert events == [
        "monitor.stop",
        "adapter.shutdown",
    ]
