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


class FakeDatabase:
    def __init__(self, database_url, events, initialize_error=None):
        self.database_url = database_url
        self.events = events
        self.initialize_error = initialize_error

    def initialize(self):
        self.events.append("database.initialize")
        if self.initialize_error is not None:
            raise self.initialize_error

    def dispose(self):
        self.events.append("database.dispose")


class FakeBackupService:
    def __init__(self, database, events):
        self.database = database
        self.events = events
        self.events.append("backup_service.initialize")

    def create_backup(self):
        self.events.append("backup.create")


class FakeMarketDataService:
    def __init__(self, source):
        self.source = source


class FakeMarketDataRepository:
    def __init__(self, database):
        self.database = database


class FakeMarketDataCollector:
    def __init__(
        self,
        service,
        repository,
        *,
        symbol,
        timeframe,
        count,
    ):
        self.service = service
        self.repository = repository
        self.symbol = symbol
        self.timeframe = timeframe
        self.count = count
        self.interval_seconds = 300.0
        self.events = []

    async def run(self, stop_event):
        self.events.append("collector.start")
        await stop_event.wait()
        self.events.append("collector.stop")


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
            healthy_sources=3,
            nts_authenticated_sources=3,
            quorum_met=True,
            error=None,
        )


def configure_lifecycle(
    monkeypatch,
    *,
    mt5_enabled,
    connect_error=None,
    monitor_stop_result=True,
    monitor_stop_error=None,
    initialize_error=None,
    migration_error=None,
):
    events = []

    settings = SimpleNamespace(
        MT5_ENABLED=mt5_enabled,
        MT5_SYMBOL="XAUUSD",
        DEFAULT_TIMEFRAME="M5",
        DATABASE_URL="sqlite:///./data/test.db",
    )

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

    def create_database(database_url):
        return FakeDatabase(
            database_url=database_url,
            events=events,
            initialize_error=initialize_error,
        )

    def create_backup_service(database):
        return FakeBackupService(database, events)

    async def fake_backup_loop(backup_service, stop_event):
        events.append("backup.start")
        await stop_event.wait()
        events.append("backup.stop")

    def apply_migrations():
        events.append("database.migrations")
        if migration_error is not None:
            raise migration_error

    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "MT5Adapter", create_adapter)
    monkeypatch.setattr(main, "MT5ConnectionMonitor", create_monitor)
    monkeypatch.setattr(main, "Database", create_database)
    monkeypatch.setattr(
        main,
        "DatabaseBackupService",
        create_backup_service,
    )
    monkeypatch.setattr(
        main,
        "_database_backup_loop",
        fake_backup_loop,
    )
    monkeypatch.setattr(main, "upgrade_database", apply_migrations)

    monkeypatch.setattr(
        main,
        "MarketDataService",
        FakeMarketDataService,
    )
    monkeypatch.setattr(
        main,
        "MarketDataRepository",
        FakeMarketDataRepository,
    )
    monkeypatch.setattr(
        main,
        "MarketDataCollector",
        FakeMarketDataCollector,
    )
    monkeypatch.setattr(
        main,
        "ChronyStatusService",
        FakeChronyStatusService,
    )

    app = SimpleNamespace(state=SimpleNamespace())
    return app, events


def test_lifespan_starts_backup_after_migrations_and_stops_before_database(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    async def run():
        async with main.lifespan(app):
            assert events == [
                "database.initialize",
                "database.migrations",
                "backup_service.initialize",
                "adapter.connect",
                "monitor.start",
            ]

            assert app.state.database_backup_service is not None
            assert app.state.database is not None
            assert app.state.market_data_repository is not None
            assert (
                app.state.market_data_repository.database
                is app.state.database
            )

    asyncio.run(run())

    assert events == [
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "adapter.connect",
        "monitor.start",
        "backup.start",
        "backup.stop",
        "monitor.stop",
        "adapter.shutdown",
        "database.dispose",
    ]


def test_lifespan_starts_backup_when_mt5_is_disabled(monkeypatch):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
    )

    async def run():
        async with main.lifespan(app):
            assert events == [
                "database.initialize",
                "database.migrations",
                "backup_service.initialize",
            ]

            assert app.state.database_backup_service is not None

    asyncio.run(run())

    assert events == [
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "backup.start",
        "backup.stop",
        "monitor.stop",
        "adapter.shutdown",
        "database.dispose",
    ]


def test_lifespan_stops_backup_when_initial_connection_raises(
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
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "adapter.connect",
        "backup.start",
        "backup.stop",
        "monitor.stop",
        "adapter.shutdown",
        "database.dispose",
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
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "adapter.connect",
        "monitor.start",
        "backup.start",
        "backup.stop",
        "monitor.stop",
        "database.dispose",
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
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "adapter.connect",
        "monitor.start",
        "backup.start",
        "backup.stop",
        "monitor.stop",
        "database.dispose",
    ]


def test_database_is_disposed_when_migrations_fail(monkeypatch):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        migration_error=RuntimeError("Simulated migration failure"),
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Simulated migration failure",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert events == [
        "database.initialize",
        "database.migrations",
        "monitor.stop",
        "adapter.shutdown",
        "database.dispose",
    ]


def test_database_is_disposed_when_initialization_fails(monkeypatch):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        initialize_error=RuntimeError("Simulated database failure"),
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Simulated database failure",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert events == [
        "database.initialize",
        "monitor.stop",
        "adapter.shutdown",
        "database.dispose",
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
            assert status.quorum_met is True
            assert status.error is None

    asyncio.run(run())

    assert events == [
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "backup.start",
        "backup.stop",
        "monitor.stop",
        "adapter.shutdown",
        "database.dispose",
    ]


def test_database_backup_loop_creates_backup_and_stops(monkeypatch):
    calls = []
    stop_event = asyncio.Event()

    class BackupService:
        def create_backup(self):
            calls.append("backup.created")

    async def fake_to_thread(function):
        function()
        stop_event.set()

    monkeypatch.setattr(
        main.asyncio,
        "to_thread",
        fake_to_thread,
    )

    asyncio.run(
        main._database_backup_loop(
            BackupService(),
            stop_event,
        )
    )

    assert calls == ["backup.created"]
