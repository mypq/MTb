import asyncio
import json
from datetime import datetime, timedelta, timezone
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


class FakeBrokerTickMonitor:
    def __init__(self, stale_after_seconds):
        self.stale_after_seconds = stale_after_seconds


class FakeBrokerTickWorker:
    def __init__(
        self,
        adapter,
        monitor,
        *,
        symbol,
        interval_seconds,
        lifecycle_order,
        stop_result=True,
        stop_error=None,
    ):
        self.adapter = adapter
        self.monitor = monitor
        self.symbol = symbol
        self.interval_seconds = interval_seconds
        self.lifecycle_order = lifecycle_order
        self.stop_result = stop_result
        self.stop_error = stop_error
        self.started = False
        self.stop_calls = 0

    def start(self):
        self.started = True
        self.lifecycle_order.append("broker_tick.start")

    def stop(self):
        self.stop_calls += 1
        self.lifecycle_order.append("broker_tick.stop")

        if self.stop_error is not None:
            raise self.stop_error

        return self.stop_result

    def status(self):
        return {
            "running": self.started,
            "state": "NO_DATA",
            "clock_verification": "UNVERIFIED",
        }


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
    def __init__(self, source, validator=None):
        self.source = source
        self.validator = validator


class FakeMarketDataValidator:
    def __init__(self, session_calendar=None):
        self.session_calendar = session_calendar


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
    mt5_symbol="XAUUSD",
    connect_error=None,
    monitor_stop_result=True,
    monitor_stop_error=None,
    worker_stop_result=True,
    worker_stop_error=None,
    initialize_error=None,
    migration_error=None,
    sessions_file=None,
    sessions_max_age_seconds=604800,
):
    events = []
    lifecycle_order = []
    collectors = []
    workers = []
    broker_monitors = []

    def create_collector(service, repository, *, symbol, timeframe, count):
        collector = FakeMarketDataCollector(
            service,
            repository,
            symbol=symbol,
            timeframe=timeframe,
            count=count,
        )
        collector.events = events
        collectors.append(collector)
        return collector

    settings = SimpleNamespace(
        MT5_ENABLED=mt5_enabled,
        MT5_SYMBOL=mt5_symbol,
        DEFAULT_TIMEFRAME="M5",
        DATABASE_URL="sqlite:///./data/test.db",
        MT5_SESSIONS_FILE=sessions_file,
        MT5_SESSIONS_MAX_AGE_SECONDS=sessions_max_age_seconds,
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

    def create_broker_tick_monitor(stale_after_seconds):
        monitor = FakeBrokerTickMonitor(
            stale_after_seconds=stale_after_seconds,
        )
        broker_monitors.append(monitor)
        return monitor

    def create_broker_tick_worker(
        adapter,
        monitor,
        *,
        symbol,
        interval_seconds,
    ):
        worker = FakeBrokerTickWorker(
            adapter=adapter,
            monitor=monitor,
            symbol=symbol,
            interval_seconds=interval_seconds,
            lifecycle_order=lifecycle_order,
            stop_result=worker_stop_result,
            stop_error=worker_stop_error,
        )
        workers.append(worker)
        return worker

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
    monkeypatch.setattr(
        main,
        "BrokerTickMonitor",
        create_broker_tick_monitor,
    )
    monkeypatch.setattr(
        main,
        "BrokerTickMonitorWorker",
        create_broker_tick_worker,
    )
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
        create_collector,
    )
    monkeypatch.setattr(
        main,
        "ChronyStatusService",
        FakeChronyStatusService,
    )

    app = SimpleNamespace(state=SimpleNamespace())
    app.state._test_workers = workers
    app.state._test_broker_monitors = broker_monitors
    app.state._test_lifecycle_order = lifecycle_order

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
            assert app.state.market_data_collector is not None
            assert app.state.market_data_collector.symbol == "XAUUSD"
            assert app.state.market_data_collector.timeframe == "M5"
            assert app.state.market_data_collector.count == 300

    asyncio.run(run())

    assert events == [
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "adapter.connect",
        "monitor.start",
        "backup.start",
        "collector.start",
        "collector.stop",
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
            assert app.state.market_data_collector is None

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


def test_lifespan_skips_collector_when_mt5_symbol_is_empty(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        mt5_symbol="   ",
    )

    async def run():
        async with main.lifespan(app):
            assert app.state.market_data_collector is None

    asyncio.run(run())

    assert events == [
        "database.initialize",
        "database.migrations",
        "backup_service.initialize",
        "adapter.connect",
        "monitor.start",
        "backup.start",
        "collector.stop",
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
        "collector.start",
        "collector.stop",
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
        "collector.start",
        "collector.stop",
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


def test_lifespan_connects_session_calendar_to_market_data_service(
    monkeypatch,
):
    calendar = SimpleNamespace(symbol="XAUUSD")
    load_calls = []
    created_validators = []
    created_services = []

    def fake_load(path, *, max_age_seconds):
        load_calls.append((path, max_age_seconds))
        return calendar

    def create_validator(session_calendar=None):
        validator = FakeMarketDataValidator(
            session_calendar=session_calendar,
        )
        created_validators.append(validator)
        return validator

    def create_service(source, validator=None):
        service = FakeMarketDataService(
            source=source,
            validator=validator,
        )
        created_services.append(service)
        return service

    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        sessions_file="sessions.json",
        sessions_max_age_seconds=3600,
    )

    monkeypatch.setattr(
        main.MarketSessionCalendar,
        "load",
        staticmethod(fake_load),
    )
    monkeypatch.setattr(
        main,
        "MarketDataValidator",
        create_validator,
    )
    monkeypatch.setattr(
        main,
        "MarketDataService",
        create_service,
    )

    async def run():
        async with main.lifespan(app):
            assert load_calls == [
                ("sessions.json", 3600),
            ]

            assert len(created_validators) == 1
            assert (
                created_validators[0].session_calendar
                is calendar
            )

            assert len(created_services) == 1
            assert (
                created_services[0].validator
                is created_validators[0]
            )

            assert (
                app.state.market_data_service
                is created_services[0]
            )

    asyncio.run(run())


def test_lifespan_stops_startup_when_session_calendar_is_invalid(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        sessions_file="invalid_sessions.json",
    )

    def fail_load(path, *, max_age_seconds):
        raise main.MarketSessionCalendarError(
            "Simulated invalid session calendar"
        )

    monkeypatch.setattr(
        main.MarketSessionCalendar,
        "load",
        staticmethod(fail_load),
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Cannot start with an invalid MT5 session calendar",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert "database.initialize" not in events
    assert "database.migrations" not in events
    assert "backup_service.initialize" not in events
    assert "backup.start" not in events


def test_lifespan_stops_startup_when_session_file_is_missing(
    monkeypatch,
    tmp_path,
):
    missing_file = tmp_path / "missing_sessions.json"

    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        sessions_file=str(missing_file),
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Cannot start with an invalid MT5 session calendar",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert not missing_file.exists()
    assert "database.initialize" not in events
    assert "database.migrations" not in events
    assert "backup_service.initialize" not in events
    assert "backup.start" not in events


def test_lifespan_stops_startup_when_session_symbol_does_not_match(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        sessions_file="sessions.json",
    )

    def fail_load(path, *, max_age_seconds):
        raise main.MarketSessionCalendarError(
            "Session calendar symbol does not match configured symbol."
        )

    monkeypatch.setattr(
        main.MarketSessionCalendar,
        "load",
        staticmethod(fail_load),
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Cannot start with an invalid MT5 session calendar",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert "database.initialize" not in events
    assert "database.migrations" not in events
    assert "backup_service.initialize" not in events
    assert "backup.start" not in events


def test_lifespan_stops_startup_when_session_file_is_stale(
    monkeypatch,
    tmp_path,
):
    stale_file = tmp_path / "stale_sessions.json"

    generated_at = int(
        (
            datetime.now(timezone.utc)
            - timedelta(days=30)
        ).timestamp()
    )

    data = {
        "schema_version": 1,
        "status": "ok",
        "symbol": "XAUUSD",
        "generated_at_utc_epoch": generated_at,
        "server_utc_offset_seconds": 7200,
        "schedule_kind": "weekly_recurring",
        "days": [
            {
                "day_of_week": day,
                "trade": (
                    [
                        {
                            "from_seconds": 3720,
                            "to_seconds": 86280,
                        }
                    ]
                    if day in (1, 2, 3, 4, 5)
                    else []
                ),
                "quote": [],
            }
            for day in range(7)
        ],
    }

    stale_file.write_text(
        json.dumps(data),
        encoding="utf-8",
    )

    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        sessions_file=str(stale_file),
        sessions_max_age_seconds=3600,
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Cannot start with an invalid MT5 session calendar",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert "database.initialize" not in events
    assert "database.migrations" not in events
    assert "backup_service.initialize" not in events
    assert "backup.start" not in events


def test_lifespan_stops_startup_when_session_file_is_too_far_in_future(
    monkeypatch,
    tmp_path,
):
    future_file = tmp_path / "future_sessions.json"

    generated_at = int(
        (
            datetime.now(timezone.utc)
            + timedelta(minutes=10)
        ).timestamp()
    )

    data = {
        "schema_version": 1,
        "status": "ok",
        "symbol": "XAUUSD",
        "generated_at_utc_epoch": generated_at,
        "server_utc_offset_seconds": 7200,
        "schedule_kind": "weekly_recurring",
        "days": [
            {
                "day_of_week": day,
                "trade": (
                    [
                        {
                            "from_seconds": 3720,
                            "to_seconds": 86280,
                        }
                    ]
                    if day in (1, 2, 3, 4, 5)
                    else []
                ),
                "quote": [],
            }
            for day in range(7)
        ],
    }

    future_file.write_text(
        json.dumps(data),
        encoding="utf-8",
    )

    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        sessions_file=str(future_file),
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Cannot start with an invalid MT5 session calendar",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert "database.initialize" not in events
    assert "database.migrations" not in events
    assert "backup_service.initialize" not in events
    assert "backup.start" not in events


def test_lifespan_uses_configured_session_file_max_age(
    monkeypatch,
    tmp_path,
):
    sessions_file = tmp_path / "sessions.json"

    generated_at = int(
        (
            datetime.now(timezone.utc)
            - timedelta(hours=2)
        ).timestamp()
    )

    data = {
        "schema_version": 1,
        "status": "ok",
        "symbol": "XAUUSD",
        "generated_at_utc_epoch": generated_at,
        "server_utc_offset_seconds": 7200,
        "schedule_kind": "weekly_recurring",
        "days": [
            {
                "day_of_week": day,
                "trade": (
                    [
                        {
                            "from_seconds": 3720,
                            "to_seconds": 86280,
                        }
                    ]
                    if day in (1, 2, 3, 4, 5)
                    else []
                ),
                "quote": [],
            }
            for day in range(7)
        ],
    }

    sessions_file.write_text(
        json.dumps(data),
        encoding="utf-8",
    )

    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
        sessions_file=str(sessions_file),
        sessions_max_age_seconds=3600,
    )

    async def run():
        with pytest.raises(
            RuntimeError,
            match="Cannot start with an invalid MT5 session calendar",
        ):
            async with main.lifespan(app):
                pass

    asyncio.run(run())

    assert "database.initialize" not in events
    assert "database.migrations" not in events
    assert "backup_service.initialize" not in events
    assert "backup.start" not in events


def test_broker_tick_worker_starts_with_configured_symbol_and_stops(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        mt5_symbol="XAUUSD",
    )

    async def run():
        async with main.lifespan(app):
            worker = app.state.broker_tick_worker

            assert worker is not None
            assert worker.started is True
            assert worker.symbol == "XAUUSD"
            assert worker.interval_seconds == 1.0

            assert worker.adapter is app.state.mt5_adapter
            assert worker.monitor is app.state.broker_tick_monitor

            assert (
                app.state.broker_tick_monitor.stale_after_seconds
                == 15.0
            )

            assert app.state._test_lifecycle_order == [
                "broker_tick.start",
            ]

    asyncio.run(run())

    worker = app.state._test_workers[0]

    assert worker.stop_calls == 1
    assert worker.started is True

    assert app.state._test_lifecycle_order == [
        "broker_tick.start",
        "broker_tick.stop",
    ]


def test_broker_tick_worker_stops_before_mt5_monitor(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    async def run():
        async with main.lifespan(app):
            pass

    asyncio.run(run())

    order = app.state._test_lifecycle_order

    assert order.index("broker_tick.start") < order.index(
        "broker_tick.stop"
    )

    assert order.index("broker_tick.stop") < order.index(
        "monitor.stop"
    )


def test_lifespan_skips_broker_tick_worker_when_mt5_disabled(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
    )

    async def run():
        async with main.lifespan(app):
            assert app.state.broker_tick_worker is None
            assert app.state._test_workers == []

    asyncio.run(run())

    assert app.state._test_workers == []


def test_lifespan_skips_broker_tick_worker_when_symbol_is_empty(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        mt5_symbol="   ",
    )

    async def run():
        async with main.lifespan(app):
            assert app.state.broker_tick_worker is None
            assert app.state._test_workers == []

    asyncio.run(run())

    assert app.state._test_workers == []


def test_lifespan_skips_adapter_shutdown_when_broker_tick_worker_fails_to_stop(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        worker_stop_result=False,
    )

    async def run():
        async with main.lifespan(app):
            pass

    asyncio.run(run())

    assert app.state._test_workers[0].stop_calls == 1

    assert "broker_tick.stop" in (
        app.state._test_lifecycle_order
    )

    assert "monitor.stop" in (
        app.state._test_lifecycle_order
    )

    assert "adapter.shutdown" not in events
    assert "database.dispose" in events


def test_lifespan_skips_adapter_shutdown_when_broker_tick_worker_stop_raises(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
        worker_stop_error=RuntimeError(
            "Simulated broker tick worker stop failure"
        ),
    )

    async def run():
        async with main.lifespan(app):
            pass

    asyncio.run(run())

    assert app.state._test_workers[0].stop_calls == 1
    assert "adapter.shutdown" not in events
    assert "database.dispose" in events
