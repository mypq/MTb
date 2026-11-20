import asyncio
import json
from datetime import datetime, timedelta, timezone
from threading import Event
from types import SimpleNamespace

import pytest

from backend.mt5.models import MT5ConnectionState
from backend.mt5.monitor import MT5ConnectionMonitor as RealMT5ConnectionMonitor
import backend.main as main


class FakeAdapter:
    def __init__(self, settings, events, connect_error=None):
        self.settings = settings
        self.events = events
        self.connect_error = connect_error
        self.candle_clock_calibrator = None
        self.broker_offset_registry = None

    def connect(self):
        self.events.append("adapter.connect")
        if self.connect_error is not None:
            raise self.connect_error
        return SimpleNamespace(state="CONNECTED")

    def shutdown(self):
        self.events.append("adapter.shutdown")

    def connection_status(self):
        return SimpleNamespace(model_dump=lambda mode: {}, state="CONNECTED")

    def set_candle_clock_calibrator(self, calibrator):
        self.candle_clock_calibrator = calibrator

    def set_broker_offset_registry(self, registry):
        self.broker_offset_registry = registry


class FakeMonitor:
    def __init__(
        self,
        adapter,
        interval_seconds,
        stop_result=True,
        stop_error=None,
        lifecycle_order=None,
    ):
        self.adapter = adapter
        self.interval_seconds = interval_seconds
        self.events = adapter.events
        self.stop_result = stop_result
        self.stop_error = stop_error
        self.lifecycle_order = lifecycle_order

    @property
    def is_running(self):
        return False

    def start(self):
        self.events.append("monitor.start")
        if self.lifecycle_order is not None:
            self.lifecycle_order.append("monitor.start")

    def stop(self):
        self.events.append("monitor.stop")
        if self.lifecycle_order is not None:
            self.lifecycle_order.append("monitor.stop")

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

    def set_session_calendar(self, session_calendar):
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
        timeframes=("M5",),
        clock_calibrator=None,
    ):
        self.service = service
        self.repository = repository
        self.symbol = symbol
        self.timeframe = timeframe
        self.count = count
        self.timeframes = tuple(timeframes)
        self.clock_calibrator = clock_calibrator
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


class FakeNtpVerificationMonitor:
    def __init__(self, verifier, interval_seconds=3600):
        self.verifier = verifier
        self.interval_seconds = interval_seconds

    async def run(self, stop_event):
        await stop_event.wait()

    def snapshot(self):
        return {
            "provider": "external_ntp_udp_123",
            "trusted": False,
            "verification_interval_seconds": self.interval_seconds,
            "last_attempt": None,
            "last_attempt_at_unix": None,
            "last_success": None,
            "last_error": None,
        }


class FakeVerifiedBrokerCandleClockCalibrator:
    """
    Lifecycle-test double.

    These tests verify application startup and shutdown ordering.
    Real candle-clock verification is covered by dedicated tests.
    """

    def __init__(self, *args, **kwargs):
        self.reset_calls = 0

    def snapshot(self):
        return SimpleNamespace(
            status="VERIFIED",
            symbol="XAUUSD",
            broker_server="Test-Server",
            offset_seconds=0.0,
            uncertainty_seconds=0.05,
            sample_count=5,
            reason=None,
        )

    def observe(self, **sample):
        return self.snapshot()

    def reset(self):
        self.reset_calls += 1


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

    def create_collector(
        service,
        repository,
        *,
        symbol,
        timeframe,
        count,
        timeframes=("M5",),
        clock_calibrator=None,
    ):
        collector = FakeMarketDataCollector(
            service,
            repository,
            symbol=symbol,
            timeframe=timeframe,
            count=count,
            timeframes=timeframes,
            clock_calibrator=clock_calibrator,
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
        adapter = FakeAdapter(
            settings=settings,
            events=events,
            connect_error=connect_error,
        )
        app.state.mt5_adapter = adapter
        return adapter

    def create_monitor(adapter, interval_seconds):
        monitor = FakeMonitor(
            adapter=adapter,
            interval_seconds=interval_seconds,
            stop_result=monitor_stop_result,
            stop_error=monitor_stop_error,
            lifecycle_order=lifecycle_order,
        )
        app.state.mt5_monitor = monitor
        return monitor

    def create_broker_tick_monitor(stale_after_seconds):
        monitor = FakeBrokerTickMonitor(
            stale_after_seconds=stale_after_seconds,
        )
        broker_monitors.append(monitor)
        app.state.broker_tick_monitor = monitor
        return monitor

    def create_broker_tick_worker(
        adapter,
        monitor,
        *,
        symbol,
        interval_seconds,
        clock_calibrator=None,
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
        app.state.broker_tick_worker = worker
        return worker

    def create_database(database_url):
        database = FakeDatabase(
            database_url=database_url,
            events=events,
            initialize_error=initialize_error,
        )
        app.state.database = database
        return database

    def create_backup_service(database):
        backup_service = FakeBackupService(database, events)
        app.state.database_backup_service = backup_service
        return backup_service

    async def fake_backup_loop(backup_service, stop_event):
        events.append("backup.start")
        await stop_event.wait()
        events.append("backup.stop")

    def apply_migrations():
        events.append("database.migrations")
        if migration_error is not None:
            raise migration_error

    app = SimpleNamespace(state=SimpleNamespace())
    app.state._test_workers = workers
    app.state._test_broker_monitors = broker_monitors
    app.state._test_lifecycle_order = lifecycle_order
    app.state.market_data_collector = None
    app.state.market_data_service = None
    app.state.market_data_repository = None
    app.state.mt5_adapter = None
    app.state.mt5_monitor = None
    app.state.broker_tick_monitor = None
    app.state.broker_tick_worker = None

    def create_service(source, validator=None):
        service = FakeMarketDataService(source=source, validator=validator)
        app.state.market_data_service = service
        return service

    def create_repository(database):
        repo = FakeMarketDataRepository(database)
        app.state.market_data_repository = repo
        return repo

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
        create_service,
    )
    monkeypatch.setattr(
        main,
        "MarketDataRepository",
        create_repository,
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
    monkeypatch.setattr(
        main,
        "NtpVerificationMonitor",
        FakeNtpVerificationMonitor,
    )

    monkeypatch.setattr(
        main,
        "BrokerCandleClockCalibrator",
        FakeVerifiedBrokerCandleClockCalibrator,
    )

    return app, events


def test_lifespan_waits_for_real_monitor_recovery_before_starting_workers(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    allow_recovery = Event()
    worker_started = Event()
    collector_started = Event()
    adapter_lock = __import__("threading").Lock()

    class ControlledAdapter:
        def __init__(self, settings):
            self.settings = settings
            self.connect_calls = 0
            self.state = MT5ConnectionState.DISCONNECTED
            self.candle_clock_calibrator = None
            self.broker_offset_registry = None
            self.candle_clock_invalidation_calls = 0

        def mark_candle_clock_connection_lost(self):
            self.candle_clock_invalidation_calls += 1

        def connect(self):
            with adapter_lock:
                self.connect_calls += 1
                call_number = self.connect_calls

            if call_number == 1:
                self.state = MT5ConnectionState.DISCONNECTED
                return SimpleNamespace(state=self.state)

            allow_recovery.wait(timeout=5)

            if allow_recovery.is_set():
                self.state = MT5ConnectionState.CONNECTED

            return SimpleNamespace(state=self.state)

        def connection_status(self):
            with adapter_lock:
                return SimpleNamespace(state=self.state)

        def shutdown(self):
            events.append("adapter.shutdown")

        def set_candle_clock_calibrator(self, calibrator):
            self.candle_clock_calibrator = calibrator

        def set_broker_offset_registry(self, registry):
            self.broker_offset_registry = registry

    class ControlledWorker:
        def __init__(
            self,
            adapter,
            monitor,
            *,
            symbol,
            interval_seconds,
            clock_calibrator=None,
        ):
            self.adapter = adapter
            self.monitor = monitor
            self.symbol = symbol
            self.interval_seconds = interval_seconds
            self.started_state = None

        def start(self):
            self.started_state = self.adapter.connection_status().state
            worker_started.set()

        def stop(self):
            return True

        def status(self):
            return {
                "running": worker_started.is_set(),
                "state": "NO_DATA",
                "clock_verification": "UNVERIFIED",
            }

    class ControlledCollector:
        def __init__(
            self,
            service,
            repository,
            *,
            symbol,
            timeframe,
            count,
            timeframes=("M5",),
            clock_calibrator=None,
        ):
            self.service = service
            self.repository = repository
            self.symbol = symbol
            self.timeframe = timeframe
            self.count = count
            self.timeframes = tuple(timeframes)
            self.clock_calibrator = clock_calibrator
            self.interval_seconds = 300.0

        async def run(self, stop_event):
            self.started_state = (
                self.service.source.connection_status().state
            )
            collector_started.set()
            await stop_event.wait()

    monkeypatch.setattr(main, "MT5Adapter", ControlledAdapter)

    monkeypatch.setattr(
        main,
        "MT5ConnectionMonitor",
        lambda adapter, interval_seconds: RealMT5ConnectionMonitor(
            adapter,
            interval_seconds=0.01,
        ),
    )

    monkeypatch.setattr(
        main,
        "BrokerTickMonitorWorker",
        ControlledWorker,
    )

    monkeypatch.setattr(
        main,
        "MarketDataCollector",
        ControlledCollector,
    )

    async def wait_for_event(event, timeout):
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout

        while not event.is_set():
            remaining = deadline - loop.time()

            if remaining <= 0:
                raise asyncio.TimeoutError

            await asyncio.sleep(min(0.01, remaining))

        return True

    async def run():
        async with main.lifespan(app):
            try:
                # The real monitor is attempting recovery.
                # Workers must not start while recovery is blocked.
                assert not worker_started.is_set()
                assert not collector_started.is_set()

                allow_recovery.set()

                # Both workers must start after the real monitor
                # successfully restores the MT5 connection.
                assert await wait_for_event(
                    worker_started,
                    timeout=2,
                )

                assert await wait_for_event(
                    collector_started,
                    timeout=2,
                )

                assert (
                    app.state.mt5_adapter.candle_clock_invalidation_calls
                    >= 1
                )

                assert (
                    app.state.mt5_adapter.connection_status().state
                    == MT5ConnectionState.CONNECTED
                )

                assert (
                    app.state.broker_tick_worker.started_state
                    == MT5ConnectionState.CONNECTED
                )

                assert (
                    app.state.market_data_collector.started_state
                    == MT5ConnectionState.CONNECTED
                )

            finally:
                allow_recovery.set()

    asyncio.run(run())


def test_lifespan_recovers_initial_mt5_disconnect_before_starting_workers(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    lifecycle_order = app.state._test_lifecycle_order

    class RecoveringAdapter:
        def __init__(self, settings):
            self.settings = settings
            self.events = events
            self.connect_calls = 0
            self.state = "DISCONNECTED"
            self.candle_clock_calibrator = None
            self.broker_offset_registry = None

        def connect(self):
            self.connect_calls += 1
            self.events.append("adapter.connect")

            if self.connect_calls == 1:
                self.state = "DISCONNECTED"
            else:
                self.state = "CONNECTED"

            return SimpleNamespace(state=self.state)

        def connection_status(self):
            return SimpleNamespace(
                state=self.state,
                model_dump=lambda mode: {},
            )

        def shutdown(self):
            self.events.append("adapter.shutdown")

        def set_candle_clock_calibrator(self, calibrator):
            self.candle_clock_calibrator = calibrator

        def set_broker_offset_registry(self, registry):
            self.broker_offset_registry = registry

    class RecoveringMonitor:
        def __init__(self, adapter, interval_seconds):
            self.adapter = adapter
            self.interval_seconds = interval_seconds

        @property
        def is_running(self):
            return True

        def start(self):
            events.append("monitor.start")
            lifecycle_order.append("monitor.start")

            status = self.adapter.connection_status()

            if status.state != "CONNECTED":
                self.adapter.connect()

        def stop(self):
            events.append("monitor.stop")
            lifecycle_order.append("monitor.stop")
            return True

    monkeypatch.setattr(
        main,
        "MT5Adapter",
        RecoveringAdapter,
    )

    monkeypatch.setattr(
        main,
        "MT5ConnectionMonitor",
        RecoveringMonitor,
    )

    async def run():
        async with main.lifespan(app):
            adapter = app.state.mt5_adapter
            monitor = app.state.mt5_monitor

            # Wait for the background startup coordinator.
            for _ in range(100):
                if (
                    app.state.broker_tick_worker is not None
                    and app.state.market_data_collector is not None
                ):
                    break

                await asyncio.sleep(0.01)

            worker = app.state.broker_tick_worker
            collector = app.state.market_data_collector

            # Initial connection failed, then recovery succeeded.
            assert adapter.connect_calls == 2
            assert adapter.connection_status().state == "CONNECTED"

            # All components use the same adapter.
            assert monitor.adapter is adapter
            assert worker is not None
            assert collector is not None
            assert worker.adapter is adapter
            assert collector.service.source is adapter

            # Workers were created and started.
            assert worker.started is True
            assert collector.symbol == "XAUUSD"

            # Monitor started before the broker tick worker.
            assert lifecycle_order.index(
                "monitor.start"
            ) < lifecycle_order.index(
                "broker_tick.start"
            )

    asyncio.run(run())


def test_lifespan_starts_backup_after_migrations_and_stops_before_database(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    async def run():
        async with main.lifespan(app):
            # The backup task is scheduled before MT5 startup, but its
            # coroutine may run as soon as startup yields to the event loop.
            # Assert the lifecycle guarantees, not scheduler-dependent timing.
            assert events[:3] == [
                "database.initialize",
                "database.migrations",
                "backup_service.initialize",
            ]
            assert events.index("database.migrations") < events.index(
                "backup_service.initialize"
            )
            if "backup.start" in events:
                assert events.index("backup_service.initialize") < events.index(
                    "backup.start"
                )

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
                "monitor.start",
                "broker_tick.start",
            ]

    asyncio.run(run())

    worker = app.state._test_workers[0]

    assert worker.stop_calls == 1
    assert worker.started is True

    assert app.state._test_lifecycle_order == [
        "monitor.start",
        "broker_tick.start",
        "broker_tick.stop",
        "monitor.stop",
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


@pytest.mark.parametrize(
    ("connection_state", "expected_invalidation"),
    [
        (MT5ConnectionState.CONNECTED, False),
        (MT5ConnectionState.DISCONNECTED, True),
        (MT5ConnectionState.ERROR, True),
    ],
)
def test_candle_clock_read_error_checks_connection_before_invalidation(
    monkeypatch,
    connection_state,
    expected_invalidation,
):
    stop_event = asyncio.Event()

    class Calibrator:
        def __init__(self):
            self.invalidation_calls = 0

        def mark_connection_lost(self):
            self.invalidation_calls += 1

    calibrator = Calibrator()

    class Adapter:
        def __init__(self):
            self.connection_checks = 0
            self.invalidation_calls = 0

        def get_active_candle_clock_sample(self, symbol):
            raise RuntimeError("Simulated temporary MT5 read failure")

        def connection_status(self):
            self.connection_checks += 1
            return SimpleNamespace(state=connection_state)

        def mark_candle_clock_connection_lost(self):
            self.invalidation_calls += 1
            calibrator.mark_connection_lost()

    adapter = Adapter()
    to_thread_calls = 0

    async def fake_to_thread(function, *args):
        nonlocal to_thread_calls
        to_thread_calls += 1

        if to_thread_calls == 1:
            raise RuntimeError("Simulated temporary MT5 read failure")

        stop_event.set()
        return function(*args)

    monkeypatch.setattr(
        main.asyncio,
        "to_thread",
        fake_to_thread,
    )

    asyncio.run(
        main._candle_clock_calibration_loop(
            adapter,
            calibrator,
            "XAUUSD",
            stop_event,
        )
    )

    assert adapter.connection_checks == 1
    assert adapter.invalidation_calls == int(expected_invalidation)
    assert calibrator.invalidation_calls == int(expected_invalidation)


def test_lifespan_waits_for_blocked_collector_and_backup_before_database_dispose(
    monkeypatch,
):
    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    collector_started = Event()
    collector_release = Event()
    backup_started = Event()
    backup_release = Event()

    entered = asyncio.Event()
    shutdown_requested = asyncio.Event()

    class BlockingCollector:
        def __init__(
            self,
            service,
            repository,
            *,
            symbol,
            timeframe,
            count,
            timeframes=("M5",),
            clock_calibrator=None,
        ):
            self.service = service
            self.repository = repository
            self.symbol = symbol
            self.timeframe = timeframe
            self.count = count
            self.timeframes = tuple(timeframes)
            self.clock_calibrator = clock_calibrator

        async def run(self, stop_event):
            events.append("collector.start")

            def blocked_collection():
                collector_started.set()

                if not collector_release.wait(timeout=5):
                    raise RuntimeError(
                        "Collector release timed out."
                    )

            await asyncio.to_thread(blocked_collection)

            events.append("collector.stop")

    async def blocking_backup_loop(backup_service, stop_event):
        events.append("backup.start")

        def blocked_backup():
            backup_started.set()

            if not backup_release.wait(timeout=5):
                raise RuntimeError(
                    "Backup release timed out."
                )

        await asyncio.to_thread(blocked_backup)

        # Match the production loop: after an active backup
        # finishes, wait for the explicit shutdown request.
        if not stop_event.is_set():
            await stop_event.wait()

        events.append("backup.stop")

    monkeypatch.setattr(
        main,
        "MarketDataCollector",
        BlockingCollector,
    )

    monkeypatch.setattr(
        main,
        "_database_backup_loop",
        blocking_backup_loop,
    )

    async def wait_for_thread(event, name):
        for _ in range(600):
            if event.is_set():
                return

            await asyncio.sleep(0.005)

        raise AssertionError(
            f"{name} did not start."
        )

    async def run_lifespan():
        async with main.lifespan(app):
            entered.set()
            await shutdown_requested.wait()

    async def scenario():
        lifecycle_task = asyncio.create_task(
            run_lifespan()
        )

        try:
            await asyncio.wait_for(
                entered.wait(),
                timeout=5,
            )

            await wait_for_thread(
                collector_started,
                "Collector thread",
            )

            await wait_for_thread(
                backup_started,
                "Backup thread",
            )

            shutdown_requested.set()

            await asyncio.sleep(0.05)

            assert not lifecycle_task.done()
            assert "database.dispose" not in events

        finally:
            collector_release.set()
            backup_release.set()
            shutdown_requested.set()

        await asyncio.wait_for(
            lifecycle_task,
            timeout=10,
        )

        assert events.index(
            "collector.stop"
        ) < events.index("backup.stop")

        assert events.index(
            "collector.stop"
        ) < events.index("database.dispose")

        assert events.index(
            "backup.stop"
        ) < events.index("database.dispose")

    asyncio.run(scenario())


def test_lifespan_skips_mt5_shutdown_while_real_broker_worker_is_blocked(
    monkeypatch,
):
    from backend.mt5.broker_tick_monitor import (
        BrokerTickMonitor as RealBrokerTickMonitor,
    )
    from backend.mt5.broker_tick_worker import (
        BrokerTickMonitorWorker as RealBrokerTickMonitorWorker,
    )
    from backend.mt5.time_models import BrokerTimeSample

    app, events = configure_lifecycle(
        monkeypatch,
        mt5_enabled=True,
    )

    worker_started = Event()
    release_worker = Event()
    adapter_shutdown_called = Event()
    worker_instances = []

    class BlockingAdapter:
        def __init__(self, settings):
            self.settings = settings
            self.candle_clock_calibrator = None
            self.broker_offset_registry = None

        def connect(self):
            events.append("adapter.connect")
            return SimpleNamespace(
                state=MT5ConnectionState.CONNECTED,
            )

        def connection_status(self):
            return SimpleNamespace(
                state=MT5ConnectionState.CONNECTED,
            )

        def mark_candle_clock_connection_lost(self):
            pass

        def set_candle_clock_calibrator(self, calibrator):
            self.candle_clock_calibrator = calibrator

        def set_broker_offset_registry(self, registry):
            self.broker_offset_registry = registry

        def get_active_candle_clock_sample(self, symbol):
            return {}

        def get_broker_time_sample(self, symbol):
            worker_started.set()

            if not release_worker.wait(timeout=5):
                raise RuntimeError(
                    "Blocked MT5 worker release timed out."
                )

            return BrokerTimeSample(
                symbol=symbol,
                broker_time_msc=1,
                received_at_utc=datetime.now(timezone.utc),
                received_monotonic_ns=0,
                bid=2000.0,
                ask=2000.1,
                account_login=12345,
                broker_server="Test-Server",
            )

        def shutdown(self):
            events.append("adapter.shutdown")
            adapter_shutdown_called.set()

    class FastStopWorker(RealBrokerTickMonitorWorker):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            worker_instances.append(self)

        def stop(self, timeout_seconds=15.0):
            return super().stop(timeout_seconds=0.05)

    monkeypatch.setattr(
        main,
        "MT5Adapter",
        BlockingAdapter,
    )

    monkeypatch.setattr(
        main,
        "MT5ConnectionMonitor",
        RealMT5ConnectionMonitor,
    )

    monkeypatch.setattr(
        main,
        "BrokerTickMonitor",
        RealBrokerTickMonitor,
    )

    monkeypatch.setattr(
        main,
        "BrokerTickMonitorWorker",
        FastStopWorker,
    )

    async def wait_for_worker():
        for _ in range(600):
            if worker_started.is_set():
                return

            await asyncio.sleep(0.005)

        raise AssertionError(
            "Real broker worker did not enter its blocked MT5 operation."
        )

    async def run():
        async with main.lifespan(app):
            await wait_for_worker()

            assert len(worker_instances) == 1
            assert worker_instances[0].is_running

    try:
        asyncio.run(run())

        worker = worker_instances[0]

        # Shutdown must not close MT5 while its real worker is blocked.
        assert worker.is_running
        assert not adapter_shutdown_called.is_set()
        assert "adapter.shutdown" not in events

        # The application must still complete its database cleanup.
        assert "database.dispose" in events

    finally:
        release_worker.set()

        if worker_instances:
            assert worker_instances[0].stop(
                timeout_seconds=3,
            )

    assert not worker_instances[0].is_running
    assert not adapter_shutdown_called.is_set()



def test_lifespan_attaches_historical_offset_registry_to_adapter(
    monkeypatch,
):
    from backend.market_data.broker_offset_registry import (
        BrokerOffsetRegistry,
    )

    app, _ = configure_lifecycle(
        monkeypatch,
        mt5_enabled=False,
    )

    async def run():
        async with main.lifespan(app):
            registry = app.state.mt5_adapter.broker_offset_registry

            assert isinstance(registry, BrokerOffsetRegistry)
            assert app.state.broker_offset_service is None
            assert app.state.broker_offset_monitor is None

    asyncio.run(run())


def _write_session_export(path, *, generated_at, offset=7200, symbol="GOLD"):
    data = {
        "schema_version": 1,
        "status": "ok",
        "symbol": symbol,
        "generated_at_utc_epoch": generated_at,
        "server_utc_offset_seconds": offset,
        "schedule_kind": "weekly_recurring",
        "days": [
            {
                "day_of_week": day,
                "trade": (
                    [{"from_seconds": 3720, "to_seconds": 86280}]
                    if day in (1, 2, 3, 4, 5)
                    else []
                ),
                "quote": [],
            }
            for day in range(7)
        ],
    }
    path.write_text(json.dumps(data), encoding="utf-8")


def test_session_calendar_reload_replaces_calendar_with_newer_valid_file(
    monkeypatch,
    tmp_path,
):
    from backend.market_data.session_calendar import MarketSessionCalendar

    path = tmp_path / "sessions.json"
    now = datetime.now(timezone.utc)
    _write_session_export(
        path,
        generated_at=int(now.timestamp()) - 10,
        offset=7200,
    )
    initial = MarketSessionCalendar.load(
        path,
        now_utc=now,
    )
    validator = main.MarketDataValidator(session_calendar=initial)

    _write_session_export(
        path,
        generated_at=int(now.timestamp()),
        offset=10800,
    )
    monkeypatch.setattr(
        main,
        "SESSION_CALENDAR_RELOAD_INTERVAL_SECONDS",
        0.01,
    )

    async def run():
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            main._session_calendar_reload_loop(
                str(path),
                validator,
                "GOLD",
                3600,
                stop_event,
            )
        )

        try:
            for _ in range(100):
                if validator.session_calendar.server_utc_offset_seconds == 10800:
                    break
                await asyncio.sleep(0.01)
            assert validator.session_calendar.server_utc_offset_seconds == 10800
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=2)

    asyncio.run(run())


@pytest.mark.parametrize(
    "invalid_export",
    ["not-json", "wrong-symbol", "older-snapshot"],
)
def test_session_calendar_reload_preserves_last_valid_calendar(
    monkeypatch,
    tmp_path,
    invalid_export,
):
    from backend.market_data.session_calendar import MarketSessionCalendar

    path = tmp_path / "sessions.json"
    now = datetime.now(timezone.utc)
    _write_session_export(
        path,
        generated_at=int(now.timestamp()) - 10,
        offset=7200,
    )
    initial = MarketSessionCalendar.load(path, now_utc=now)
    validator = main.MarketDataValidator(session_calendar=initial)

    if invalid_export == "not-json":
        path.write_text("not-json", encoding="utf-8")
    elif invalid_export == "wrong-symbol":
        _write_session_export(
            path,
            generated_at=int(now.timestamp()),
            offset=3600,
            symbol="EURUSD",
        )
    else:
        _write_session_export(
            path,
            generated_at=int(now.timestamp()) - 20,
            offset=3600,
        )

    monkeypatch.setattr(
        main,
        "SESSION_CALENDAR_RELOAD_INTERVAL_SECONDS",
        0.01,
    )

    async def run():
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            main._session_calendar_reload_loop(
                str(path),
                validator,
                "GOLD",
                3600,
                stop_event,
            )
        )
        try:
            await asyncio.sleep(0.05)
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=2)

    asyncio.run(run())
    assert validator.session_calendar is initial
    assert validator.session_calendar.server_utc_offset_seconds == 7200
