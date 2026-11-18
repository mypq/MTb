
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import backend.main as main


class FakeAdapter:
    def __init__(self, settings):
        self.settings = settings
        self.shutdown_called = False

    def set_candle_clock_calibrator(self, calibrator):
        self.calibrator = calibrator

    def shutdown(self):
        self.shutdown_called = True


class FakeConnectionMonitor:
    def __init__(self, adapter, interval_seconds):
        self.adapter = adapter
        self.interval_seconds = interval_seconds

    def stop(self):
        return True


class FakeBrokerTickMonitor:
    def __init__(self, stale_after_seconds):
        self.stale_after_seconds = stale_after_seconds


class FakeDatabase:
    def __init__(self, database_url):
        self.database_url = database_url
        self.initialized = False
        self.disposed = False

    def initialize(self):
        self.initialized = True

    def dispose(self):
        self.disposed = True


class FakeNtpVerificationMonitor:
    def __init__(self, verifier, interval_seconds):
        self.verifier = verifier
        self.interval_seconds = interval_seconds

    async def run(self, stop_event):
        await stop_event.wait()


class FakeClockCalibrator:
    def __init__(self, *args, **kwargs):
        pass


class FakeMarketDataRepository:
    def __init__(self, database):
        self.database = database


class FakeMarketDataValidator:
    def __init__(self, session_calendar=None):
        self.session_calendar = session_calendar


class FakeMarketDataService:
    def __init__(self, source, validator):
        self.source = source
        self.validator = validator


class FakeBackupService:
    def __init__(self, database):
        self.database = database


class FakeBrokerOffsetDetector:
    def __init__(self, verifier):
        self.verifier = verifier


class FakeBrokerOffsetRegistry:
    def __init__(self, path):
        self.path = path


class FakeBrokerOffsetService:
    def __init__(self, history_path, detector, registry):
        self.history_path = history_path
        self.detector = detector
        self.registry = registry


class FakeBrokerOffsetMonitor:
    def __init__(self, service, *, interval_seconds):
        self.service = service
        self.interval_seconds = interval_seconds
        self.started_event = asyncio.Event()
        self.stopped = False
        self.running = False

    @property
    def is_running(self):
        return self.running

    async def run(self, stop_event):
        self.running = True
        self.started_event.set()

        try:
            await stop_event.wait()
        finally:
            self.running = False
            self.stopped = True


async def fake_backup_loop(backup_service, stop_event):
    await stop_event.wait()


def test_lifespan_starts_and_stops_configured_broker_offset_pipeline(
    monkeypatch,
):
    settings = SimpleNamespace(
        MT5_ENABLED=False,
        MT5_SYMBOL="",
        DEFAULT_TIMEFRAME="M5",
        DATABASE_URL="sqlite:///./data/runtime-test.db",
        MT5_SESSIONS_FILE=None,
        MT5_CLOCK_HISTORY_FILE="runtime/clock_history.csv",
        BROKER_OFFSET_REGISTRY_FILE="runtime/broker_offsets.jsonl",
        BROKER_OFFSET_POLL_INTERVAL_SECONDS=2.5,
    )

    app = SimpleNamespace(state=SimpleNamespace())

    created = {}

    def create_offset_detector(verifier):
        detector = FakeBrokerOffsetDetector(verifier)
        created["detector"] = detector
        return detector

    def create_offset_registry(path):
        registry = FakeBrokerOffsetRegistry(path)
        created["registry"] = registry
        return registry

    def create_offset_service(history_path, detector, registry):
        service = FakeBrokerOffsetService(
            history_path,
            detector,
            registry,
        )
        created["service"] = service
        return service

    def create_offset_monitor(service, *, interval_seconds):
        monitor = FakeBrokerOffsetMonitor(
            service,
            interval_seconds=interval_seconds,
        )
        created["monitor"] = monitor
        return monitor

    monkeypatch.setattr(main, "get_settings", lambda: settings)
    monkeypatch.setattr(main, "MT5Adapter", FakeAdapter)
    monkeypatch.setattr(
        main,
        "MT5ConnectionMonitor",
        FakeConnectionMonitor,
    )
    monkeypatch.setattr(
        main,
        "BrokerTickMonitor",
        FakeBrokerTickMonitor,
    )
    monkeypatch.setattr(main, "Database", FakeDatabase)
    monkeypatch.setattr(
        main,
        "MarketDataRepository",
        FakeMarketDataRepository,
    )
    monkeypatch.setattr(
        main,
        "MarketDataValidator",
        FakeMarketDataValidator,
    )
    monkeypatch.setattr(
        main,
        "MarketDataService",
        FakeMarketDataService,
    )
    monkeypatch.setattr(
        main,
        "DatabaseBackupService",
        FakeBackupService,
    )
    monkeypatch.setattr(
        main,
        "_database_backup_loop",
        fake_backup_loop,
    )
    monkeypatch.setattr(
        main,
        "NtpVerificationMonitor",
        FakeNtpVerificationMonitor,
    )
    monkeypatch.setattr(
        main,
        "BrokerClockCalibrator",
        FakeClockCalibrator,
    )
    monkeypatch.setattr(
        main,
        "BrokerCandleClockCalibrator",
        FakeClockCalibrator,
    )
    monkeypatch.setattr(
        main,
        "BrokerOffsetDetector",
        create_offset_detector,
    )
    monkeypatch.setattr(
        main,
        "BrokerOffsetRegistry",
        create_offset_registry,
    )
    monkeypatch.setattr(
        main,
        "BrokerOffsetEvidenceService",
        create_offset_service,
    )
    monkeypatch.setattr(
        main,
        "BrokerOffsetEvidenceMonitor",
        create_offset_monitor,
    )
    monkeypatch.setattr(
        main,
        "upgrade_database",
        lambda: None,
    )

    async def scenario():
        async with main.lifespan(app):
            monitor = created["monitor"]

            await asyncio.wait_for(
                monitor.started_event.wait(),
                timeout=1.0,
            )

            assert app.state.broker_offset_service is (
                created["service"]
            )
            assert app.state.broker_offset_monitor is monitor

            assert created["service"].history_path == (
                "runtime/clock_history.csv"
            )
            assert created["registry"].path == (
                "runtime/broker_offsets.jsonl"
            )
            assert monitor.interval_seconds == 2.5
            assert monitor.is_running is True
            assert monitor.stopped is False

        assert created["monitor"].is_running is False
        assert created["monitor"].stopped is True
        assert app.state.database.disposed is True
        assert app.state.mt5_adapter.shutdown_called is True

    asyncio.run(scenario())
