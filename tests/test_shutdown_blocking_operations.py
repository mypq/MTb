
import asyncio
import time
from threading import Event
from types import SimpleNamespace

from backend.market_data.collector import MarketDataCollector
from backend.market_data.ntp_monitor import NtpVerificationMonitor
from backend.main import (
    _candle_clock_calibration_loop,
    _database_backup_loop,
)


async def wait_until_thread_starts(started: Event) -> None:
    """Wait for a worker thread without blocking the event loop."""
    deadline = time.monotonic() + 3.0

    while not started.is_set():
        if time.monotonic() >= deadline:
            raise AssertionError("Blocking worker did not start.")

        await asyncio.sleep(0.005)


def test_database_backup_loop_waits_for_blocked_backup():
    started = Event()
    release = Event()
    stop_event = asyncio.Event()

    class BackupService:
        def create_backup(self):
            started.set()
            if not release.wait(timeout=5):
                raise RuntimeError("Backup release timed out.")

    async def scenario():
        task = asyncio.create_task(
            _database_backup_loop(
                BackupService(),
                stop_event,
            )
        )

        try:
            await wait_until_thread_starts(started)

            stop_event.set()

            # Setting the stop event must not pretend that the
            # currently executing backup thread has stopped.
            await asyncio.sleep(0.05)
            assert not task.done()

        finally:
            release.set()

        await task
        assert task.done()

    asyncio.run(scenario())


def test_collector_waits_for_blocked_collection_before_returning():
    started = Event()
    release = Event()
    stop_event = asyncio.Event()

    collector = MarketDataCollector(
        service=SimpleNamespace(),
        repository=SimpleNamespace(),
        symbol="XAUUSD",
        timeframe="M5",
        count=10,
    )

    def blocked_collect_once():
        started.set()

        if not release.wait(timeout=5):
            raise RuntimeError("Collection release timed out.")

        return None

    collector.collect_once = blocked_collect_once

    async def scenario():
        task = asyncio.create_task(
            collector.run(stop_event)
        )

        try:
            await wait_until_thread_starts(started)

            stop_event.set()

            await asyncio.sleep(0.05)

            # The collector coroutine must remain alive until
            # its active collection thread finishes.
            assert not task.done()

        finally:
            release.set()

        await task
        assert task.done()

    asyncio.run(scenario())


def test_candle_clock_calibration_waits_for_blocked_mt5_call():
    started = Event()
    release = Event()
    stop_event = asyncio.Event()

    class Adapter:
        def get_active_candle_clock_sample(self, symbol):
            assert symbol == "XAUUSD"
            started.set()

            if not release.wait(timeout=5):
                raise RuntimeError("MT5 release timed out.")

            return {"sample": "released"}

    class Calibrator:
        def __init__(self):
            self.observed = []

        def observe(self, **sample):
            self.observed.append(sample)

            return SimpleNamespace(
                status="VERIFIED",
                symbol="XAUUSD",
                broker_server="Test-Server",
                offset_seconds=7200,
                uncertainty_seconds=0.05,
                sample_count=5,
                reason=None,
            )

    calibrator = Calibrator()

    async def scenario():
        task = asyncio.create_task(
            _candle_clock_calibration_loop(
                Adapter(),
                calibrator,
                "XAUUSD",
                stop_event,
            )
        )

        try:
            await wait_until_thread_starts(started)

            stop_event.set()

            await asyncio.sleep(0.05)
            assert not task.done()

        finally:
            release.set()

        await task

        assert task.done()
        assert calibrator.observed == [
            {"sample": "released"}
        ]

    asyncio.run(scenario())


def test_ntp_monitor_waits_for_blocked_verification():
    started = Event()
    release = Event()
    stop_event = asyncio.Event()

    class Verifier:
        def verify(self):
            started.set()

            if not release.wait(timeout=5):
                raise RuntimeError("NTP release timed out.")

            return SimpleNamespace(
                verified=True,
                offset_seconds=0.01,
                uncertainty_seconds=0.02,
                sample_count=5,
                servers=["time.cloudflare.com"],
                error=None,
            )

        def is_trusted(self):
            return True

        def last_verification(self):
            return None

    monitor = NtpVerificationMonitor(
        verifier=Verifier(),
        interval_seconds=3600,
    )

    async def scenario():
        task = asyncio.create_task(
            monitor.run(stop_event)
        )

        try:
            await wait_until_thread_starts(started)

            stop_event.set()

            await asyncio.sleep(0.05)
            assert not task.done()

        finally:
            release.set()

        await task

        assert task.done()
        assert monitor.snapshot()["trusted"] is True

    asyncio.run(scenario())
