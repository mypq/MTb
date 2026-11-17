
from __future__ import annotations

import asyncio

import pytest

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDetector,
)
from backend.market_data.broker_offset_monitor import (
    BrokerOffsetEvidenceMonitor,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
)
from backend.market_data.broker_offset_service import (
    BrokerOffsetEvidenceService,
)
from backend.market_data.ntp_time import (
    NtpSample,
    NtpTimeVerifier,
)


def make_monitor(tmp_path, interval=0.01):
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        wall_clock=lambda: 1_794_952_458.0,
        monotonic_clock=lambda: 100.0,
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )

    service = BrokerOffsetEvidenceService(
        tmp_path / "clock_history.csv",
        BrokerOffsetDetector(verifier),
        BrokerOffsetRegistry(tmp_path / "offsets.jsonl"),
    )

    return BrokerOffsetEvidenceMonitor(
        service,
        interval_seconds=interval,
    )


def test_monitor_stops_cleanly(tmp_path):
    async def scenario():
        monitor = make_monitor(tmp_path)
        stop_event = asyncio.Event()

        task = asyncio.create_task(monitor.run(stop_event))

        await asyncio.sleep(0.03)
        stop_event.set()

        await asyncio.wait_for(task, timeout=1.0)

        assert monitor.is_running is False

    asyncio.run(scenario())


def test_monitor_performs_initial_poll(tmp_path):
    async def scenario():
        monitor = make_monitor(tmp_path)
        stop_event = asyncio.Event()

        task = asyncio.create_task(monitor.run(stop_event))

        try:
            for _ in range(100):
                if monitor.snapshot().state == "ERROR":
                    break
                await asyncio.sleep(0.01)

            assert monitor.snapshot().state == "ERROR"
            assert monitor.snapshot().error is not None
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(scenario())


def test_monitor_rejects_duplicate_run(tmp_path):
    async def scenario():
        monitor = make_monitor(tmp_path)
        stop_event = asyncio.Event()

        first_task = asyncio.create_task(monitor.run(stop_event))

        try:
            for _ in range(100):
                if monitor.is_running:
                    break
                await asyncio.sleep(0.001)

            with pytest.raises(RuntimeError):
                await monitor.run(asyncio.Event())
        finally:
            stop_event.set()
            await asyncio.wait_for(first_task, timeout=1.0)

    asyncio.run(scenario())


def test_monitor_rejects_invalid_stop_event(tmp_path):
    async def scenario():
        monitor = make_monitor(tmp_path)

        with pytest.raises(TypeError):
            await monitor.run(None)

    asyncio.run(scenario())


def test_monitor_rejects_invalid_interval(tmp_path):
    for interval in (0, -1, float("inf"), float("nan"), True):
        with pytest.raises(ValueError):
            make_monitor(tmp_path, interval=interval)


def test_monitor_rejects_invalid_service():
    with pytest.raises(TypeError):
        BrokerOffsetEvidenceMonitor(None)
