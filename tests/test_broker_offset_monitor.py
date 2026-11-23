from __future__ import annotations

import asyncio
import logging
import os
import time
import threading

import pytest

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDetector,
)
from backend.market_data.broker_offset_monitor import (
    BrokerOffsetEvidenceMonitor,
    ClockHistoryFreshnessSnapshot,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
)
from backend.market_data.broker_offset_service import (
    BrokerOffsetEvidenceService,
    BrokerOffsetServiceSnapshot,
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

    history_path = tmp_path / "clock_history.csv"
    history_path.write_text("invalid\\n", encoding="utf-8")

    service = BrokerOffsetEvidenceService(
        history_path,
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


def test_unexpected_poll_failure_is_visible_in_monitor_snapshot(
    tmp_path,
    monkeypatch,
):
    async def scenario():
        monitor = make_monitor(tmp_path)
        monitor._clock_history_path.write_text("fresh invalid\\n", encoding="utf-8")

        def fail_poll():
            raise RuntimeError("Simulated unexpected poll failure.")

        monkeypatch.setattr(
            monitor._service,
            "poll_once",
            fail_poll,
        )

        stop_event = asyncio.Event()
        task = asyncio.create_task(monitor.run(stop_event))

        try:
            for _ in range(100):
                snapshot = monitor.snapshot()
                if snapshot.state == "ERROR":
                    break
                await asyncio.sleep(0.01)

            snapshot = monitor.snapshot()

            assert snapshot.state == "ERROR"
            assert snapshot.error == (
                "RuntimeError: Simulated unexpected poll failure."
            )
            assert snapshot.last_decision is None
            assert monitor.is_running is True
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=1.0)

        assert monitor.is_running is False

    asyncio.run(scenario())


@pytest.mark.parametrize(
    "state",
    [
        "BASELINE_RECORDED",
        "CONFIRMATION_RECORDED",
        "OFFSET_CHANGE_RECORDED",
    ],
)
def test_monitor_logs_all_recorded_event_states(
    tmp_path,
    monkeypatch,
    caplog,
    state,
):
    async def scenario():
        monitor = make_monitor(tmp_path)
        stop_event = asyncio.Event()

        expected_snapshot = BrokerOffsetServiceSnapshot(
            state=state,
            samples_processed=3,
            last_decision=None,
            last_record=None,
            error=None,
        )

        def return_recorded_snapshot():
            stop_event.set()
            return expected_snapshot

        monkeypatch.setattr(
            monitor._service,
            "poll_once",
            return_recorded_snapshot,
        )

        await monitor.run(stop_event)

        assert monitor.snapshot() == expected_snapshot
        assert monitor.is_running is False

    with caplog.at_level(
        logging.INFO,
        logger="backend.market_data.broker_offset_monitor",
    ):
        asyncio.run(scenario())

    assert any(
        "Broker offset event recorded" in record.message
        and f"state={state}" in record.message
        for record in caplog.records
    )


def test_monitor_reports_missing_history_without_polling(tmp_path, monkeypatch):
    async def scenario():
        monitor = make_monitor(tmp_path)
        monitor._clock_history_path.unlink()
        stop_event = asyncio.Event()

        def unexpected_poll():
            raise AssertionError("Evidence polling must pause for a missing file.")

        monkeypatch.setattr(monitor._service, "poll_once", unexpected_poll)
        task = asyncio.create_task(monitor.run(stop_event))

        try:
            for _ in range(100):
                if monitor.source_freshness().state == "MISSING":
                    break
                await asyncio.sleep(0.005)

            assert monitor.source_freshness().state == "MISSING"
            assert monitor.snapshot().state == "NOT_STARTED"
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(scenario())


def test_monitor_pauses_stale_history_and_resumes_when_file_updates(
    tmp_path,
    monkeypatch,
):
    async def scenario():
        monitor = make_monitor(tmp_path)
        monitor._clock_history_max_age_seconds = 5
        history_path = monitor._clock_history_path
        history_path.write_text("header\\n", encoding="utf-8")
        old_time = time.time() - 20
        os.utime(history_path, (old_time, old_time))

        stop_event = asyncio.Event()
        poll_calls = 0

        def record_poll():
            nonlocal poll_calls
            poll_calls += 1
            stop_event.set()
            return BrokerOffsetServiceSnapshot(
                state="COLLECTING",
                samples_processed=0,
                last_decision=None,
                last_record=None,
                error=None,
            )

        monkeypatch.setattr(monitor._service, "poll_once", record_poll)
        task = asyncio.create_task(monitor.run(stop_event))

        try:
            await asyncio.sleep(0.03)
            assert monitor.source_freshness().state == "STALE"
            assert poll_calls == 0

            history_path.write_text("header resumed\\n", encoding="utf-8")
            for _ in range(100):
                if poll_calls:
                    break
                await asyncio.sleep(0.01)
            assert poll_calls == 1
            assert monitor.source_freshness().state == "FRESH"
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=1.0)

    asyncio.run(scenario())


def test_monitor_rejects_invalid_clock_history_max_age(tmp_path):
    monitor = make_monitor(tmp_path)

    for value in (0, -1, True, 1.5):
        with pytest.raises(ValueError):
            BrokerOffsetEvidenceMonitor(
                monitor._service,
                clock_history_max_age_seconds=value,
            )



def test_monitor_reports_unavailable_history_without_polling(
    tmp_path,
    monkeypatch,
):
    async def scenario():
        monitor = make_monitor(tmp_path)
        stop_event = asyncio.Event()

        def unavailable():
            return ClockHistoryFreshnessSnapshot(
                state="UNAVAILABLE",
                age_seconds=None,
                last_modified_unix=None,
                max_age_seconds=180,
                error="PermissionError: access denied",
            )

        def unexpected_poll():
            raise AssertionError(
                "Evidence polling must pause for an unavailable file."
            )

        monkeypatch.setattr(
            monitor,
            "_read_source_freshness",
            unavailable,
        )
        monkeypatch.setattr(
            monitor._service,
            "poll_once",
            unexpected_poll,
        )

        task = asyncio.create_task(monitor.run(stop_event))
        try:
            await asyncio.sleep(0.03)
            assert monitor.snapshot().state == "NOT_STARTED"
            assert monitor.is_running is True
        finally:
            stop_event.set()
            await asyncio.wait_for(task, timeout=1.0)

        assert monitor.is_running is False

    asyncio.run(scenario())


def test_monitor_poll_does_not_block_asyncio_event_loop(
    tmp_path,
    monkeypatch,
):
    async def scenario():
        monitor = make_monitor(tmp_path)
        monitor._clock_history_path.write_text(
            "fresh history",
            encoding="utf-8",
        )

        poll_started = threading.Event()
        release_poll = threading.Event()
        heartbeat_ran = asyncio.Event()
        stop_event = asyncio.Event()

        def blocking_poll():
            poll_started.set()
            if not release_poll.wait(timeout=2.0):
                raise TimeoutError("Test did not release the blocked poll.")
            return BrokerOffsetServiceSnapshot(
                state="COLLECTING",
                samples_processed=0,
                last_decision=None,
                last_record=None,
                error=None,
            )

        monkeypatch.setattr(
            monitor._service,
            "poll_once",
            blocking_poll,
        )

        task = asyncio.create_task(monitor.run(stop_event))

        try:
            assert await asyncio.to_thread(
                poll_started.wait,
                1.0,
            )

            async def heartbeat():
                await asyncio.sleep(0.02)
                heartbeat_ran.set()

            await asyncio.wait_for(heartbeat(), timeout=0.25)
            assert heartbeat_ran.is_set()
        finally:
            stop_event.set()
            release_poll.set()
            await asyncio.wait_for(task, timeout=1.0)

        assert monitor.snapshot().state == "COLLECTING"
        assert monitor.is_running is False

    asyncio.run(scenario())


def test_source_freshness_reports_os_error(tmp_path, monkeypatch):
    monitor = make_monitor(tmp_path)
    original_stat = type(monitor._clock_history_path).stat

    def fail_target_stat(path, *args, **kwargs):
        if path == monitor._clock_history_path:
            raise PermissionError("access denied")
        return original_stat(path, *args, **kwargs)

    monkeypatch.setattr(
        type(monitor._clock_history_path),
        "stat",
        fail_target_stat,
    )

    snapshot = monitor.source_freshness()

    assert snapshot.state == "UNAVAILABLE"
    assert snapshot.age_seconds is None
    assert snapshot.last_modified_unix is None
    assert snapshot.error == "PermissionError: access denied"
