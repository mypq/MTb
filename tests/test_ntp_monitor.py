from __future__ import annotations

import asyncio

import pytest

from backend.market_data.ntp_monitor import NtpVerificationMonitor
from backend.market_data.ntp_time import NtpVerification


class FakeVerifier:
    def __init__(
        self,
        result,
        *,
        trusted=False,
        last_success=None,
    ):
        self.result = result
        self.trusted = trusted
        self.success = last_success
        self.calls = 0

    def verify(self):
        self.calls += 1
        return self.result

    def is_trusted(self):
        return self.trusted

    def last_verification(self):
        return self.success if self.trusted else None


def successful_result():
    return NtpVerification(
        verified=True,
        checked_at_unix=1_800_000_000.0,
        offset_seconds=0.05,
        uncertainty_seconds=0.06,
        sample_count=3,
        servers=(
            "time.google.com",
            "time.cloudflare.com",
            "pool.ntp.org",
        ),
    )


def failed_result():
    return NtpVerification(
        verified=False,
        checked_at_unix=None,
        offset_seconds=None,
        uncertainty_seconds=None,
        sample_count=0,
        servers=(),
        error="UDP 123 unavailable",
    )


def test_monitor_verifies_immediately_on_start():
    async def run():
        stop_event = asyncio.Event()
        result = successful_result()

        verifier = FakeVerifier(
            result,
            trusted=True,
            last_success=result,
        )

        monitor = NtpVerificationMonitor(
            verifier=verifier,
            interval_seconds=3600,
            wall_clock=lambda: 1_800_000_001.0,
        )

        task = asyncio.create_task(monitor.run(stop_event))

        while verifier.calls == 0:
            await asyncio.sleep(0.01)

        stop_event.set()
        await task

        assert verifier.calls == 1

        snapshot = monitor.snapshot()

        assert snapshot["trusted"] is True
        assert snapshot["last_attempt"]["verified"] is True
        assert snapshot["last_success"]["verified"] is True
        assert snapshot["last_attempt_at_unix"] == 1_800_000_001.0

    asyncio.run(run())


def test_failed_attempt_preserves_previous_trusted_success():
    async def run():
        stop_event = asyncio.Event()
        previous_success = successful_result()

        verifier = FakeVerifier(
            failed_result(),
            trusted=True,
            last_success=previous_success,
        )

        monitor = NtpVerificationMonitor(
            verifier=verifier,
            interval_seconds=3600,
        )

        task = asyncio.create_task(monitor.run(stop_event))

        while verifier.calls == 0:
            await asyncio.sleep(0.01)

        stop_event.set()
        await task

        snapshot = monitor.snapshot()

        assert snapshot["trusted"] is True
        assert snapshot["last_attempt"]["verified"] is False
        assert snapshot["last_success"]["verified"] is True
        assert snapshot["last_error"] == "UDP 123 unavailable"

    asyncio.run(run())


def test_monitor_rejects_invalid_stop_event():
    monitor = NtpVerificationMonitor(
        verifier=FakeVerifier(successful_result()),
    )

    async def run():
        with pytest.raises(TypeError):
            await monitor.run(None)

    asyncio.run(run())


def test_monitor_rejects_invalid_interval():
    with pytest.raises(ValueError):
        NtpVerificationMonitor(interval_seconds=0)

    with pytest.raises(ValueError):
        NtpVerificationMonitor(interval_seconds=-1)

    with pytest.raises(ValueError):
        NtpVerificationMonitor(interval_seconds=float("nan"))
