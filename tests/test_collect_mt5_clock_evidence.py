
from __future__ import annotations

import pytest

from backend.market_data.ntp_time import (
    NtpSample,
    NtpTimeVerifier,
)
from scripts.diagnostics.collect_mt5_clock_evidence import (
    resolve_ntp_state,
)


class FakeClock:
    def __init__(self):
        self.wall = 1_800_000_000.0
        self.monotonic = 1000.0

    def time(self):
        return self.wall

    def monotonic_time(self):
        return self.monotonic

    def advance(self, seconds):
        self.wall += seconds
        self.monotonic += seconds


def valid_samples():
    return {
        "time.google.com": NtpSample(
            server="time.google.com",
            offset_seconds=0.05,
            round_trip_seconds=0.08,
            root_dispersion_seconds=0.01,
        ),
        "time.cloudflare.com": NtpSample(
            server="time.cloudflare.com",
            offset_seconds=0.06,
            round_trip_seconds=0.10,
            root_dispersion_seconds=0.01,
        ),
        "pool.ntp.org": NtpSample(
            server="pool.ntp.org",
            offset_seconds=0.04,
            round_trip_seconds=0.12,
            root_dispersion_seconds=0.01,
        ),
    }


def make_verifier(clock):
    samples = valid_samples()

    def query(server):
        return samples[server]

    return NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=query,
    )


def test_successful_verification_is_used():
    clock = FakeClock()
    verifier = make_verifier(clock)

    attempt = verifier.verify()

    result, trusted, error = resolve_ntp_state(
        verifier, attempt
    )

    assert trusted is True
    assert result is attempt
    assert error == ""


def test_failed_refresh_keeps_previous_success():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True

    clock.advance(60)

    def failed_query(server):
        raise OSError("UDP 123 unavailable")

    verifier._query = failed_query

    failed_attempt = verifier.verify()

    result, trusted, error = resolve_ntp_state(
        verifier, failed_attempt
    )

    assert failed_attempt.verified is False
    assert trusted is True
    assert result is first
    assert result.offset_seconds == pytest.approx(0.05)
    assert "Fewer than three valid NTP samples" in error


def test_failed_refresh_does_not_refresh_verification_age():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    clock.advance(60)

    verifier._query = lambda server: (_ for _ in ()).throw(
        OSError("NTP unavailable")
    )

    failed_attempt = verifier.verify()

    result, trusted, _ = resolve_ntp_state(
        verifier, failed_attempt
    )

    assert trusted is True
    assert result is first

    clock.advance(6 * 60 * 60)

    result, trusted, _ = resolve_ntp_state(
        verifier, failed_attempt
    )

    assert result is None
    assert trusted is False


def test_first_failed_verification_has_no_trusted_fallback():
    clock = FakeClock()
    verifier = make_verifier(clock)

    verifier._query = lambda server: (_ for _ in ()).throw(
        OSError("NTP unavailable")
    )

    failed_attempt = verifier.verify()

    result, trusted, error = resolve_ntp_state(
        verifier, failed_attempt
    )

    assert result is None
    assert trusted is False
    assert error


def test_expired_previous_verification_is_not_reused():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True

    clock.advance(6 * 60 * 60 + 1)

    result, trusted, _ = resolve_ntp_state(
        verifier, None, "NTP refresh failed"
    )

    assert result is None
    assert trusted is False


def test_refresh_exception_preserves_previous_success():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    clock.advance(60)

    result, trusted, error = resolve_ntp_state(
        verifier,
        None,
        "OSError: UDP 123 unavailable",
    )

    assert trusted is True
    assert result is first
    assert error == "OSError: UDP 123 unavailable"


def test_successful_new_verification_replaces_previous_one():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    clock.advance(60)

    second = verifier.verify()

    result, trusted, error = resolve_ntp_state(
        verifier, second
    )

    assert trusted is True
    assert result is second
    assert result is not first
    assert error == ""
