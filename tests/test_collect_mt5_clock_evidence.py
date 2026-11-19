from __future__ import annotations

import pytest

from backend.market_data.ntp_time import (
    NtpSample,
    NtpTimeVerifier,
)
from scripts.diagnostics.collect_mt5_clock_evidence import (
    resolve_ntp_state,
    track_update,
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


def test_failed_refresh_immediately_invalidates_previous_success():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True
    assert verifier.is_trusted() is True

    clock.advance(60)

    def failed_query(server):
        raise OSError("UDP 123 unavailable")

    verifier._query = failed_query
    failed_attempt = verifier.verify()

    result, trusted, error = resolve_ntp_state(
        verifier, failed_attempt
    )

    assert failed_attempt.verified is False
    assert result is None
    assert trusted is False
    assert error
    assert verifier.last_verification() is None


def test_failed_refresh_does_not_preserve_previous_verification():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True

    clock.advance(60)

    verifier._query = lambda server: (_ for _ in ()).throw(
        OSError("NTP unavailable")
    )

    failed_attempt = verifier.verify()

    result, trusted, _ = resolve_ntp_state(
        verifier, failed_attempt
    )

    assert result is None
    assert trusted is False

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


def test_refresh_exception_invalidates_previous_success():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True

    clock.advance(60)

    result, trusted, error = resolve_ntp_state(
        verifier,
        None,
        "OSError: UDP 123 unavailable",
    )

    assert result is None
    assert trusted is False
    assert error == "OSError: UDP 123 unavailable"
    assert verifier.last_verification() is None


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


def test_first_market_observation_is_initial():
    status, changed_at, unchanged_seconds = track_update(
        previous_value=None,
        current_value=12345,
        now_monotonic=100.0,
        previous_change_monotonic=None,
    )

    assert status == "INITIAL"
    assert changed_at == 100.0
    assert unchanged_seconds == 0.0


def test_market_observation_detects_changed_value():
    status, changed_at, unchanged_seconds = track_update(
        previous_value=12345,
        current_value=12346,
        now_monotonic=105.0,
        previous_change_monotonic=100.0,
    )

    assert status == "UPDATED"
    assert changed_at == 105.0
    assert unchanged_seconds == 0.0


def test_market_observation_detects_unchanged_value():
    status, changed_at, unchanged_seconds = track_update(
        previous_value=12345,
        current_value=12345,
        now_monotonic=112.5,
        previous_change_monotonic=100.0,
    )

    assert status == "UNCHANGED"
    assert changed_at == 100.0
    assert unchanged_seconds == pytest.approx(12.5)


def test_market_observation_does_not_reset_unchanged_duration():
    first = track_update(
        previous_value=12345,
        current_value=12345,
        now_monotonic=110.0,
        previous_change_monotonic=100.0,
    )

    second = track_update(
        previous_value=12345,
        current_value=12345,
        now_monotonic=115.0,
        previous_change_monotonic=first[1],
    )

    assert first[0] == "UNCHANGED"
    assert first[2] == pytest.approx(10.0)
    assert second[0] == "UNCHANGED"
    assert second[1] == 100.0
    assert second[2] == pytest.approx(15.0)


def test_missing_market_observation_is_no_data():
    status, changed_at, unchanged_seconds = track_update(
        previous_value=12345,
        current_value=None,
        now_monotonic=110.0,
        previous_change_monotonic=100.0,
    )

    assert status == "NO_DATA"
    assert changed_at == 100.0
    assert unchanged_seconds == ""


def test_market_observation_with_no_previous_change_time():
    status, changed_at, unchanged_seconds = track_update(
        previous_value=12345,
        current_value=12345,
        now_monotonic=110.0,
        previous_change_monotonic=None,
    )

    assert status == "UNCHANGED"
    assert changed_at == 110.0
    assert unchanged_seconds == 0.0

def test_successful_verification_is_preserved_without_refresh():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()

    result, trusted, error = resolve_ntp_state(
        verifier,
        first,
    )

    assert trusted is True

    clock.advance(10)

    result, trusted, error = resolve_ntp_state(
        verifier,
        None,
    )

    assert trusted is True
    assert result is first
    assert error == ""


def test_successful_verification_is_preserved_between_refreshes():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()

    clock.advance(60)

    result, trusted, error = resolve_ntp_state(
        verifier,
        None,
    )

    assert trusted is True
    assert result is first
    assert result.offset_seconds == first.offset_seconds
    assert error == ""


def test_no_refresh_without_previous_success_is_not_trusted():
    clock = FakeClock()
    verifier = make_verifier(clock)

    result, trusted, error = resolve_ntp_state(
        verifier,
        None,
    )

    assert result is None
    assert trusted is False
    assert error != ""


def test_expired_verification_is_not_reused_without_refresh():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True

    clock.advance(6 * 60 * 60 + 1)

    result, trusted, error = resolve_ntp_state(
        verifier,
        None,
    )

    assert result is None
    assert trusted is False
    assert error != ""


def test_failed_refresh_still_invalidates_previous_success():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True

    clock.advance(60)

    def failed_query(server):
        raise OSError("NTP server unavailable")

    verifier._query = failed_query
    failed_attempt = verifier.verify()

    result, trusted, error = resolve_ntp_state(
        verifier,
        failed_attempt,
    )

    assert failed_attempt.verified is False
    assert result is None
    assert trusted is False
    assert error != ""
    assert verifier.last_verification() is None


def test_refresh_exception_still_invalidates_previous_success():
    clock = FakeClock()
    verifier = make_verifier(clock)

    first = verifier.verify()
    assert first.verified is True

    clock.advance(60)

    result, trusted, error = resolve_ntp_state(
        verifier,
        None,
        "TimeoutError: NTP request timed out",
    )

    assert result is None
    assert trusted is False
    assert error == "TimeoutError: NTP request timed out"
    assert verifier.last_verification() is None
