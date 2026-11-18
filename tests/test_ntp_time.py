from __future__ import annotations

from datetime import datetime, timezone

import pytest

from backend.market_data.ntp_time import (
    NtpSample,
    NtpTimeVerifier,
)


class FakeClock:
    def __init__(self):
        self.wall = 1_800_000_000.0
        self.monotonic = 1000.0

    def time(self):
        return self.wall

    def monotonic_time(self):
        return self.monotonic

    def advance(self, seconds: float):
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


def make_verifier(clock, samples=None):
    sample_map = samples or valid_samples()

    def query(server):
        return sample_map[server]

    return NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=query,
    )


def test_verification_requires_three_valid_samples():
    clock = FakeClock()
    verifier = make_verifier(clock)

    result = verifier.verify()

    assert result.verified is True
    assert result.sample_count == 3
    assert result.offset_seconds == pytest.approx(0.05)
    assert verifier.is_trusted() is True


def test_offset_at_one_second_is_rejected():
    clock = FakeClock()
    samples = valid_samples()

    samples = {
        name: NtpSample(
            server=name,
            offset_seconds=1.0,
            round_trip_seconds=0.05,
            root_dispersion_seconds=0.01,
        )
        for name in samples
    }

    verifier = make_verifier(clock, samples)

    result = verifier.verify()

    assert result.verified is False
    assert verifier.is_trusted() is False


def test_offset_above_one_second_is_rejected():
    clock = FakeClock()
    samples = valid_samples()

    samples = {
        name: NtpSample(
            server=name,
            offset_seconds=1.2,
            round_trip_seconds=0.05,
            root_dispersion_seconds=0.01,
        )
        for name in samples
    }

    verifier = make_verifier(clock, samples)

    assert verifier.verify().verified is False


def test_fewer_than_three_samples_are_rejected():
    clock = FakeClock()
    samples = valid_samples()
    samples.pop("pool.ntp.org")

    verifier = make_verifier(clock, samples)

    result = verifier.verify()

    assert result.verified is False
    assert verifier.is_trusted() is False


def test_failed_reverification_immediately_invalidates_previous_trust():
    clock = FakeClock()
    verifier = make_verifier(clock)

    assert verifier.verify().verified is True
    assert verifier.is_trusted() is True

    clock.advance(60)

    def failed_query(server):
        raise OSError("UDP 123 unavailable")

    verifier._query = failed_query

    result = verifier.verify()

    assert result.verified is False
    assert verifier.is_trusted() is False
    assert verifier.last_verification() is None


def test_verification_expires_after_six_hours():
    clock = FakeClock()
    verifier = make_verifier(clock)

    assert verifier.verify().verified is True
    assert verifier.is_trusted() is True

    clock.advance(6 * 60 * 60 + 1)

    assert verifier.is_trusted() is False
    assert verifier.last_verification() is None


def test_backward_monotonic_clock_invalidates_trust():
    clock = FakeClock()
    verifier = make_verifier(clock)

    assert verifier.verify().verified is True

    clock.monotonic -= 1

    assert verifier.is_trusted() is False


def test_reset_clears_previous_verification():
    clock = FakeClock()
    verifier = make_verifier(clock)

    assert verifier.verify().verified is True

    verifier.reset()

    assert verifier.is_trusted() is False
    assert verifier.last_verification() is None


def test_invalid_configuration_is_rejected():
    with pytest.raises(ValueError):
        NtpTimeVerifier(timeout_seconds=0)

    with pytest.raises(ValueError):
        NtpTimeVerifier(max_age_seconds=-1)

    with pytest.raises(ValueError):
        NtpTimeVerifier(servers=())


def test_non_finite_sample_is_rejected():
    clock = FakeClock()
    samples = valid_samples()

    samples["time.google.com"] = NtpSample(
        server="time.google.com",
        offset_seconds=float("nan"),
        round_trip_seconds=0.1,
        root_dispersion_seconds=0.01,
    )

    verifier = make_verifier(clock, samples)

    result = verifier.verify()

    assert result.verified is False


def test_duplicate_ntp_servers_are_rejected():
    with pytest.raises(ValueError):
        NtpTimeVerifier(
            servers=(
                "time.google.com",
                "TIME.GOOGLE.COM",
                "pool.ntp.org",
            )
        )


def test_inconsistent_ntp_server_offsets_are_rejected():
    clock = FakeClock()

    samples = {
        "time.google.com": NtpSample(
            server="time.google.com",
            offset_seconds=0.0,
            round_trip_seconds=0.05,
            root_dispersion_seconds=0.01,
        ),
        "time.cloudflare.com": NtpSample(
            server="time.cloudflare.com",
            offset_seconds=0.1,
            round_trip_seconds=0.05,
            root_dispersion_seconds=0.01,
        ),
        "pool.ntp.org": NtpSample(
            server="pool.ntp.org",
            offset_seconds=0.9,
            round_trip_seconds=0.05,
            root_dispersion_seconds=0.01,
        ),
    }

    verifier = make_verifier(clock, samples)

    result = verifier.verify()

    assert result.verified is False
    assert verifier.is_trusted() is False
