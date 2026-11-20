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


def test_root_delay_is_included_in_conservative_uncertainty():
    clock = FakeClock()
    samples = {
        name: NtpSample(
            server=name,
            offset_seconds=0.05,
            round_trip_seconds=0.08,
            root_dispersion_seconds=0.01,
            root_delay_seconds=0.40,
        )
        for name in valid_samples()
    }

    result = make_verifier(clock, samples).verify()

    assert result.verified is True
    assert result.uncertainty_seconds == pytest.approx(0.25)


def test_negative_root_delay_is_counted_by_absolute_magnitude():
    clock = FakeClock()
    samples = {
        name: NtpSample(
            server=name,
            offset_seconds=0.05,
            round_trip_seconds=0.08,
            root_dispersion_seconds=0.01,
            root_delay_seconds=-0.40,
        )
        for name in valid_samples()
    }

    result = make_verifier(clock, samples).verify()

    assert result.verified is True
    assert result.uncertainty_seconds == pytest.approx(0.25)


def test_root_delay_outside_limit_invalidates_verification():
    clock = FakeClock()
    samples = {
        name: NtpSample(
            server=name,
            offset_seconds=0.05,
            round_trip_seconds=0.08,
            root_dispersion_seconds=0.01,
            root_delay_seconds=2.01,
        )
        for name in valid_samples()
    }

    verifier = make_verifier(clock, samples)

    assert verifier.verify().verified is False
    assert verifier.last_verification() is None


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
        NtpTimeVerifier(max_clock_jump_seconds=0)

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
@pytest.mark.parametrize(
    "offset_seconds",
    [-120.0, 120.0],
)
def test_large_positive_and_negative_clock_skew_are_rejected(
    offset_seconds: float,
) -> None:
    clock = FakeClock()
    samples = valid_samples()

    samples = {
        name: NtpSample(
            server=name,
            offset_seconds=offset_seconds,
            round_trip_seconds=0.05,
            root_dispersion_seconds=0.01,
        )
        for name in samples
    }

    verifier = make_verifier(clock, samples)

    result = verifier.verify()

    assert result.verified is False
    assert verifier.is_trusted() is False
    assert verifier.last_verification() is None



def test_nts_quorum_gate_is_required_for_successful_verification():
    clock = FakeClock()
    verifier = NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=lambda server: valid_samples()[server],
        nts_authentication_check=lambda: True,
    )

    result = verifier.verify()

    assert result.verified is True
    assert result.nts_quorum_verified is True
    assert verifier.requires_nts_quorum is True
    assert verifier.is_trusted() is True


def test_missing_nts_quorum_rejects_otherwise_valid_ntp_samples():
    clock = FakeClock()
    verifier = NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=lambda server: valid_samples()[server],
        nts_authentication_check=lambda: False,
    )

    result = verifier.verify()

    assert result.verified is False
    assert result.nts_quorum_verified is False
    assert "NTS authentication quorum" in result.error
    assert verifier.is_trusted() is False


def test_nts_quorum_check_exception_rejects_verification():
    clock = FakeClock()

    def unavailable_chrony():
        raise RuntimeError("Chrony unavailable")

    verifier = NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=lambda server: valid_samples()[server],
        nts_authentication_check=unavailable_chrony,
    )

    result = verifier.verify()

    assert result.verified is False
    assert "Chrony NTS authentication quorum check failed" in result.error
    assert "Chrony unavailable" in result.error
    assert verifier.is_trusted() is False


def test_failed_nts_quorum_reverification_invalidates_previous_trust():
    clock = FakeClock()
    gate = {"verified": True}
    verifier = NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=lambda server: valid_samples()[server],
        nts_authentication_check=lambda: gate["verified"],
    )

    assert verifier.verify().verified is True
    assert verifier.is_trusted() is True

    gate["verified"] = False
    result = verifier.verify()

    assert result.verified is False
    assert verifier.is_trusted() is False
    assert verifier.last_verification() is None


def test_non_callable_nts_authentication_check_is_rejected():
    with pytest.raises(ValueError, match="must be callable"):
        NtpTimeVerifier(nts_authentication_check=True)


@pytest.mark.parametrize("adjustment", [2.0, -2.0])
def test_wall_clock_jump_during_server_query_rejects_entire_verification(
    adjustment: float,
):
    clock = FakeClock()
    samples = valid_samples()

    def query(server):
        if server == "time.cloudflare.com":
            clock.wall += adjustment
        return samples[server]

    verifier = NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=query,
    )

    result = verifier.verify()

    assert result.verified is False
    assert "wall clock jump" in result.error.lower()
    assert verifier.is_trusted() is False
    assert verifier.last_verification() is None


def test_wall_clock_jump_between_server_requests_rejects_verification():
    clock = FakeClock()
    samples = valid_samples()
    wall_reads = {"count": 0}

    def wall_clock():
        wall_reads["count"] += 1
        if wall_reads["count"] == 4:
            clock.wall += 2.0
        return clock.wall

    verifier = NtpTimeVerifier(
        wall_clock=wall_clock,
        monotonic_clock=clock.monotonic_time,
        query=lambda server: samples[server],
    )

    result = verifier.verify()

    assert result.verified is False
    assert "wall clock jump" in result.error.lower()
    assert verifier.is_trusted() is False


def test_normal_wall_and_monotonic_progress_still_verifies():
    clock = FakeClock()
    samples = valid_samples()

    def query(server):
        clock.advance(0.05)
        return samples[server]

    verifier = NtpTimeVerifier(
        wall_clock=clock.time,
        monotonic_clock=clock.monotonic_time,
        query=query,
    )

    result = verifier.verify()

    assert result.verified is True
    assert verifier.is_trusted() is True


@pytest.mark.parametrize("adjustment", [2.0, -2.0])
def test_wall_clock_jump_after_success_invalidates_existing_trust(
    adjustment: float,
):
    clock = FakeClock()
    verifier = make_verifier(clock)

    assert verifier.verify().verified is True
    assert verifier.is_trusted() is True

    clock.wall += adjustment

    assert verifier.is_trusted() is False
    assert verifier.last_verification() is None
