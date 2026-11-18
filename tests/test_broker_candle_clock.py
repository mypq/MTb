from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from backend.market_data.broker_candle_clock import (
    BrokerCandleClockCalibrator,
)
from backend.market_data.ntp_time import NtpSample, NtpTimeVerifier


UTC = timezone.utc
OBSERVED_AT = datetime(2026, 11, 18, 4, 34, 10, tzinfo=UTC)
BROKER_OFFSET = 7200


def make_verifier(*, max_age_seconds=21600):
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        max_age_seconds=max_age_seconds,
        wall_clock=lambda: OBSERVED_AT.timestamp(),
        monotonic_clock=lambda: 1000.0,
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )

    result = verifier.verify()
    assert result.verified is True

    return verifier


def make_calibrator(
    *,
    required_samples=3,
    minimum_span_seconds=1.0,
    maximum_spread_seconds=0.5,
    maximum_sample_age_seconds=30.0,
):
    verifier = make_verifier()

    calibrator = BrokerCandleClockCalibrator(
        verifier,
        required_samples=required_samples,
        minimum_span_seconds=minimum_span_seconds,
        maximum_spread_seconds=maximum_spread_seconds,
        maximum_sample_age_seconds=maximum_sample_age_seconds,
    )

    return calibrator, verifier


def make_observation(
    *,
    symbol="XAUUSD",
    account_login=123456,
    broker_server="Demo-Server",
    offset=BROKER_OFFSET,
    observed_at=OBSERVED_AT,
    m1_adjustment=0,
    m5_adjustment=0,
):
    observed_epoch = observed_at.astimezone(UTC).timestamp()

    return {
        "symbol": symbol,
        "account_login": account_login,
        "broker_server": broker_server,
        "m1_active_open_epoch": (
            int(observed_epoch // 60) * 60
            + offset
            + m1_adjustment
        ),
        "m5_active_open_epoch": (
            int(observed_epoch // 300) * 300
            + offset
            + m5_adjustment
        ),
        "observed_at_utc": observed_at,
    }


def calibrate(
    calibrator,
    *,
    observation=None,
    monotonic_values=(10.0, 10.5, 11.0),
):
    observation = observation or make_observation()

    with patch(
        "backend.market_data.broker_candle_clock.monotonic",
        side_effect=monotonic_values,
    ):
        snapshot = None

        for _ in monotonic_values:
            snapshot = calibrator.observe(**observation)

    return snapshot


def test_calibration_requires_all_configured_samples():
    calibrator, _ = make_calibrator(required_samples=3)
    observation = make_observation()

    with patch(
        "backend.market_data.broker_candle_clock.monotonic",
        side_effect=(10.0, 10.5),
    ):
        first = calibrator.observe(**observation)
        second = calibrator.observe(**observation)

    assert first.status == "COLLECTING"
    assert first.sample_count == 1
    assert first.offset_seconds is None

    assert second.status == "COLLECTING"
    assert second.sample_count == 2
    assert second.offset_seconds is None


def test_calibration_verifies_after_stable_samples():
    calibrator, _ = make_calibrator(required_samples=3)

    snapshot = calibrate(calibrator)

    assert snapshot.status == "VERIFIED"
    assert snapshot.offset_seconds == BROKER_OFFSET
    assert snapshot.sample_count == 3
    assert snapshot.symbol == "XAUUSD"
    assert snapshot.account_login == 123456
    assert snapshot.broker_server == "Demo-Server"
    assert snapshot.uncertainty_seconds is not None


def test_calibration_rejects_untrusted_ntp():
    calibrator, verifier = make_calibrator()

    verifier.reset()

    snapshot = calibrator.observe(**make_observation())

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert snapshot.sample_count == 0
    assert "NTP" in snapshot.reason


def test_calibration_rejects_missing_ntp_verification():
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )

    calibrator = BrokerCandleClockCalibrator(verifier)

    snapshot = calibrator.observe(**make_observation())

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None


def test_calibration_rejects_naive_observation_time():
    calibrator, _ = make_calibrator()

    observation = make_observation(
        observed_at=OBSERVED_AT.replace(tzinfo=None)
    )

    snapshot = calibrator.observe(**observation)

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert "timezone-aware" in snapshot.reason


@pytest.mark.parametrize(
    ("m1_adjustment", "m5_adjustment"),
    [
        (0, 60),
        (60, 0),
        (0, 2),
    ],
)
def test_calibration_rejects_disagreeing_m1_and_m5_boundaries(
    m1_adjustment,
    m5_adjustment,
):
    calibrator, _ = make_calibrator()

    snapshot = calibrator.observe(
        **make_observation(
            m1_adjustment=m1_adjustment,
            m5_adjustment=m5_adjustment,
        )
    )

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert "disagree" in snapshot.reason


def test_calibration_rejects_offset_not_aligned_to_whole_minute():
    calibrator, _ = make_calibrator()

    snapshot = calibrator.observe(
        **make_observation(offset=BROKER_OFFSET + 30)
    )

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert "whole minute" in snapshot.reason


@pytest.mark.parametrize(
    "field_value",
    [
        0,
        -1,
        True,
        1.5,
        None,
    ],
)
def test_calibration_rejects_invalid_m1_timestamp(field_value):
    calibrator, _ = make_calibrator()

    observation = make_observation()
    observation["m1_active_open_epoch"] = field_value

    snapshot = calibrator.observe(**observation)

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None


@pytest.mark.parametrize(
    "field_value",
    [
        0,
        -1,
        True,
        1.5,
        None,
    ],
)
def test_calibration_rejects_invalid_m5_timestamp(field_value):
    calibrator, _ = make_calibrator()

    observation = make_observation()
    observation["m5_active_open_epoch"] = field_value

    snapshot = calibrator.observe(**observation)

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None


def test_calibration_rejects_incomplete_broker_identity():
    calibrator, _ = make_calibrator()

    snapshot = calibrator.observe(
        **make_observation(symbol="   ")
    )

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None


def test_calibration_rejects_unstable_offset_samples():
    calibrator, _ = make_calibrator(required_samples=3)

    first = make_observation()
    second = make_observation(offset=BROKER_OFFSET + 60)

    with patch(
        "backend.market_data.broker_candle_clock.monotonic",
        side_effect=(10.0, 10.5, 11.0),
    ):
        calibrator.observe(**first)
        calibrator.observe(**second)
        snapshot = calibrator.observe(**second)

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert "unstable" in snapshot.reason


def test_calibration_does_not_verify_before_minimum_time_span():
    calibrator, _ = make_calibrator(
        required_samples=3,
        minimum_span_seconds=3.0,
    )

    snapshot = calibrate(
        calibrator,
        monotonic_values=(10.0, 10.5, 11.0),
    )

    assert snapshot.status == "COLLECTING"
    assert snapshot.offset_seconds is None
    assert "span enough time" in snapshot.reason


@pytest.mark.parametrize(
    ("changed_field", "changed_value"),
    [
        ("symbol", "EURUSD"),
        ("account_login", 987654),
        ("broker_server", "Different-Server"),
    ],
)
def test_identity_change_discards_previous_calibration(
    changed_field,
    changed_value,
):
    calibrator, _ = make_calibrator(required_samples=3)

    verified = calibrate(calibrator)

    assert verified.status == "VERIFIED"
    assert verified.offset_seconds == BROKER_OFFSET

    changed_observation = make_observation()
    changed_observation[changed_field] = changed_value

    snapshot = calibrator.observe(**changed_observation)

    assert snapshot.status == "COLLECTING"
    assert snapshot.offset_seconds is None
    assert snapshot.sample_count == 0
    assert snapshot.reason == (
        "Broker identity changed; recalibration started."
    )


def test_losing_ntp_trust_invalidates_previous_calibration():
    calibrator, verifier = make_calibrator(required_samples=3)

    verified = calibrate(calibrator)

    assert verified.status == "VERIFIED"

    verifier.reset()

    snapshot = calibrator.snapshot()

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert snapshot.sample_count == 0
    assert "expired" in snapshot.reason


def test_stale_calibration_is_invalidated():
    calibrator, _ = make_calibrator(
        required_samples=3,
        maximum_sample_age_seconds=30.0,
    )

    verified = calibrate(calibrator)

    assert verified.status == "VERIFIED"

    with patch(
        "backend.market_data.broker_candle_clock.monotonic",
        return_value=50.0,
    ):
        snapshot = calibrator.snapshot()

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert snapshot.sample_count == 0
    assert "stale" in snapshot.reason


def test_manual_reset_clears_identity_and_calibration():
    calibrator, _ = make_calibrator(required_samples=3)

    verified = calibrate(calibrator)

    assert verified.status == "VERIFIED"

    calibrator.reset()

    snapshot = calibrator.snapshot()

    assert snapshot.status == "UNVERIFIED"
    assert snapshot.symbol is None
    assert snapshot.account_login is None
    assert snapshot.broker_server is None
    assert snapshot.offset_seconds is None
    assert snapshot.uncertainty_seconds is None
    assert snapshot.sample_count == 0
    assert snapshot.reason == "Calibration manually reset."

def test_delayed_active_m1_update_near_minute_boundary_preserves_verified_calibration():
    calibrator, _ = make_calibrator(required_samples=3)

    verified = calibrate(calibrator)
    assert verified.status == "VERIFIED"
    assert verified.offset_seconds == BROKER_OFFSET

    boundary_time = OBSERVED_AT.replace(minute=35, second=2)
    delayed_observation = make_observation(
        observed_at=boundary_time,
    )

    # MT5 has not yet received the first tick of the new M1 candle.
    delayed_observation["m1_active_open_epoch"] -= 60

    snapshot = calibrator.observe(**delayed_observation)

    assert snapshot.status == "VERIFIED"
    assert snapshot.offset_seconds == BROKER_OFFSET
    assert snapshot.sample_count == verified.sample_count
    assert "Ignored a delayed active-candle update" in snapshot.reason


def test_real_offset_change_near_minute_boundary_is_not_ignored():
    calibrator, _ = make_calibrator(required_samples=3)

    verified = calibrate(calibrator)
    assert verified.status == "VERIFIED"

    boundary_time = OBSERVED_AT.replace(minute=35, second=2)
    changed_observation = make_observation(
        observed_at=boundary_time,
        offset=BROKER_OFFSET + 60,
    )

    snapshot = calibrator.observe(**changed_observation)

    assert snapshot.status == "DEGRADED"
    assert snapshot.offset_seconds is None
    assert snapshot.sample_count == 0
