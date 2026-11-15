from datetime import datetime, timezone

import pytest

from backend.mt5.broker_tick_monitor import (
    BrokerTickMonitor,
    BrokerTickState,
)
from backend.mt5.time_models import BrokerTimeSample


BASE_TIME_NS = 1_000_000_000


def make_sample(
    *,
    timestamp=1794729600123,
    received_ns=BASE_TIME_NS,
    symbol="XAUUSD",
    login=123456,
    server="Demo-Server",
):
    return BrokerTimeSample(
        symbol=symbol,
        broker_time_msc=timestamp,
        received_at_utc=datetime(
            2026, 11, 15, 12, 0, 1,
            tzinfo=timezone.utc,
        ),
        received_monotonic_ns=received_ns,
        bid=4100.10,
        ask=4100.20,
        account_login=login,
        broker_server=server,
    )


def test_first_sample_establishes_healthy_baseline():
    monitor = BrokerTickMonitor()

    result = monitor.observe(make_sample())

    assert result.state == BrokerTickState.HEALTHY
    assert result.last_broker_time_msc == 1794729600123
    assert result.samples_received == 1
    assert result.timestamp_advances == 0
    assert result.clock_verification == "UNVERIFIED"


def test_newer_timestamp_is_healthy():
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample())

    result = monitor.observe(
        make_sample(
            timestamp=1794729601123,
            received_ns=BASE_TIME_NS + 1_000_000_000,
        )
    )

    assert result.state == BrokerTickState.HEALTHY
    assert result.timestamp_advances == 1
    assert result.last_broker_time_msc == 1794729601123
    assert result.seconds_since_progress == 0


def test_repeated_timestamp_does_not_refresh_progress_time():
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample())

    result = monitor.observe(
        make_sample(
            received_ns=BASE_TIME_NS + 2_000_000_000,
        )
    )

    assert result.state == BrokerTickState.TIMESTAMP_REPEATED
    assert result.repeated_timestamps == 1
    assert result.seconds_since_progress == 2


def test_repeated_ticks_eventually_become_stale():
    monitor = BrokerTickMonitor(stale_after_seconds=5)
    monitor.observe(make_sample())

    monitor.observe(
        make_sample(
            received_ns=BASE_TIME_NS + 2_000_000_000,
        )
    )

    result = monitor.snapshot(
        now_monotonic_ns=BASE_TIME_NS + 5_000_000_000,
    )

    assert result.state == BrokerTickState.STALE
    assert result.seconds_since_progress == 5


def test_newer_timestamp_restores_healthy_state():
    monitor = BrokerTickMonitor(stale_after_seconds=5)
    monitor.observe(make_sample())

    stale = monitor.snapshot(
        now_monotonic_ns=BASE_TIME_NS + 5_000_000_000,
    )
    assert stale.state == BrokerTickState.STALE

    recovered = monitor.observe(
        make_sample(
            timestamp=1794729602123,
            received_ns=BASE_TIME_NS + 6_000_000_000,
        )
    )

    assert recovered.state == BrokerTickState.HEALTHY
    assert recovered.seconds_since_progress == 0


def test_older_timestamp_is_reported_as_regression():
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample(timestamp=2000))

    result = monitor.observe(
        make_sample(
            timestamp=1999,
            received_ns=BASE_TIME_NS + 1_000_000_000,
        )
    )

    assert result.state == BrokerTickState.TIMESTAMP_REGRESSION
    assert result.timestamp_regressions == 1
    assert result.last_broker_time_msc == 2000


def test_regression_does_not_replace_highest_timestamp():
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample(timestamp=2000))

    monitor.observe(
        make_sample(
            timestamp=1900,
            received_ns=BASE_TIME_NS + 1_000_000_000,
        )
    )

    result = monitor.observe(
        make_sample(
            timestamp=2100,
            received_ns=BASE_TIME_NS + 2_000_000_000,
        )
    )

    assert result.state == BrokerTickState.HEALTHY
    assert result.last_broker_time_msc == 2100
    assert result.timestamp_advances == 1
    assert result.timestamp_regressions == 1


@pytest.mark.parametrize(
    "changed_identity",
    [
        {"symbol": "EURUSD"},
        {"login": 654321},
        {"server": "Live-Server"},
    ],
)
def test_identity_change_is_detected_without_mixing_samples(
    changed_identity,
):
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample(timestamp=2000))

    result = monitor.observe(
        make_sample(
            timestamp=1000,
            received_ns=BASE_TIME_NS + 1_000_000_000,
            **changed_identity,
        )
    )

    assert result.state == BrokerTickState.IDENTITY_CHANGED
    assert result.identity_changes == 1
    assert result.last_broker_time_msc == 1000
    assert result.clock_verification == "UNVERIFIED"


def test_new_identity_can_progress_after_change():
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample())

    monitor.observe(
        make_sample(
            symbol="EURUSD",
            received_ns=BASE_TIME_NS + 1_000_000_000,
        )
    )

    result = monitor.observe(
        make_sample(
            symbol="EURUSD",
            timestamp=1794729602123,
            received_ns=BASE_TIME_NS + 2_000_000_000,
        )
    )

    assert result.state == BrokerTickState.HEALTHY
    assert result.symbol == "EURUSD"
    assert result.identity_changes == 1
    assert result.timestamp_advances == 1


def test_empty_monitor_reports_no_data():
    monitor = BrokerTickMonitor()

    result = monitor.snapshot(
        now_monotonic_ns=BASE_TIME_NS,
    )

    assert result.state == BrokerTickState.NO_DATA
    assert result.symbol is None
    assert result.seconds_since_progress is None
    assert result.samples_received == 0


def test_reset_clears_all_monitor_state():
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample())

    monitor.observe(
        make_sample(
            timestamp=1794729602123,
            received_ns=BASE_TIME_NS + 1_000_000_000,
        )
    )

    monitor.reset()

    result = monitor.snapshot(
        now_monotonic_ns=BASE_TIME_NS + 2_000_000_000,
    )

    assert result.state == BrokerTickState.NO_DATA
    assert result.symbol is None
    assert result.last_broker_time_msc is None
    assert result.samples_received == 0
    assert result.timestamp_advances == 0


@pytest.mark.parametrize(
    "value",
    [0, -1, True, float("nan"), float("inf"), "5"],
)
def test_invalid_stale_threshold_is_rejected(value):
    with pytest.raises(ValueError):
        BrokerTickMonitor(stale_after_seconds=value)


@pytest.mark.parametrize(
    "value",
    [-1, True, 1.5, "100"],
)
def test_invalid_snapshot_monotonic_time_is_rejected(value):
    monitor = BrokerTickMonitor()

    with pytest.raises(ValueError):
        monitor.snapshot(now_monotonic_ns=value)


def test_snapshot_rejects_monotonic_time_before_last_progress():
    monitor = BrokerTickMonitor()
    monitor.observe(make_sample())

    with pytest.raises(ValueError, match="earlier than"):
        monitor.snapshot(
            now_monotonic_ns=BASE_TIME_NS - 1,
        )


def test_non_sample_input_is_rejected():
    monitor = BrokerTickMonitor()

    with pytest.raises(TypeError):
        monitor.observe(object())
