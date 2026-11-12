
from threading import Event
from unittest.mock import Mock

import pytest

from backend.mt5.models import MT5ConnectionState, MT5ConnectionStatus
from backend.mt5.monitor import MT5ConnectionMonitor


def make_status(state, message="Test status"):
    return MT5ConnectionStatus(
        state=state,
        terminal_connected=state == MT5ConnectionState.CONNECTED,
        trading_allowed=False,
        message=message,
    )


@pytest.mark.parametrize("interval", [0, -1, True, "10", float("nan"), float("inf")])
def test_invalid_monitor_interval_is_rejected(interval):
    with pytest.raises(ValueError):
        MT5ConnectionMonitor(
            adapter=Mock(),
            interval_seconds=interval,
        )


@pytest.mark.parametrize(
    "timeout",
    [-1, True, "1", float("nan"), float("inf")],
)
def test_invalid_stop_timeout_is_rejected(timeout):
    monitor = MT5ConnectionMonitor(adapter=Mock())

    with pytest.raises(ValueError):
        monitor.stop(timeout_seconds=timeout)


def test_connected_status_does_not_trigger_reconnection():
    adapter = Mock()
    adapter.connection_status.return_value = make_status(
        MT5ConnectionState.CONNECTED
    )

    monitor = MT5ConnectionMonitor(adapter)

    status = monitor.check_once()

    assert status.state == MT5ConnectionState.CONNECTED
    adapter.connect.assert_not_called()


def test_disabled_status_does_not_trigger_reconnection():
    adapter = Mock()
    adapter.connection_status.return_value = make_status(
        MT5ConnectionState.DISABLED
    )

    monitor = MT5ConnectionMonitor(adapter)

    status = monitor.check_once()

    assert status.state == MT5ConnectionState.DISABLED
    adapter.connect.assert_not_called()


@pytest.mark.parametrize(
    "initial_state",
    [
        MT5ConnectionState.DISCONNECTED,
        MT5ConnectionState.ERROR,
    ],
)
def test_disconnected_or_error_status_triggers_reconnection(initial_state):
    adapter = Mock()
    adapter.connection_status.return_value = make_status(initial_state)
    adapter.connect.return_value = make_status(
        MT5ConnectionState.CONNECTED,
        "Reconnected",
    )

    monitor = MT5ConnectionMonitor(adapter)

    status = monitor.check_once()

    assert status.state == MT5ConnectionState.CONNECTED
    adapter.connect.assert_called_once()


def test_failed_reconnection_returns_failure_status():
    adapter = Mock()
    adapter.connection_status.return_value = make_status(
        MT5ConnectionState.DISCONNECTED
    )
    adapter.connect.return_value = make_status(
        MT5ConnectionState.ERROR,
        "Reconnect failed",
    )

    monitor = MT5ConnectionMonitor(adapter)

    status = monitor.check_once()

    assert status.state == MT5ConnectionState.ERROR
    assert status.message == "Reconnect failed"
    adapter.connect.assert_called_once()


def test_stop_before_start_returns_true():
    monitor = MT5ConnectionMonitor(adapter=Mock())

    assert monitor.stop() is True
    assert monitor.is_running is False


def test_start_is_idempotent_and_stop_joins_worker():
    adapter = Mock()
    adapter.connection_status.return_value = make_status(
        MT5ConnectionState.CONNECTED
    )

    first_check_completed = Event()
    original_connection_status = adapter.connection_status

    def connection_status_side_effect():
        first_check_completed.set()
        return original_connection_status.return_value

    adapter.connection_status.side_effect = connection_status_side_effect

    monitor = MT5ConnectionMonitor(
        adapter,
        interval_seconds=60.0,
    )

    monitor.start()
    monitor.start()

    assert first_check_completed.wait(timeout=2.0)
    assert monitor.is_running is True
    adapter.connection_status.assert_called_once()

    assert monitor.stop(timeout_seconds=2.0) is True
    assert monitor.is_running is False


def test_stop_returns_false_when_worker_exceeds_timeout():
    adapter = Mock()
    worker_entered = Event()
    release_worker = Event()

    def blocking_connection_status():
        worker_entered.set()
        release_worker.wait(timeout=5.0)
        return make_status(MT5ConnectionState.CONNECTED)

    adapter.connection_status.side_effect = blocking_connection_status

    monitor = MT5ConnectionMonitor(
        adapter,
        interval_seconds=60.0,
    )

    monitor.start()

    assert worker_entered.wait(timeout=2.0)
    assert monitor.stop(timeout_seconds=0.01) is False
    assert monitor.is_running is True

    release_worker.set()

    assert monitor.stop(timeout_seconds=2.0) is True
    assert monitor.is_running is False


def test_stop_is_idempotent():
    adapter = Mock()
    adapter.connection_status.return_value = make_status(
        MT5ConnectionState.CONNECTED
    )

    monitor = MT5ConnectionMonitor(
        adapter,
        interval_seconds=60.0,
    )

    monitor.start()

    assert monitor.stop(timeout_seconds=2.0) is True
    assert monitor.stop(timeout_seconds=2.0) is True
    assert monitor.is_running is False
