import time
from datetime import datetime, timezone
from threading import Event
from time import monotonic_ns

import pytest

from backend.mt5.broker_tick_monitor import (
    BrokerTickMonitor,
    BrokerTickState,
)
from backend.mt5.broker_tick_worker import (
    BrokerTickMonitorWorker,
)
from backend.mt5.models import (
    MT5ConnectionState,
    MT5ConnectionStatus,
)
from backend.mt5.monitor import MT5ConnectionMonitor
from backend.mt5.time_models import BrokerTimeSample


class FakeAdapter:
    def __init__(self, sample=None, error=None):
        self.sample = sample
        self.error = error
        self.requested_symbols = []

    def get_broker_time_sample(self, symbol):
        self.requested_symbols.append(symbol)

        if self.error is not None:
            raise self.error

        return self.sample


def make_sample(
    *,
    symbol="XAUUSD",
    timestamp=1794708000000,
    monotonic_ns=1_000_000_000,
):
    return BrokerTimeSample(
        symbol=symbol,
        broker_time_msc=timestamp,
        received_at_utc=datetime.now(timezone.utc),
        received_monotonic_ns=monotonic_ns,
        bid=4000.0,
        ask=4000.1,
        account_login=123456,
        broker_server="TestServer",
    )


def make_worker(
    *,
    sample=None,
    adapter_error=None,
    symbol="XAUUSD",
    interval_seconds=0.01,
):
    adapter = FakeAdapter(
        sample=sample or make_sample(),
        error=adapter_error,
    )

    monitor = BrokerTickMonitor(
        stale_after_seconds=15,
    )

    worker = BrokerTickMonitorWorker(
        adapter,
        monitor,
        symbol=symbol,
        interval_seconds=interval_seconds,
    )

    return worker, adapter, monitor


def test_worker_normalizes_symbol():
    worker, adapter, monitor = make_worker(
        symbol=" XAUUSD ",
    )

    assert worker.symbol == "XAUUSD"


def test_worker_rejects_empty_symbol():
    adapter = FakeAdapter()
    monitor = BrokerTickMonitor()

    with pytest.raises(ValueError, match="Symbol"):
        BrokerTickMonitorWorker(
            adapter,
            monitor,
            symbol="   ",
        )


@pytest.mark.parametrize(
    "interval",
    [
        0,
        -1,
        True,
        False,
        float("inf"),
        float("-inf"),
        float("nan"),
        "1",
    ],
)
def test_worker_rejects_invalid_interval(interval):
    with pytest.raises(ValueError, match="interval_seconds"):
        BrokerTickMonitorWorker(
            FakeAdapter(),
            BrokerTickMonitor(),
            symbol="XAUUSD",
            interval_seconds=interval,
        )


def test_worker_rejects_invalid_monitor():
    with pytest.raises(TypeError, match="BrokerTickMonitor"):
        BrokerTickMonitorWorker(
            FakeAdapter(),
            object(),
            symbol="XAUUSD",
        )


def test_check_once_requests_configured_symbol():
    worker, adapter, monitor = make_worker()

    worker.check_once()

    assert adapter.requested_symbols == ["XAUUSD"]


def test_check_once_records_sample_in_monitor():
    worker, adapter, monitor = make_worker(
        sample=make_sample(timestamp=1000),
    )

    snapshot = worker.check_once()

    assert snapshot.state == BrokerTickState.HEALTHY
    assert snapshot.symbol == "XAUUSD"
    assert snapshot.samples_received == 1
    assert snapshot.clock_verification == "UNVERIFIED"


def test_check_once_rejects_unexpected_symbol():
    worker, adapter, monitor = make_worker(
        sample=make_sample(symbol="EURUSD"),
    )

    with pytest.raises(ValueError, match="unexpected symbol"):
        worker.check_once()

    snapshot = monitor.snapshot(
        now_monotonic_ns=1_000_000_000,
    )

    assert snapshot.state == BrokerTickState.NO_DATA
    assert snapshot.samples_received == 0


def test_check_once_rejects_invalid_adapter_result():
    worker, adapter, monitor = make_worker()
    adapter.sample = None

    with pytest.raises(TypeError, match="invalid broker time sample"):
        worker.check_once()


def test_check_once_propagates_adapter_errors():
    worker, adapter, monitor = make_worker(
        adapter_error=RuntimeError("MT5 unavailable"),
    )

    with pytest.raises(RuntimeError, match="MT5 unavailable"):
        worker.check_once()

    assert monitor.snapshot(
        now_monotonic_ns=1_000_000_000,
    ).state == BrokerTickState.NO_DATA


def test_status_contains_monitor_and_worker_state():
    worker, adapter, monitor = make_worker()

    status = worker.status()

    assert status["state"] == "NO_DATA"
    assert status["clock_verification"] == "UNVERIFIED"
    assert status["running"] is False
    assert status["last_error"] is None
    assert status["symbol"] is None


def test_status_is_json_compatible():
    import json
    from time import monotonic_ns

    worker, adapter, monitor = make_worker(
        sample=make_sample(
            monotonic_ns=monotonic_ns(),
        ),
    )

    worker.check_once()

    encoded = json.dumps(worker.status())

    assert '"state": "HEALTHY"' in encoded
    assert '"clock_verification": "UNVERIFIED"' in encoded


def test_worker_start_is_idempotent_and_stop_works():
    worker, adapter, monitor = make_worker()

    worker.start()
    worker.start()

    try:
        deadline = time.monotonic() + 2

        while (
            not adapter.requested_symbols
            and time.monotonic() < deadline
        ):
            time.sleep(0.005)

        assert adapter.requested_symbols
        assert worker.is_running is True

    finally:
        assert worker.stop(timeout_seconds=2) is True

    assert worker.is_running is False


def test_worker_stop_before_start_succeeds():
    worker, adapter, monitor = make_worker()

    assert worker.stop() is True
    assert worker.is_running is False


@pytest.mark.parametrize(
    "timeout",
    [-1, True, False, float("inf"), float("nan"), "1"],
)
def test_worker_stop_rejects_invalid_timeout(timeout):
    worker, adapter, monitor = make_worker()

    with pytest.raises(ValueError, match="timeout_seconds"):
        worker.stop(timeout_seconds=timeout)


def test_worker_recovers_after_mt5_connection_is_restored():
    class RecoveringAdapter:
        def __init__(self):
            self.connected = False
            self.failure_seen = Event()
            self.success_seen = Event()
            self.tick_calls = 0
            self.candle_clock_connection_lost = False

        def mark_candle_clock_connection_lost(self):
            self.candle_clock_connection_lost = True

        def connection_status(self):
            state = (
                MT5ConnectionState.CONNECTED
                if self.connected
                else MT5ConnectionState.DISCONNECTED
            )

            return MT5ConnectionStatus(
                state=state,
                terminal_connected=self.connected,
                trading_allowed=False,
                message=state.value,
            )

        def connect(self):
            self.connected = True

            return MT5ConnectionStatus(
                state=MT5ConnectionState.CONNECTED,
                terminal_connected=True,
                trading_allowed=False,
                message="Reconnected",
            )

        def get_broker_time_sample(self, symbol):
            if not self.connected:
                self.failure_seen.set()
                raise RuntimeError("MT5 connection unavailable")

            self.tick_calls += 1
            self.success_seen.set()

            return make_sample(
                symbol=symbol,
                timestamp=1794708000000 + self.tick_calls,
                monotonic_ns=monotonic_ns(),
            )

    adapter = RecoveringAdapter()
    monitor = BrokerTickMonitor(stale_after_seconds=15)

    worker = BrokerTickMonitorWorker(
        adapter,
        monitor,
        symbol="XAUUSD",
        interval_seconds=0.01,
    )

    connection_monitor = MT5ConnectionMonitor(
        adapter,
        interval_seconds=60,
    )

    worker.start()

    try:
        assert adapter.failure_seen.wait(timeout=2)

        deadline = time.monotonic() + 2

        while (
            worker.last_error != "RuntimeError"
            and time.monotonic() < deadline
        ):
            Event().wait(0.005)

        assert worker.last_error == "RuntimeError"
        assert monitor.snapshot(
            now_monotonic_ns=monotonic_ns(),
        ).samples_received == 0

        connection_status = connection_monitor.check_once()

        assert connection_status.state == (
            MT5ConnectionState.CONNECTED
        )
        assert adapter.connected is True
        assert adapter.candle_clock_connection_lost is True

        assert adapter.success_seen.wait(timeout=2)

        deadline = time.monotonic() + 2

        while (
            (
                worker.last_error is not None
                or monitor.snapshot(
                    now_monotonic_ns=monotonic_ns(),
                ).samples_received == 0
            )
            and time.monotonic() < deadline
        ):
            Event().wait(0.005)

        status = worker.status()

        assert status["running"] is True
        assert status["state"] == "HEALTHY"
        assert status["samples_received"] >= 1
        assert status["last_error"] is None
        assert status["clock_verification"] == "UNVERIFIED"

    finally:
        assert worker.stop(timeout_seconds=2) is True


def test_worker_does_not_claim_broker_clock_is_verified():
    worker, adapter, monitor = make_worker(
        sample=make_sample(),
    )

    worker.check_once()

    assert worker.status()["clock_verification"] == "UNVERIFIED"
