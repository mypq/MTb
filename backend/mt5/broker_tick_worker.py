
from __future__ import annotations

import logging
from dataclasses import asdict
from math import isfinite
from threading import Event, Lock, Thread, current_thread
from time import monotonic_ns
from typing import TYPE_CHECKING

from backend.mt5.broker_tick_monitor import BrokerTickMonitor
from backend.mt5.time_models import BrokerTimeSample

if TYPE_CHECKING:
    from backend.mt5.adapter import MT5Adapter


logger = logging.getLogger(__name__)


class BrokerTickMonitorWorker:
    """Background worker that observes read-only MT5 quote timestamps."""

    def __init__(
        self,
        adapter: MT5Adapter,
        monitor: BrokerTickMonitor,
        *,
        symbol: str,
        interval_seconds: float = 1.0,
    ) -> None:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must be a non-empty string.")

        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (int, float))
            or not isfinite(interval_seconds)
            or interval_seconds <= 0
        ):
            raise ValueError(
                "interval_seconds must be a positive finite number."
            )

        if not isinstance(monitor, BrokerTickMonitor):
            raise TypeError(
                "monitor must be a BrokerTickMonitor instance."
            )

        self._adapter = adapter
        self._monitor = monitor
        self._symbol = symbol.strip()
        self._interval_seconds = float(interval_seconds)

        self._stop_event = Event()
        self._lifecycle_lock = Lock()
        self._status_lock = Lock()

        self._thread: Thread | None = None
        self._last_error: str | None = None

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

    @property
    def is_running(self) -> bool:
        with self._lifecycle_lock:
            return (
                self._thread is not None
                and self._thread.is_alive()
            )

    @property
    def last_error(self) -> str | None:
        with self._status_lock:
            return self._last_error

    def start(self) -> None:
        """Start the worker once."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop_event = Event()
            self._thread = Thread(
                target=self._run,
                name="BrokerTickMonitorWorker",
                daemon=True,
            )
            self._thread.start()

        logger.info(
            "Broker tick monitor worker started: "
            "symbol=%s interval=%.2f seconds.",
            self._symbol,
            self._interval_seconds,
        )

    def stop(self, timeout_seconds: float = 15.0) -> bool:
        """Stop the worker and report whether its thread has terminated."""
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not isfinite(timeout_seconds)
            or timeout_seconds < 0
        ):
            raise ValueError(
                "timeout_seconds must be a non-negative finite number."
            )

        with self._lifecycle_lock:
            thread = self._thread

            if thread is None:
                return True

            self._stop_event.set()

        if thread is current_thread():
            logger.error(
                "Broker tick worker cannot join its own thread."
            )
            return False

        thread.join(timeout=float(timeout_seconds))

        if thread.is_alive():
            logger.error(
                "Broker tick worker did not stop within %.1f seconds.",
                timeout_seconds,
            )
            return False

        with self._lifecycle_lock:
            if self._thread is thread:
                self._thread = None

        logger.info(
            "Broker tick monitor worker stopped: symbol=%s.",
            self._symbol,
        )
        return True

    def check_once(self):
        """Read and observe one broker quote sample."""
        sample = self._adapter.get_broker_time_sample(
            self._symbol
        )

        if not isinstance(sample, BrokerTimeSample):
            raise TypeError(
                "MT5 adapter returned an invalid broker time sample."
            )

        if sample.symbol != self._symbol:
            raise ValueError(
                "MT5 adapter returned a sample for an unexpected symbol."
            )

        snapshot = self._monitor.observe(sample)

        with self._status_lock:
            self._last_error = None

        return snapshot

    def status(self) -> dict[str, object]:
        """Return a JSON-compatible worker and monitor status."""
        snapshot = self._monitor.snapshot(
            now_monotonic_ns=monotonic_ns()
        )

        result = asdict(snapshot)
        result["state"] = snapshot.state.value
        result["running"] = self.is_running
        result["last_error"] = self.last_error

        return result

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.check_once()
            except Exception as exc:
                error_name = type(exc).__name__

                with self._status_lock:
                    self._last_error = error_name

                logger.exception(
                    "Broker tick monitoring cycle failed: "
                    "symbol=%s error=%s",
                    self._symbol,
                    error_name,
                )

            if self._stop_event.wait(self._interval_seconds):
                break
