
from __future__ import annotations

import logging
from math import isfinite
from threading import Event, Lock, Thread, current_thread
from typing import TYPE_CHECKING

from backend.mt5.models import (
    MT5ConnectionState,
    MT5ConnectionStatus,
)

if TYPE_CHECKING:
    from backend.mt5.adapter import MT5Adapter


logger = logging.getLogger(__name__)


class MT5ConnectionMonitor:
    """Background monitor for MT5 connection health and recovery."""

    def __init__(
        self,
        adapter: MT5Adapter,
        *,
        interval_seconds: float = 10.0,
    ) -> None:
        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (int, float))
            or not isfinite(interval_seconds)
            or interval_seconds <= 0
        ):
            raise ValueError("interval_seconds must be a positive finite number.")

        self._adapter = adapter
        self._interval_seconds = float(interval_seconds)

        self._stop_event = Event()
        self._lifecycle_lock = Lock()
        self._thread: Thread | None = None
        self._last_state: MT5ConnectionState | None = None

    @property
    def is_running(self) -> bool:
        with self._lifecycle_lock:
            return self._thread is not None and self._thread.is_alive()

    def start(self) -> None:
        """Start the monitor once."""
        with self._lifecycle_lock:
            if self._thread is not None and self._thread.is_alive():
                return

            self._stop_event = Event()
            self._thread = Thread(
                target=self._run,
                name="MT5ConnectionMonitor",
                daemon=True,
            )
            self._thread.start()

        logger.info(
            "MT5 connection monitor started; interval=%.1f seconds.",
            self._interval_seconds,
        )

    def stop(self, timeout_seconds: float = 15.0) -> bool:
        """Stop monitoring and report whether the worker has fully stopped."""
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
                "MT5 monitor cannot join its own worker thread."
            )
            return False

        thread.join(timeout=float(timeout_seconds))

        if thread.is_alive():
            logger.error(
                "MT5 monitor did not stop within %.1f seconds.",
                timeout_seconds,
            )
            return False

        with self._lifecycle_lock:
            if self._thread is thread:
                self._thread = None

        logger.info("MT5 connection monitor stopped.")
        return True

    def check_once(self) -> MT5ConnectionStatus:
        """Check connection and attempt recovery when necessary."""
        status = self._adapter.connection_status()

        if status.state == MT5ConnectionState.DISABLED:
            self._record_state(status)
            return status

        if status.state == MT5ConnectionState.CONNECTED:
            self._record_state(status)
            return status

        logger.warning(
            "MT5 connection is %s; attempting recovery.",
            status.state.value,
        )

        recovered_status = self._adapter.connect()
        self._record_state(recovered_status)

        if recovered_status.state == MT5ConnectionState.CONNECTED:
            logger.info("MT5 connection successfully restored.")
        else:
            logger.warning(
                "MT5 reconnection unsuccessful: %s",
                recovered_status.message,
            )

        return recovered_status

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self.check_once()
            except Exception:
                logger.exception(
                    "MT5 connection monitoring cycle failed."
                )

            if self._stop_event.wait(self._interval_seconds):
                break

    def _record_state(self, status: MT5ConnectionStatus) -> None:
        if status.state == self._last_state:
            return

        previous_state = self._last_state
        self._last_state = status.state

        if previous_state is None:
            logger.info(
                "Initial MT5 connection state: %s.",
                status.state.value,
            )
            return

        logger.info(
            "MT5 connection state changed: %s -> %s.",
            previous_state.value,
            status.state.value,
        )
