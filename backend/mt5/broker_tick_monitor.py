from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from math import isfinite
from threading import RLock

from backend.mt5.time_models import BrokerTimeSample


NANOSECONDS_PER_SECOND = 1_000_000_000


class BrokerTickState(str, Enum):
    NO_DATA = "NO_DATA"
    HEALTHY = "HEALTHY"
    STALE = "STALE"
    TIMESTAMP_REPEATED = "TIMESTAMP_REPEATED"
    TIMESTAMP_REGRESSION = "TIMESTAMP_REGRESSION"
    IDENTITY_CHANGED = "IDENTITY_CHANGED"


@dataclass(frozen=True)
class BrokerTickSnapshot:
    state: BrokerTickState
    clock_verification: str
    symbol: str | None
    account_login: int | None
    broker_server: str | None
    last_broker_time_msc: int | None
    seconds_since_progress: float | None
    samples_received: int
    timestamp_advances: int
    repeated_timestamps: int
    timestamp_regressions: int
    identity_changes: int


class BrokerTickMonitor:
    """Monitor quote timestamp progression without claiming UTC alignment."""

    def __init__(
        self,
        *,
        stale_after_seconds: float = 15.0,
    ) -> None:
        if (
            isinstance(stale_after_seconds, bool)
            or not isinstance(stale_after_seconds, (int, float))
            or not isfinite(stale_after_seconds)
            or stale_after_seconds <= 0
        ):
            raise ValueError(
                "stale_after_seconds must be a positive finite number."
            )

        self._stale_after_ns = int(
            float(stale_after_seconds) * NANOSECONDS_PER_SECOND
        )
        if self._stale_after_ns < 1:
            raise ValueError(
                "stale_after_seconds is too small."
            )

        self._lock = RLock()
        self._state = BrokerTickState.NO_DATA

        self._symbol: str | None = None
        self._account_login: int | None = None
        self._broker_server: str | None = None

        self._last_broker_time_msc: int | None = None
        self._last_progress_monotonic_ns: int | None = None

        self._samples_received = 0
        self._timestamp_advances = 0
        self._repeated_timestamps = 0
        self._timestamp_regressions = 0
        self._identity_changes = 0

    def observe(
        self,
        sample: BrokerTimeSample,
    ) -> BrokerTickSnapshot:
        if not isinstance(sample, BrokerTimeSample):
            raise TypeError(
                "sample must be a BrokerTimeSample instance."
            )

        with self._lock:
            self._samples_received += 1

            identity = (
                sample.symbol,
                sample.account_login,
                sample.broker_server,
            )

            current_identity = (
                self._symbol,
                self._account_login,
                self._broker_server,
            )

            if self._symbol is None:
                self._set_identity(identity)
                self._accept_baseline(sample)
                self._state = BrokerTickState.HEALTHY
                return self._snapshot_at(
                    sample.received_monotonic_ns
                )

            if identity != current_identity:
                self._identity_changes += 1
                self._set_identity(identity)
                self._accept_baseline(sample)
                self._state = BrokerTickState.IDENTITY_CHANGED
                return self._snapshot_at(
                    sample.received_monotonic_ns
                )

            if self._last_progress_monotonic_ns is None:
                raise RuntimeError(
                    "Monitor internal progression state is invalid."
                )

            if (
                sample.received_monotonic_ns
                < self._last_progress_monotonic_ns
            ):
                raise ValueError(
                    "Sample monotonic receipt time moved backwards."
                )

            previous_timestamp = self._last_broker_time_msc

            if previous_timestamp is None:
                raise RuntimeError(
                    "Monitor has no baseline broker timestamp."
                )

            if sample.broker_time_msc < previous_timestamp:
                self._timestamp_regressions += 1
                self._state = BrokerTickState.TIMESTAMP_REGRESSION

            elif sample.broker_time_msc == previous_timestamp:
                self._repeated_timestamps += 1
                self._state = BrokerTickState.TIMESTAMP_REPEATED

            else:
                self._last_broker_time_msc = (
                    sample.broker_time_msc
                )
                self._last_progress_monotonic_ns = (
                    sample.received_monotonic_ns
                )
                self._timestamp_advances += 1
                self._state = BrokerTickState.HEALTHY

            return self._snapshot_at(
                sample.received_monotonic_ns
            )

    def snapshot(
        self,
        *,
        now_monotonic_ns: int,
    ) -> BrokerTickSnapshot:
        if (
            isinstance(now_monotonic_ns, bool)
            or not isinstance(now_monotonic_ns, int)
            or now_monotonic_ns < 0
        ):
            raise ValueError(
                "now_monotonic_ns must be a non-negative integer."
            )

        with self._lock:
            return self._snapshot_at(now_monotonic_ns)

    def reset(self) -> None:
        """Clear all observations and identity information."""
        with self._lock:
            self._state = BrokerTickState.NO_DATA

            self._symbol = None
            self._account_login = None
            self._broker_server = None

            self._last_broker_time_msc = None
            self._last_progress_monotonic_ns = None

            self._samples_received = 0
            self._timestamp_advances = 0
            self._repeated_timestamps = 0
            self._timestamp_regressions = 0
            self._identity_changes = 0

    def _set_identity(
        self,
        identity: tuple[str, int, str],
    ) -> None:
        (
            self._symbol,
            self._account_login,
            self._broker_server,
        ) = identity

    def _accept_baseline(
        self,
        sample: BrokerTimeSample,
    ) -> None:
        self._last_broker_time_msc = sample.broker_time_msc
        self._last_progress_monotonic_ns = (
            sample.received_monotonic_ns
        )

    def _snapshot_at(
        self,
        now_monotonic_ns: int,
    ) -> BrokerTickSnapshot:
        seconds_since_progress: float | None = None
        state = self._state

        if self._last_progress_monotonic_ns is not None:
            elapsed_ns = (
                now_monotonic_ns
                - self._last_progress_monotonic_ns
            )

            if elapsed_ns < 0:
                raise ValueError(
                    "Monotonic clock value is earlier than "
                    "the last accepted sample."
                )

            seconds_since_progress = (
                elapsed_ns / NANOSECONDS_PER_SECOND
            )

            if elapsed_ns >= self._stale_after_ns:
                state = BrokerTickState.STALE

        return BrokerTickSnapshot(
            state=state,
            clock_verification="UNVERIFIED",
            symbol=self._symbol,
            account_login=self._account_login,
            broker_server=self._broker_server,
            last_broker_time_msc=self._last_broker_time_msc,
            seconds_since_progress=seconds_since_progress,
            samples_received=self._samples_received,
            timestamp_advances=self._timestamp_advances,
            repeated_timestamps=self._repeated_timestamps,
            timestamp_regressions=self._timestamp_regressions,
            identity_changes=self._identity_changes,
        )
