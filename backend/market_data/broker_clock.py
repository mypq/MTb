
from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from math import isfinite
from statistics import median
from threading import RLock

from backend.market_data.ntp_time import NtpTimeVerifier
from backend.mt5.time_models import BrokerTimeSample


class BrokerClockStatus(str, Enum):
    UNVERIFIED = "UNVERIFIED"
    COLLECTING = "COLLECTING"
    ESTIMATED = "ESTIMATED"
    VERIFIED = "VERIFIED"
    DEGRADED = "DEGRADED"
    IDENTITY_CHANGED = "IDENTITY_CHANGED"


@dataclass(frozen=True, slots=True)
class BrokerClockSnapshot:
    status: BrokerClockStatus
    symbol: str | None
    account_login: int | None
    broker_server: str | None
    offset_seconds: float | None
    uncertainty_seconds: float | None
    sample_count: int
    required_samples: int
    last_sample_at: object | None
    reason: str | None


class BrokerClockCalibrator:
    """
    Estimates broker quote-clock offset against independently verified UTC.

    Only advancing quote timestamps are used. The local receipt time is
    corrected using the latest trusted external NTP offset.

    IMPORTANT:
    This is an event-derived estimate, not proof of the broker's wall clock.
    Quote age, network latency, and processing delay can affect the result.

    The reported uncertainty is a dispersion indicator for the collected
    estimates. It is not a complete bound on network or quote-age error.

    This estimate must not be used to correct candle timestamps.
    """

    def __init__(
        self,
        ntp_verifier: NtpTimeVerifier,
        *,
        required_samples: int = 5,
        max_samples: int = 9,
        minimum_span_seconds: float = 3.0,
        maximum_spread_seconds: float = 0.5,
        maximum_offset_change_seconds: float = 1.0,
    ) -> None:
        if not isinstance(ntp_verifier, NtpTimeVerifier):
            raise TypeError("ntp_verifier must be an NtpTimeVerifier.")

        if (
            isinstance(required_samples, bool)
            or not isinstance(required_samples, int)
            or required_samples < 3
        ):
            raise ValueError("required_samples must be at least 3.")

        if (
            isinstance(max_samples, bool)
            or not isinstance(max_samples, int)
            or max_samples < required_samples
        ):
            raise ValueError("max_samples must be >= required_samples.")

        for name, value in (
            ("minimum_span_seconds", minimum_span_seconds),
            ("maximum_spread_seconds", maximum_spread_seconds),
            ("maximum_offset_change_seconds", maximum_offset_change_seconds),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be a positive finite number.")

        self._ntp = ntp_verifier
        self._required_samples = required_samples
        self._minimum_span_ns = int(minimum_span_seconds * 1_000_000_000)
        self._maximum_spread_seconds = float(maximum_spread_seconds)
        self._maximum_offset_change_seconds = float(
            maximum_offset_change_seconds
        )

        self._samples: deque[tuple[float, int]] = deque(maxlen=max_samples)
        self._lock = RLock()

        self._identity: tuple[str, int, str] | None = None
        self._last_broker_time_msc: int | None = None
        self._offset_seconds: float | None = None
        self._uncertainty_seconds: float | None = None
        self._last_sample_at = None
        self._status = BrokerClockStatus.UNVERIFIED
        self._reason: str | None = "No broker samples have been collected."

    def observe(
        self,
        sample: BrokerTimeSample,
    ) -> BrokerClockSnapshot:
        if not isinstance(sample, BrokerTimeSample):
            raise TypeError("sample must be a BrokerTimeSample.")

        with self._lock:
            if not self._ntp.is_trusted():
                self._invalidate(
                    BrokerClockStatus.DEGRADED,
                    "Independent NTP verification is not trusted.",
                )
                return self._snapshot()

            ntp_result = self._ntp.last_verification()

            if (
                ntp_result is None
                or not ntp_result.verified
                or ntp_result.offset_seconds is None
                or ntp_result.uncertainty_seconds is None
            ):
                self._invalidate(
                    BrokerClockStatus.DEGRADED,
                    "A current successful NTP verification is unavailable.",
                )
                return self._snapshot()

            if (
                not isfinite(ntp_result.offset_seconds)
                or not isfinite(ntp_result.uncertainty_seconds)
                or ntp_result.uncertainty_seconds < 0
            ):
                self._invalidate(
                    BrokerClockStatus.DEGRADED,
                    "NTP verification contains invalid timing values.",
                )
                return self._snapshot()

            identity = (
                sample.symbol,
                sample.account_login,
                sample.broker_server,
            )

            if self._identity is None:
                self._identity = identity

            elif identity != self._identity:
                self._reset_measurement()
                self._identity = identity
                self._last_broker_time_msc = sample.broker_time_msc
                self._last_sample_at = sample.received_at_utc
                self._status = BrokerClockStatus.IDENTITY_CHANGED
                self._reason = (
                    "Broker identity changed; collecting a new calibration."
                )
                return self._snapshot()

            if self._last_broker_time_msc is not None:
                if sample.broker_time_msc < self._last_broker_time_msc:
                    self._invalidate(
                        BrokerClockStatus.DEGRADED,
                        "Broker quote timestamp moved backwards.",
                    )
                    self._last_broker_time_msc = sample.broker_time_msc
                    return self._snapshot()

                if sample.broker_time_msc == self._last_broker_time_msc:
                    self._reason = "Waiting for an advancing broker quote."
                    return self._snapshot()

            corrected_receipt_epoch = (
                sample.received_at_utc.timestamp()
                + ntp_result.offset_seconds
            )

            broker_event_epoch = sample.broker_time_msc / 1000.0
            measured_offset = broker_event_epoch - corrected_receipt_epoch

            if not isfinite(measured_offset):
                self._invalidate(
                    BrokerClockStatus.DEGRADED,
                    "Calculated broker offset is not finite.",
                )
                return self._snapshot()

            self._last_broker_time_msc = sample.broker_time_msc
            self._last_sample_at = sample.received_at_utc

            self._samples.append(
                (measured_offset, sample.received_monotonic_ns)
            )

            self._status = BrokerClockStatus.COLLECTING
            self._reason = "Collecting advancing quote samples."

            if len(self._samples) < self._required_samples:
                return self._snapshot()

            sample_span_ns = (
                self._samples[-1][1] - self._samples[0][1]
            )

            if sample_span_ns < self._minimum_span_ns:
                self._reason = (
                    "Advancing samples have not covered the minimum "
                    "measurement interval."
                )
                return self._snapshot()

            offsets = [item[0] for item in self._samples]
            measured_median = median(offsets)
            spread = max(offsets) - min(offsets)

            if spread > self._maximum_spread_seconds:
                self._invalidate(
                    BrokerClockStatus.DEGRADED,
                    "Broker offset samples are too dispersed.",
                )
                return self._snapshot()

            if self._offset_seconds is not None:
                if (
                    abs(measured_median - self._offset_seconds)
                    > self._maximum_offset_change_seconds
                ):
                    self._invalidate(
                        BrokerClockStatus.DEGRADED,
                        "Broker offset changed beyond the permitted limit; "
                        "a fresh calibration is required.",
                    )
                    return self._snapshot()

            self._offset_seconds = measured_median

            # This is only a dispersion indicator, not a complete
            # uncertainty bound for network latency or quote age.
            self._uncertainty_seconds = (
                ntp_result.uncertainty_seconds + spread / 2.0
            )

            self._status = BrokerClockStatus.ESTIMATED
            self._reason = (
                "Event-derived offset estimate only. "
                "Quote age and network latency are not independently bounded; "
                "this result is not verified broker UTC."
            )

            return self._snapshot()

    def snapshot(self) -> BrokerClockSnapshot:
        with self._lock:
            if not self._ntp.is_trusted():
                self._invalidate(
                    BrokerClockStatus.DEGRADED,
                    "Independent NTP verification expired.",
                )
            return self._snapshot()

    def reset(self) -> None:
        with self._lock:
            self._reset_measurement()
            self._identity = None
            self._last_broker_time_msc = None
            self._status = BrokerClockStatus.UNVERIFIED
            self._reason = "Calibration was manually reset."

    def _reset_measurement(self) -> None:
        self._samples.clear()
        self._offset_seconds = None
        self._uncertainty_seconds = None

    def _invalidate(
        self,
        status: BrokerClockStatus,
        reason: str,
    ) -> None:
        self._reset_measurement()
        self._status = status
        self._reason = reason

    def _snapshot(self) -> BrokerClockSnapshot:
        return BrokerClockSnapshot(
            status=self._status,
            symbol=self._identity[0] if self._identity else None,
            account_login=self._identity[1] if self._identity else None,
            broker_server=self._identity[2] if self._identity else None,
            offset_seconds=self._offset_seconds,
            uncertainty_seconds=self._uncertainty_seconds,
            sample_count=len(self._samples),
            required_samples=self._required_samples,
            last_sample_at=self._last_sample_at,
            reason=self._reason,
        )
