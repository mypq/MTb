from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from math import floor, isfinite
from statistics import median
from threading import RLock
from time import monotonic

from backend.market_data.ntp_time import NtpTimeVerifier, NtpVerification


@dataclass(frozen=True, slots=True)
class BrokerCandleClockSnapshot:
    status: str
    symbol: str | None
    account_login: int | None
    broker_server: str | None
    offset_seconds: float | None
    uncertainty_seconds: float | None
    sample_count: int
    reason: str | None


class BrokerCandleClockCalibrator:
    """
    Calibrates MT5 candle timestamps against independently verified UTC.

    Uses simultaneously observed active M1 and M5 candle boundaries.
    The inferred broker offset must be minute-aligned and stable.

    A verified calibration is preserved when an otherwise consistent
    active-candle read is delayed across a minute boundary by MT5 waiting
    for the first tick of the new period.
    """

    def __init__(
        self,
        ntp_verifier: NtpTimeVerifier,
        *,
        required_samples: int = 5,
        minimum_span_seconds: float = 3.0,
        maximum_spread_seconds: float = 0.5,
        maximum_sample_age_seconds: float = 30.0,
        boundary_grace_seconds: float = 5.0,
    ) -> None:
        if not isinstance(ntp_verifier, NtpTimeVerifier):
            raise TypeError("ntp_verifier must be an NtpTimeVerifier.")

        if required_samples < 3:
            raise ValueError("required_samples must be at least 3.")

        if minimum_span_seconds <= 0:
            raise ValueError("minimum_span_seconds must be positive.")

        if maximum_spread_seconds <= 0:
            raise ValueError("maximum_spread_seconds must be positive.")

        if maximum_sample_age_seconds <= 0:
            raise ValueError("maximum_sample_age_seconds must be positive.")

        if not 0 < boundary_grace_seconds < 30:
            raise ValueError(
                "boundary_grace_seconds must be greater than 0 "
                "and less than 30."
            )

        self._ntp = ntp_verifier
        self._required_samples = required_samples
        self._minimum_span = minimum_span_seconds
        self._maximum_spread = maximum_spread_seconds
        self._maximum_age = maximum_sample_age_seconds
        self._boundary_grace = boundary_grace_seconds

        self._samples: deque[tuple[float, float]] = deque(
            maxlen=max(required_samples, 9)
        )
        self._identity: tuple[str, int, str] | None = None
        self._offset: float | None = None
        self._uncertainty: float | None = None
        self._last_sample_monotonic: float | None = None
        self._ntp_evidence: NtpVerification | None = None
        self._status = "UNVERIFIED"
        self._reason = "No candle clock samples collected."
        self._lock = RLock()

    def observe(
        self,
        *,
        symbol: str,
        account_login: int,
        broker_server: str,
        m1_active_open_epoch: int,
        m5_active_open_epoch: int,
        observed_at_utc: datetime,
    ) -> BrokerCandleClockSnapshot:
        with self._lock:
            if not self._ntp.is_trusted():
                return self._invalidate(
                    "DEGRADED",
                    "Independent NTP verification is not trusted.",
                )

            ntp = self._ntp.last_verification()

            if (
                ntp is None
                or not ntp.verified
                or ntp.offset_seconds is None
                or ntp.uncertainty_seconds is None
            ):
                return self._invalidate(
                    "DEGRADED",
                    "A trusted NTP result is unavailable.",
                )

            if (
                not isinstance(observed_at_utc, datetime)
                or observed_at_utc.tzinfo is None
                or observed_at_utc.utcoffset() is None
            ):
                return self._invalidate(
                    "DEGRADED",
                    "Observation time must be timezone-aware.",
                )

            if (
                isinstance(m1_active_open_epoch, bool)
                or not isinstance(m1_active_open_epoch, int)
                or isinstance(m5_active_open_epoch, bool)
                or not isinstance(m5_active_open_epoch, int)
                or m1_active_open_epoch <= 0
                or m5_active_open_epoch <= 0
            ):
                return self._invalidate(
                    "DEGRADED",
                    "Active candle timestamps are invalid.",
                )

            identity = (
                symbol.strip(),
                account_login,
                broker_server.strip(),
            )

            if not identity[0] or not identity[2]:
                return self._invalidate(
                    "DEGRADED",
                    "Broker identity is incomplete.",
                )

            if self._identity is not None and identity != self._identity:
                self._reset()
                self._identity = identity
                self._reason = (
                    "Broker identity changed; recalibration started."
                )
                self._status = "COLLECTING"
                return self._snapshot()

            self._identity = identity

            observed = observed_at_utc.astimezone(timezone.utc).timestamp()
            corrected_utc = observed + ntp.offset_seconds

            m1_boundary = floor(corrected_utc / 60) * 60
            m5_boundary = floor(corrected_utc / 300) * 300

            m1_offset = m1_active_open_epoch - m1_boundary
            m5_offset = m5_active_open_epoch - m5_boundary

            if not all(isfinite(v) for v in (m1_offset, m5_offset)):
                return self._invalidate(
                    "DEGRADED",
                    "Calculated candle offset is invalid.",
                )

            if abs(m1_offset - m5_offset) > 1.0:
                if self._is_boundary_lag_sample(
                    corrected_utc=corrected_utc,
                    m1_active_open_epoch=m1_active_open_epoch,
                    m5_active_open_epoch=m5_active_open_epoch,
                    m1_boundary=m1_boundary,
                    m5_boundary=m5_boundary,
                ):
                    self._reason = (
                        "Ignored a delayed active-candle update near "
                        "the minute boundary; verified calibration preserved."
                    )
                    return self._snapshot()

                return self._invalidate(
                    "DEGRADED",
                    "M1 and M5 active candle boundaries disagree.",
                )

            candidate = median((m1_offset, m5_offset))
            minute_aligned = round(candidate / 60.0) * 60.0

            if abs(candidate - minute_aligned) > 1.0:
                return self._invalidate(
                    "DEGRADED",
                    "Broker offset is not aligned to a whole minute.",
                )

            now_mono = monotonic()
            self._samples.append((minute_aligned, now_mono))
            self._last_sample_monotonic = now_mono
            self._ntp_evidence = ntp
            self._status = "COLLECTING"
            self._reason = "Collecting stable M1/M5 boundary samples."

            if len(self._samples) < self._required_samples:
                return self._snapshot()

            values = [item[0] for item in self._samples]
            span = self._samples[-1][1] - self._samples[0][1]
            spread = max(values) - min(values)

            if span < self._minimum_span:
                self._reason = (
                    "Calibration samples do not span enough time."
                )
                return self._snapshot()

            if spread > self._maximum_spread:
                return self._invalidate(
                    "DEGRADED",
                    "Candle offset samples are unstable.",
                )

            measured = median(values)

            if (
                self._offset is not None
                and abs(measured - self._offset) > 60
            ):
                return self._invalidate(
                    "DEGRADED",
                    "Broker candle offset changed; recalibration required.",
                )

            self._offset = measured
            self._uncertainty = (
                ntp.uncertainty_seconds + spread / 2.0 + 1.0
            )
            self._status = "VERIFIED"
            self._reason = (
                "M1 and M5 active boundaries agree with independently "
                "verified UTC across stable samples."
            )
            return self._snapshot()

    def _is_boundary_lag_sample(
        self,
        *,
        corrected_utc: float,
        m1_active_open_epoch: int,
        m5_active_open_epoch: int,
        m1_boundary: int,
        m5_boundary: int,
    ) -> bool:
        """
        Return True only for a recognizable delayed candle update.

        This exception is available only while preserving an existing
        VERIFIED calibration. It does not accept a new or changed offset.
        """
        if self._status != "VERIFIED" or self._offset is None:
            return False

        seconds_into_minute = corrected_utc % 60.0

        near_minute_boundary = (
            seconds_into_minute <= self._boundary_grace
            or seconds_into_minute >= 60.0 - self._boundary_grace
        )

        if not near_minute_boundary:
            return False

        expected_m1 = m1_boundary + self._offset
        expected_m5 = m5_boundary + self._offset

        m1_is_current = abs(
            m1_active_open_epoch - expected_m1
        ) <= 1.0

        m1_is_previous = abs(
            m1_active_open_epoch - (expected_m1 - 60)
        ) <= 1.0

        m5_is_current = abs(
            m5_active_open_epoch - expected_m5
        ) <= 1.0

        m5_is_previous = abs(
            m5_active_open_epoch - (expected_m5 - 300)
        ) <= 1.0

        has_delayed_candle = m1_is_previous or m5_is_previous

        return (
            has_delayed_candle
            and (m1_is_current or m1_is_previous)
            and (m5_is_current or m5_is_previous)
        )

    def snapshot(self) -> BrokerCandleClockSnapshot:
        with self._lock:
            if not self._ntp.is_trusted():
                return self._invalidate(
                    "DEGRADED",
                    "Independent NTP verification expired.",
                )

            if (
                self._last_sample_monotonic is not None
                and monotonic() - self._last_sample_monotonic
                > self._maximum_age
            ):
                return self._invalidate(
                    "DEGRADED",
                    "Candle clock calibration is stale.",
                )

            return self._snapshot()

    def trusted_evidence(
        self,
    ) -> tuple[BrokerCandleClockSnapshot, NtpVerification | None]:
        with self._lock:
            snapshot = self.snapshot()

            if snapshot.status != "VERIFIED":
                return snapshot, None

            evidence = self._ntp_evidence
            current_evidence = self._ntp.last_verification()

            if (
                evidence is None
                or current_evidence is None
                or evidence != current_evidence
                or not evidence.verified
                or evidence.checked_at_unix is None
                or evidence.offset_seconds is None
                or evidence.uncertainty_seconds is None
                or evidence.sample_count < 3
                or len(evidence.servers) < 3
                or len(set(evidence.servers)) < 3
                or not self._ntp.is_trusted()
            ):
                invalid = self._invalidate(
                    "DEGRADED",
                    "Trusted NTP evidence is unavailable, changed, or expired.",
                )
                return invalid, None

            return snapshot, evidence

    def reset(self) -> None:
        with self._lock:
            self._reset()
            self._identity = None
            self._status = "UNVERIFIED"
            self._reason = "Calibration manually reset."

    def _reset(self) -> None:
        self._samples.clear()
        self._offset = None
        self._uncertainty = None
        self._last_sample_monotonic = None
        self._ntp_evidence = None

    def _invalidate(
        self,
        status: str,
        reason: str,
    ) -> BrokerCandleClockSnapshot:
        self._reset()
        self._status = status
        self._reason = reason
        return self._snapshot()

    def _snapshot(self) -> BrokerCandleClockSnapshot:
        return BrokerCandleClockSnapshot(
            status=self._status,
            symbol=self._identity[0] if self._identity else None,
            account_login=self._identity[1] if self._identity else None,
            broker_server=self._identity[2] if self._identity else None,
            offset_seconds=self._offset,
            uncertainty_seconds=self._uncertainty,
            sample_count=len(self._samples),
            reason=self._reason,
        )
