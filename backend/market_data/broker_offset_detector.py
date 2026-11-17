
"""Detect a broker UTC offset from MT5 clock history and trusted NTP evidence."""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum
from math import isfinite
from statistics import median

from backend.market_data.clock_history import MT5ClockHistorySample
from backend.market_data.ntp_time import NtpTimeVerifier


class BrokerOffsetStatus(str, Enum):
    WAITING_FOR_NTP = "WAITING_FOR_NTP"
    COLLECTING = "COLLECTING"
    CANDIDATE_READY = "CANDIDATE_READY"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True, slots=True)
class BrokerOffsetDecision:
    status: BrokerOffsetStatus
    candidate_ready: bool
    offset_seconds: int | None
    uncertainty_seconds: float | None
    sample_count: int
    required_samples: int
    run_id: str | None
    identity: tuple[int, str, str, int] | None
    reason: str


class BrokerOffsetDetector:
    """Build a three-sample broker-offset candidate using verified NTP time."""

    def __init__(
        self,
        ntp_verifier: NtpTimeVerifier,
        *,
        required_samples: int = 3,
        max_sample_ntp_distance_seconds: float = 6 * 60 * 60,
        max_candidate_spread_seconds: float = 1.5,
        max_integer_residual_seconds: float = 1.5,
    ) -> None:
        if not isinstance(ntp_verifier, NtpTimeVerifier):
            raise TypeError("ntp_verifier must be an NtpTimeVerifier.")

        if (
            isinstance(required_samples, bool)
            or not isinstance(required_samples, int)
            or required_samples != 3
        ):
            raise ValueError("required_samples must be exactly 3.")

        for name, value in (
            (
                "max_sample_ntp_distance_seconds",
                max_sample_ntp_distance_seconds,
            ),
            (
                "max_candidate_spread_seconds",
                max_candidate_spread_seconds,
            ),
            (
                "max_integer_residual_seconds",
                max_integer_residual_seconds,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{name} must be positive and finite.")

        self._ntp = ntp_verifier
        self._required_samples = required_samples
        self._max_sample_ntp_distance = float(
            max_sample_ntp_distance_seconds
        )
        self._max_spread = float(max_candidate_spread_seconds)
        self._max_integer_residual = float(
            max_integer_residual_seconds
        )

        self._window: deque[
            tuple[MT5ClockHistorySample, float, float]
        ] = deque(maxlen=required_samples)

    def observe(
        self,
        sample: MT5ClockHistorySample,
    ) -> BrokerOffsetDecision:
        if not isinstance(sample, MT5ClockHistorySample):
            raise TypeError(
                "sample must be an MT5ClockHistorySample."
            )

        ntp = self._trusted_ntp_for(sample)

        if ntp is None:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.WAITING_FOR_NTP,
                "A fresh, trusted NTP verification is unavailable "
                "or too far from this MT5 sample.",
            )

        ntp_offset, ntp_uncertainty = ntp

        if not sample.terminal_connected:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.DEGRADED,
                "MT5 terminal was not connected for this sample.",
            )

        corrected_gmt = (
            float(sample.time_gmt_epoch) + ntp_offset
        )
        candidate = (
            float(sample.time_trade_server_epoch) - corrected_gmt
        )

        if not isfinite(candidate) or abs(candidate) > 14 * 60 * 60:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.DEGRADED,
                "Calculated broker offset is outside the supported range.",
            )

        rounded_candidate = int(round(candidate))

        if (
            abs(candidate - rounded_candidate)
            > self._max_integer_residual
        ):
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.DEGRADED,
                "Calculated offset is not sufficiently close to "
                "a whole-second broker offset.",
            )

        if self._window:
            previous = self._window[-1][0]

            same_run = sample.run_id == previous.run_id
            same_identity = sample.identity == previous.identity
            next_sequence = (
                sample.sample_sequence
                == previous.sample_sequence + 1
            )

            tick_delta = (
                sample.monotonic_ms - previous.monotonic_ms
            ) & 0xFFFFFFFF

            if not (
                same_run
                and same_identity
                and next_sequence
                and tick_delta > 0
            ):
                self._window.clear()
            elif rounded_candidate != int(
                round(self._window[-1][1])
            ):
                self._window.clear()

        self._window.append(
            (sample, candidate, ntp_uncertainty)
        )

        candidates = [item[1] for item in self._window]
        spread = max(candidates) - min(candidates)

        if spread > self._max_spread:
            self._window.clear()
            self._window.append(
                (sample, candidate, ntp_uncertainty)
            )
            return self._decision(
                BrokerOffsetStatus.COLLECTING,
                "Offset samples were inconsistent; a new sequence "
                "has started with the latest sample.",
            )

        if len(self._window) < self._required_samples:
            return self._decision(
                BrokerOffsetStatus.COLLECTING,
                "Collecting consecutive, NTP-supported MT5 samples.",
            )

        measured_offset = int(round(median(candidates)))
        uncertainty = (
            max(item[2] for item in self._window)
            + 1.0
            + spread / 2.0
        )

        return self._decision(
            BrokerOffsetStatus.CANDIDATE_READY,
            "Three consecutive samples from the same run and identity "
            "agree. Candidate is ready for registry validation; "
            "it is not yet a persisted or historically verified offset.",
            offset_seconds=measured_offset,
            uncertainty_seconds=uncertainty,
        )

    def reset(self) -> None:
        self._window.clear()

    def _trusted_ntp_for(
        self,
        sample: MT5ClockHistorySample,
    ) -> tuple[float, float] | None:
        if not self._ntp.is_trusted():
            return None

        verification = self._ntp.last_verification()

        if (
            verification is None
            or not verification.verified
            or verification.sample_count < 3
            or verification.checked_at_unix is None
            or verification.offset_seconds is None
            or verification.uncertainty_seconds is None
        ):
            return None

        offset = verification.offset_seconds
        uncertainty = verification.uncertainty_seconds

        if (
            not isfinite(offset)
            or not isfinite(uncertainty)
            or uncertainty < 0
        ):
            return None

        corrected_sample_gmt = (
            float(sample.time_gmt_epoch) + float(offset)
        )

        if (
            abs(corrected_sample_gmt - verification.checked_at_unix)
            > self._max_sample_ntp_distance
        ):
            return None

        return float(offset), float(uncertainty)

    def _decision(
        self,
        status: BrokerOffsetStatus,
        reason: str,
        *,
        offset_seconds: int | None = None,
        uncertainty_seconds: float | None = None,
    ) -> BrokerOffsetDecision:
        last_sample = self._window[-1][0] if self._window else None

        return BrokerOffsetDecision(
            status=status,
            candidate_ready=(
                status == BrokerOffsetStatus.CANDIDATE_READY
            ),
            offset_seconds=offset_seconds,
            uncertainty_seconds=uncertainty_seconds,
            sample_count=len(self._window),
            required_samples=self._required_samples,
            run_id=last_sample.run_id if last_sample else None,
            identity=last_sample.identity if last_sample else None,
            reason=reason,
        )
