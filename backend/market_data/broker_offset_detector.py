
"""Detect broker UTC offsets using consecutive MT5 samples and NTP evidence."""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass
from enum import Enum
from math import isfinite
from statistics import median

from backend.market_data.clock_history import MT5ClockHistorySample
from backend.market_data.ntp_time import (
    NtpTimeVerifier,
    NtpVerification,
)


class BrokerOffsetStatus(str, Enum):
    WAITING_FOR_NTP = "WAITING_FOR_NTP"
    COLLECTING = "COLLECTING"
    CANDIDATE_READY = "CANDIDATE_READY"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True, slots=True)
class BrokerOffsetEvidenceSample:
    run_id: str
    sample_sequence: int
    monotonic_ms: int
    time_gmt_epoch: int
    time_trade_server_epoch: int
    time_current_quote_epoch: int
    time_local_epoch: int
    server_utc_offset_seconds: int
    account_login: int
    broker_server: str
    symbol: str
    terminal_build: int
    terminal_connected: bool
    calculated_offset_seconds: float
    ntp_offset_seconds: float
    ntp_uncertainty_seconds: float
    ntp_checked_at_unix: float

    @classmethod
    def from_sample(
        cls,
        sample: MT5ClockHistorySample,
        *,
        calculated_offset_seconds: float,
        ntp_offset_seconds: float,
        ntp_uncertainty_seconds: float,
        ntp_checked_at_unix: float,
    ) -> "BrokerOffsetEvidenceSample":
        return cls(
            run_id=sample.run_id,
            sample_sequence=sample.sample_sequence,
            monotonic_ms=sample.monotonic_ms,
            time_gmt_epoch=sample.time_gmt_epoch,
            time_trade_server_epoch=sample.time_trade_server_epoch,
            time_current_quote_epoch=sample.time_current_quote_epoch,
            time_local_epoch=sample.time_local_epoch,
            server_utc_offset_seconds=sample.server_utc_offset_seconds,
            account_login=sample.account_login,
            broker_server=sample.broker_server,
            symbol=sample.symbol,
            terminal_build=sample.terminal_build,
            terminal_connected=sample.terminal_connected,
            calculated_offset_seconds=calculated_offset_seconds,
            ntp_offset_seconds=ntp_offset_seconds,
            ntp_uncertainty_seconds=ntp_uncertainty_seconds,
            ntp_checked_at_unix=ntp_checked_at_unix,
        )


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
    evidence_samples: tuple[BrokerOffsetEvidenceSample, ...] = ()
    ntp_verified: bool = False
    ntp_sample_count: int = 0
    ntp_servers: tuple[str, ...] = ()
    ntp_checked_at_unix: float | None = None
    ntp_offset_seconds: float | None = None
    ntp_uncertainty_seconds: float | None = None


class BrokerOffsetDetector:
    """Build a candidate from exactly three consecutive supported samples."""

    def __init__(
        self,
        ntp_verifier: NtpTimeVerifier,
        *,
        required_samples: int = 3,
        max_sample_ntp_distance_seconds: float = 90 * 60,
        minimum_sample_interval_seconds: float = 3.0,
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
            ("max_sample_ntp_distance_seconds",
             max_sample_ntp_distance_seconds),
            ("minimum_sample_interval_seconds",
             minimum_sample_interval_seconds),
            ("max_candidate_spread_seconds",
             max_candidate_spread_seconds),
            ("max_integer_residual_seconds",
             max_integer_residual_seconds),
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
        self._minimum_sample_interval_ms = round(
            minimum_sample_interval_seconds * 1000
        )
        self._max_spread = float(max_candidate_spread_seconds)
        self._max_integer_residual = float(
            max_integer_residual_seconds
        )
        self._window: deque[
            tuple[
                MT5ClockHistorySample,
                float,
                float,
                float,
                float,
                NtpVerification,
            ]
        ] = deque(maxlen=required_samples)

    @property
    def trusted_ntp_snapshot(self) -> NtpVerification | None:
        """Return the exact currently trusted NTP snapshot, if any."""
        return self._ntp.last_verification()

    @property
    def has_trusted_ntp(self) -> bool:
        """Whether the verifier currently has a trusted NTP result."""
        return self.trusted_ntp_snapshot is not None

    def observe(
        self,
        sample: MT5ClockHistorySample,
    ) -> BrokerOffsetDecision:
        if not isinstance(sample, MT5ClockHistorySample):
            raise TypeError("sample must be an MT5ClockHistorySample.")

        ntp = self._trusted_ntp_for(sample)

        if ntp is None:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.WAITING_FOR_NTP,
                "Fresh trusted NTP verification is unavailable.",
            )

        ntp_offset = ntp.offset_seconds
        ntp_uncertainty = ntp.uncertainty_seconds
        checked_at = ntp.checked_at_unix

        if not sample.terminal_connected:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.DEGRADED,
                "MT5 terminal is disconnected.",
            )

        raw_offset = (
            sample.time_trade_server_epoch
            - sample.time_gmt_epoch
        )

        if raw_offset != sample.server_utc_offset_seconds:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.DEGRADED,
                "Raw MT5 clock difference conflicts with recorded server offset.",
            )

        # TimeGMT() is derived from the terminal computer's clock.
        # Convert that clock reading to NTP-corrected UTC before deriving
        # the broker offset; otherwise sub-second host skew can shift the
        # integer-second broker offset by one second.
        candidate = float(raw_offset) - ntp_offset

        if not isfinite(candidate) or abs(candidate) > 14 * 60 * 60:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.DEGRADED,
                "Calculated broker offset is outside the supported range.",
            )

        rounded = round(candidate)

        if abs(candidate - rounded) > self._max_integer_residual:
            self._window.clear()
            return self._decision(
                BrokerOffsetStatus.DEGRADED,
                "Calculated offset is not close enough to whole seconds.",
            )

        if self._window:
            previous = self._window[-1][0]
            tick_delta = (
                sample.monotonic_ms - previous.monotonic_ms
            ) & 0xFFFFFFFF

            continuous = (
                sample.run_id == previous.run_id
                and sample.identity == previous.identity
                and sample.sample_sequence == previous.sample_sequence + 1
                and tick_delta >= self._minimum_sample_interval_ms
            )

            if not continuous:
                self._window.clear()
            elif round(self._window[-1][1]) != rounded:
                self._window.clear()

        self._window.append(
            (
                sample,
                candidate,
                ntp_uncertainty,
                ntp_offset,
                checked_at,
                ntp,
            )
        )

        candidates = [entry[1] for entry in self._window]
        spread = max(candidates) - min(candidates)

        if spread > self._max_spread:
            self._window.clear()
            self._window.append(
                (
                    sample,
                    candidate,
                    ntp_uncertainty,
                    ntp_offset,
                    checked_at,
                    ntp,
                )
            )
            return self._decision(
                BrokerOffsetStatus.COLLECTING,
                "Inconsistent samples; collection restarted.",
            )

        if len(self._window) != self._required_samples:
            return self._decision(
                BrokerOffsetStatus.COLLECTING,
                "Collecting three consecutive NTP-supported samples.",
            )

        offset = int(round(median(candidates)))
        uncertainty = (
            max(entry[2] for entry in self._window)
            + 1.0
            + spread / 2.0
        )

        evidence = tuple(
            BrokerOffsetEvidenceSample.from_sample(
                entry[0],
                calculated_offset_seconds=entry[1],
                ntp_uncertainty_seconds=entry[2],
                ntp_offset_seconds=entry[3],
                ntp_checked_at_unix=entry[4],
            )
            for entry in self._window
        )

        verification = self._window[-1][5]

        return self._decision(
            BrokerOffsetStatus.CANDIDATE_READY,
            "Three consecutive samples agree and are ready for registry validation.",
            offset_seconds=offset,
            uncertainty_seconds=uncertainty,
            evidence_samples=evidence,
            ntp_verified=True,
            ntp_sample_count=verification.sample_count,
            ntp_servers=verification.servers,
            ntp_checked_at_unix=verification.checked_at_unix,
            ntp_offset_seconds=verification.offset_seconds,
            ntp_uncertainty_seconds=verification.uncertainty_seconds,
        )

    def reset(self) -> None:
        self._window.clear()

    def _trusted_ntp_for(
        self,
        sample: MT5ClockHistorySample,
    ) -> NtpVerification | None:
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

        checked_at = verification.checked_at_unix
        offset = verification.offset_seconds
        uncertainty = verification.uncertainty_seconds

        if not all(isfinite(value) for value in (
            checked_at, offset, uncertainty
        )) or uncertainty < 0:
            return None

        corrected_gmt = sample.time_gmt_epoch + offset

        if (
            not isfinite(corrected_gmt)
            or abs(corrected_gmt - checked_at)
            > self._max_sample_ntp_distance
        ):
            return None

        return verification

    def _decision(
        self,
        status: BrokerOffsetStatus,
        reason: str,
        *,
        offset_seconds: int | None = None,
        uncertainty_seconds: float | None = None,
        evidence_samples: tuple[BrokerOffsetEvidenceSample, ...] = (),
        ntp_verified: bool = False,
        ntp_sample_count: int = 0,
        ntp_servers: tuple[str, ...] = (),
        ntp_checked_at_unix: float | None = None,
        ntp_offset_seconds: float | None = None,
        ntp_uncertainty_seconds: float | None = None,
    ) -> BrokerOffsetDecision:
        last = self._window[-1][0] if self._window else None

        return BrokerOffsetDecision(
            status=status,
            candidate_ready=status == BrokerOffsetStatus.CANDIDATE_READY,
            offset_seconds=offset_seconds,
            uncertainty_seconds=uncertainty_seconds,
            sample_count=len(self._window),
            required_samples=self._required_samples,
            run_id=last.run_id if last else None,
            identity=last.identity if last else None,
            reason=reason,
            evidence_samples=evidence_samples,
            ntp_verified=ntp_verified,
            ntp_sample_count=ntp_sample_count,
            ntp_servers=ntp_servers,
            ntp_checked_at_unix=ntp_checked_at_unix,
            ntp_offset_seconds=ntp_offset_seconds,
            ntp_uncertainty_seconds=ntp_uncertainty_seconds,
        )
