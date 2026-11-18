
from __future__ import annotations

import math
import socket
import struct
import time
from dataclasses import dataclass
from statistics import median
from typing import Callable, Sequence


NTP_EPOCH_OFFSET = 2_208_988_800
NTP_PACKET_SIZE = 48


@dataclass(frozen=True, slots=True)
class NtpSample:
    server: str
    offset_seconds: float
    round_trip_seconds: float
    root_dispersion_seconds: float


@dataclass(frozen=True, slots=True)
class NtpVerification:
    verified: bool
    checked_at_unix: float | None
    offset_seconds: float | None
    uncertainty_seconds: float | None
    sample_count: int
    servers: tuple[str, ...]
    error: str | None = None


class NtpTimeVerifier:
    """
    Verifies the local system clock against multiple NTP servers.

    This implementation uses unauthenticated NTP over UDP.
    It validates the response structure and source endpoint, but
    does not provide NTS cryptographic authentication.

    A successful verification requires:
    - At least three distinct valid server samples.
    - Consistent offsets across the samples.
    - An acceptable median local-clock offset.
    - An acceptable conservative uncertainty estimate.

    Any failed verification immediately invalidates previous trust.
    """

    DEFAULT_SERVERS = (
        "time.google.com",
        "time.cloudflare.com",
        "pool.ntp.org",
    )

    def __init__(
        self,
        *,
        servers: Sequence[str] | None = None,
        timeout_seconds: float = 2.0,
        max_round_trip_seconds: float = 2.0,
        max_root_dispersion_seconds: float = 1.0,
        max_offset_seconds: float = 1.0,
        max_offset_spread_seconds: float = 0.5,
        max_uncertainty_seconds: float = 1.0,
        max_age_seconds: float = 6 * 60 * 60,
        wall_clock: Callable[[], float] | None = None,
        monotonic_clock: Callable[[], float] | None = None,
        query: Callable[[str], NtpSample] | None = None,
    ) -> None:
        selected_servers = tuple(
            servers if servers is not None else self.DEFAULT_SERVERS
        )

        normalized_servers = tuple(
            server.strip().lower()
            for server in selected_servers
            if isinstance(server, str) and server.strip()
        )

        if len(normalized_servers) < 3:
            raise ValueError(
                "At least three distinct NTP server names are required."
            )

        if len(set(normalized_servers)) != len(normalized_servers):
            raise ValueError(
                "Duplicate NTP server names are not allowed."
            )

        if len(normalized_servers) != len(selected_servers):
            raise ValueError(
                "NTP server names must be non-empty strings."
            )

        self._servers = selected_servers

        self._timeout_seconds = self._positive(
            timeout_seconds, "timeout_seconds"
        )
        self._max_round_trip_seconds = self._positive(
            max_round_trip_seconds, "max_round_trip_seconds"
        )
        self._max_root_dispersion_seconds = self._positive(
            max_root_dispersion_seconds,
            "max_root_dispersion_seconds",
        )
        self._max_offset_seconds = self._positive(
            max_offset_seconds, "max_offset_seconds"
        )
        self._max_offset_spread_seconds = self._positive(
            max_offset_spread_seconds,
            "max_offset_spread_seconds",
        )
        self._max_uncertainty_seconds = self._positive(
            max_uncertainty_seconds,
            "max_uncertainty_seconds",
        )
        self._max_age_seconds = self._positive(
            max_age_seconds, "max_age_seconds"
        )

        self._wall_clock = wall_clock or time.time
        self._monotonic_clock = monotonic_clock or time.monotonic
        self._query = query or self._query_server

        self._last_success_monotonic: float | None = None
        self._last_success_unix: float | None = None
        self._last_verification: NtpVerification | None = None

    @staticmethod
    def _positive(value: float, name: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(
                f"{name} must be a finite positive number."
            )

        return float(value)

    @staticmethod
    def _encode_timestamp(unix_seconds: float) -> bytes:
        ntp_seconds = unix_seconds + NTP_EPOCH_OFFSET
        whole_seconds = int(ntp_seconds)
        fraction = int(
            (ntp_seconds - whole_seconds) * (2**32)
        )

        return struct.pack(
            "!II",
            whole_seconds & 0xFFFFFFFF,
            fraction & 0xFFFFFFFF,
        )

    @staticmethod
    def _decode_timestamp(packet: bytes, offset: int) -> float:
        seconds, fraction = struct.unpack_from(
            "!II",
            packet,
            offset,
        )

        return (
            seconds
            - NTP_EPOCH_OFFSET
            + fraction / (2**32)
        )

    def _query_server(self, server: str) -> NtpSample:
        t1 = self._wall_clock()

        if not math.isfinite(t1) or t1 <= 0:
            raise RuntimeError(
                "Local system clock returned an invalid time."
            )

        packet = bytearray(NTP_PACKET_SIZE)
        packet[0] = 0x23
        packet[40:48] = self._encode_timestamp(t1)

        # A connected UDP socket restricts received packets to the
        # remote endpoint selected by connect(), rather than accepting
        # arbitrary datagram senders.
        with socket.socket(
            socket.AF_INET,
            socket.SOCK_DGRAM,
        ) as client:
            client.settimeout(self._timeout_seconds)
            client.connect((server, 123))
            client.send(packet)
            response = client.recv(512)

        t4 = self._wall_clock()

        if len(response) < NTP_PACKET_SIZE:
            raise RuntimeError(
                "NTP response is shorter than 48 bytes."
            )

        leap_indicator = response[0] >> 6
        version = (response[0] >> 3) & 0x07
        mode = response[0] & 0x07
        stratum = response[1]

        if leap_indicator == 3:
            raise RuntimeError(
                "NTP server reports an unsynchronized clock."
            )

        if version not in (3, 4) or mode != 4:
            raise RuntimeError(
                "Invalid NTP response version or mode."
            )

        if not 1 <= stratum <= 15:
            raise RuntimeError(
                "NTP server returned an invalid stratum."
            )

        if response[24:32] != packet[40:48]:
            raise RuntimeError(
                "NTP originate timestamp does not match."
            )

        t2 = self._decode_timestamp(response, 32)
        t3 = self._decode_timestamp(response, 40)

        if (
            not all(math.isfinite(v) for v in (t1, t2, t3, t4))
            or t4 < t1
            or t3 < t2
            or t2 <= 0
            or t3 <= 0
        ):
            raise RuntimeError(
                "NTP timestamps are inconsistent."
            )

        round_trip = (t4 - t1) - (t3 - t2)

        if (
            not math.isfinite(round_trip)
            or round_trip < 0
            or round_trip > self._max_round_trip_seconds
        ):
            raise RuntimeError(
                "NTP round-trip time is outside the limit."
            )

        offset = ((t2 - t1) + (t3 - t4)) / 2.0

        root_delay_raw = struct.unpack_from(
            "!i",
            response,
            4,
        )[0]

        root_dispersion_raw = struct.unpack_from(
            "!I",
            response,
            8,
        )[0]

        root_delay = root_delay_raw / (2**16)
        root_dispersion = root_dispersion_raw / (2**16)

        if (
            not math.isfinite(offset)
            or not math.isfinite(root_delay)
            or not math.isfinite(root_dispersion)
            or root_dispersion < 0
            or root_dispersion > self._max_root_dispersion_seconds
        ):
            raise RuntimeError(
                "NTP response uncertainty is outside the limit."
            )

        if root_delay < -self._max_round_trip_seconds:
            raise RuntimeError(
                "NTP root delay is outside the supported limit."
            )

        return NtpSample(
            server=server,
            offset_seconds=offset,
            round_trip_seconds=round_trip,
            root_dispersion_seconds=root_dispersion,
        )

    def verify(self) -> NtpVerification:
        # A new verification attempt starts by invalidating previous
        # trust. Only this attempt can establish a new trusted result.
        self.reset()

        samples: list[NtpSample] = []
        errors: list[str] = []
        used_servers: set[str] = set()

        for server in self._servers:
            try:
                sample = self._query(server)

                if not isinstance(sample, NtpSample):
                    raise RuntimeError(
                        "NTP query returned an invalid sample."
                    )

                if (
                    not isinstance(sample.server, str)
                    or sample.server.strip().lower()
                    != server.strip().lower()
                ):
                    raise RuntimeError(
                        "NTP sample server identity does not match."
                    )

                normalized_server = sample.server.strip().lower()

                if normalized_server in used_servers:
                    raise RuntimeError(
                        "Duplicate NTP sample server."
                    )

                if not all(
                    math.isfinite(value)
                    for value in (
                        sample.offset_seconds,
                        sample.round_trip_seconds,
                        sample.root_dispersion_seconds,
                    )
                ):
                    raise RuntimeError(
                        "NTP sample contains non-finite values."
                    )

                if (
                    sample.round_trip_seconds < 0
                    or sample.round_trip_seconds
                    > self._max_round_trip_seconds
                ):
                    raise RuntimeError(
                        "NTP sample round-trip is invalid."
                    )

                if (
                    sample.root_dispersion_seconds < 0
                    or sample.root_dispersion_seconds
                    > self._max_root_dispersion_seconds
                ):
                    raise RuntimeError(
                        "NTP sample dispersion is invalid."
                    )

                used_servers.add(normalized_server)
                samples.append(sample)

            except Exception as exc:
                errors.append(
                    f"{server}: {type(exc).__name__}: {exc}"
                )

        if len(samples) < 3:
            return self._failed(
                "Fewer than three valid NTP samples. "
                + "; ".join(errors)
            )

        offsets = [
            sample.offset_seconds
            for sample in samples
        ]

        offset = median(offsets)
        spread = max(offsets) - min(offsets)

        if spread > self._max_offset_spread_seconds:
            return self._failed(
                "NTP server offsets disagree beyond the allowed spread."
            )

        if abs(offset) >= self._max_offset_seconds:
            return self._failed(
                "Local clock offset is at least the allowed limit."
            )

        # Use the worst sample's network and server uncertainty,
        # rather than the median, then include inter-server disagreement.
        uncertainty = max(
            sample.round_trip_seconds / 2.0
            + sample.root_dispersion_seconds
            for sample in samples
        ) + spread / 2.0

        if (
            not math.isfinite(uncertainty)
            or uncertainty > self._max_uncertainty_seconds
        ):
            return self._failed(
                "NTP uncertainty exceeds the allowed limit."
            )

        checked_at = self._wall_clock()
        monotonic_now = self._monotonic_clock()

        if (
            not math.isfinite(checked_at)
            or checked_at <= 0
            or not math.isfinite(monotonic_now)
        ):
            return self._failed(
                "Local clock readings are invalid."
            )

        result = NtpVerification(
            verified=True,
            checked_at_unix=checked_at,
            offset_seconds=offset,
            uncertainty_seconds=uncertainty,
            sample_count=len(samples),
            servers=tuple(
                sample.server for sample in samples
            ),
        )

        self._last_success_unix = checked_at
        self._last_success_monotonic = monotonic_now
        self._last_verification = result

        return result

    def _failed(self, error: str) -> NtpVerification:
        self.reset()

        return NtpVerification(
            verified=False,
            checked_at_unix=None,
            offset_seconds=None,
            uncertainty_seconds=None,
            sample_count=0,
            servers=(),
            error=error,
        )

    def is_trusted(self) -> bool:
        if (
            self._last_success_monotonic is None
            or self._last_success_unix is None
            or self._last_verification is None
            or not self._last_verification.verified
        ):
            return False

        now_monotonic = self._monotonic_clock()
        elapsed = now_monotonic - self._last_success_monotonic

        if (
            not math.isfinite(elapsed)
            or elapsed < 0
            or elapsed > self._max_age_seconds
        ):
            return False

        return True

    def last_verification(self) -> NtpVerification | None:
        if not self.is_trusted():
            return None

        return self._last_verification

    def reset(self) -> None:
        self._last_success_monotonic = None
        self._last_success_unix = None
        self._last_verification = None
