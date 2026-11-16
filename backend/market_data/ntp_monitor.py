from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import asdict
from typing import Callable

from backend.market_data.ntp_time import (
    NtpTimeVerifier,
    NtpVerification,
)

logger = logging.getLogger(__name__)

DEFAULT_NTP_VERIFICATION_INTERVAL_SECONDS = 60 * 60


class NtpVerificationMonitor:
    """Runs independent NTP verification at startup and periodically."""

    def __init__(
        self,
        verifier: NtpTimeVerifier | None = None,
        *,
        interval_seconds: float = (
            DEFAULT_NTP_VERIFICATION_INTERVAL_SECONDS
        ),
        wall_clock: Callable[[], float] | None = None,
    ) -> None:
        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (int, float))
            or not math.isfinite(interval_seconds)
            or interval_seconds <= 0
        ):
            raise ValueError(
                "interval_seconds must be a finite positive number."
            )

        self._verifier = verifier or NtpTimeVerifier()
        self._interval_seconds = float(interval_seconds)
        self._wall_clock = wall_clock or time.time

        self._last_attempt: NtpVerification | None = None
        self._last_attempt_at_unix: float | None = None
        self._last_error: str | None = None

    async def run(self, stop_event: asyncio.Event) -> None:
        """Verify immediately, then repeat at the configured interval."""

        if not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event.")

        logger.info(
            "Independent NTP verification monitor started: "
            "interval_seconds=%s",
            self._interval_seconds,
        )

        try:
            while not stop_event.is_set():
                try:
                    result = await asyncio.to_thread(
                        self._verifier.verify
                    )

                    self._last_attempt = result
                    self._last_attempt_at_unix = self._wall_clock()
                    self._last_error = result.error

                    if result.verified:
                        logger.info(
                            "Independent NTP verification succeeded: "
                            "offset_seconds=%.6f "
                            "uncertainty_seconds=%.6f "
                            "samples=%d servers=%s",
                            result.offset_seconds,
                            result.uncertainty_seconds,
                            result.sample_count,
                            ",".join(result.servers),
                        )
                    else:
                        logger.warning(
                            "Independent NTP verification failed: "
                            "error=%s trusted_previous=%s",
                            result.error,
                            self._verifier.is_trusted(),
                        )

                except Exception as exc:
                    self._last_error = (
                        f"{type(exc).__name__}: {exc}"
                    )
                    self._last_attempt_at_unix = self._wall_clock()

                    logger.exception(
                        "Independent NTP verification raised an error."
                    )

                if stop_event.is_set():
                    break

                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._interval_seconds,
                    )
                except asyncio.TimeoutError:
                    continue

        finally:
            logger.info(
                "Independent NTP verification monitor stopped."
            )

    def snapshot(self) -> dict[str, object]:
        """Return the latest attempt and currently trusted verification."""

        last_success = self._verifier.last_verification()

        return {
            "provider": "external_ntp_udp_123",
            "trusted": self._verifier.is_trusted(),
            "verification_interval_seconds": self._interval_seconds,
            "last_attempt": (
                asdict(self._last_attempt)
                if self._last_attempt is not None
                else None
            ),
            "last_attempt_at_unix": self._last_attempt_at_unix,
            "last_success": (
                asdict(last_success)
                if last_success is not None
                else None
            ),
            "last_error": self._last_error,
        }
