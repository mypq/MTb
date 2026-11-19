
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
DEFAULT_NTP_RETRY_INTERVAL_SECONDS = 60


class NtpVerificationMonitor:
    """Runs independent NTP verification at startup and periodically."""

    def __init__(
        self,
        verifier: NtpTimeVerifier | None = None,
        *,
        interval_seconds: float = (
            DEFAULT_NTP_VERIFICATION_INTERVAL_SECONDS
        ),
        retry_interval_seconds: float = (
            DEFAULT_NTP_RETRY_INTERVAL_SECONDS
        ),
        wall_clock: Callable[[], float] | None = None,
    ) -> None:
        self._validate_interval(
            interval_seconds,
            "interval_seconds",
        )
        self._validate_interval(
            retry_interval_seconds,
            "retry_interval_seconds",
        )

        self._verifier = verifier or NtpTimeVerifier()
        self._interval_seconds = float(interval_seconds)
        self._retry_interval_seconds = float(
            retry_interval_seconds
        )
        self._wall_clock = wall_clock or time.time

        self._last_attempt: NtpVerification | None = None
        self._last_attempt_at_unix: float | None = None
        self._last_error: str | None = None

    @staticmethod
    def _validate_interval(
        value: float,
        name: str,
    ) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(
                f"{name} must be a finite positive number."
            )

    async def run(self, stop_event: asyncio.Event) -> None:
        """Verify immediately, then retry according to the result."""

        if not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event.")

        logger.info(
            "Independent NTP verification monitor started: "
            "success_interval_seconds=%s retry_interval_seconds=%s",
            self._interval_seconds,
            self._retry_interval_seconds,
        )

        try:
            while not stop_event.is_set():
                retry_required = True

                try:
                    result = await asyncio.to_thread(
                        self._verifier.verify
                    )

                    self._last_attempt = result
                    self._last_attempt_at_unix = self._wall_clock()
                    self._last_error = result.error
                    retry_required = not result.verified

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
                            "error=%s trusted_previous=%s "
                            "retry_in_seconds=%s",
                            result.error,
                            self._verifier.is_trusted(),
                            self._retry_interval_seconds,
                        )

                except Exception as exc:
                    self._last_error = (
                        f"{type(exc).__name__}: {exc}"
                    )
                    self._last_attempt_at_unix = self._wall_clock()
                    retry_required = True

                    logger.exception(
                        "Independent NTP verification raised an error; "
                        "retry_in_seconds=%s",
                        self._retry_interval_seconds,
                    )

                if stop_event.is_set():
                    break

                wait_seconds = (
                    self._retry_interval_seconds
                    if retry_required
                    else self._interval_seconds
                )

                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=wait_seconds,
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
            "retry_interval_seconds": self._retry_interval_seconds,
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
