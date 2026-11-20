
"""Periodic runtime monitor for MT5 broker-offset evidence."""

from __future__ import annotations

import asyncio
import logging
import math
import time
from dataclasses import dataclass
from pathlib import Path

from backend.market_data.broker_offset_service import (
    BrokerOffsetEvidenceService,
    BrokerOffsetServiceSnapshot,
)

logger = logging.getLogger(__name__)

DEFAULT_BROKER_OFFSET_POLL_INTERVAL_SECONDS = 1.0
DEFAULT_CLOCK_HISTORY_MAX_AGE_SECONDS = 180


@dataclass(frozen=True, slots=True)
class ClockHistoryFreshnessSnapshot:
    state: str
    age_seconds: float | None
    last_modified_unix: float | None
    max_age_seconds: int
    error: str | None = None


RECORDED_EVENT_STATES = frozenset(
    {
        "BASELINE_RECORDED",
        "CONFIRMATION_RECORDED",
        "OFFSET_CHANGE_RECORDED",
    }
)


class BrokerOffsetEvidenceMonitor:
    """Poll the evidence service without blocking the application event loop."""

    def __init__(
        self,
        service: BrokerOffsetEvidenceService,
        *,
        interval_seconds: float = (
            DEFAULT_BROKER_OFFSET_POLL_INTERVAL_SECONDS
        ),
        clock_history_max_age_seconds: int = (
            DEFAULT_CLOCK_HISTORY_MAX_AGE_SECONDS
        ),
    ) -> None:
        if not isinstance(service, BrokerOffsetEvidenceService):
            raise TypeError(
                "service must be a BrokerOffsetEvidenceService."
            )

        if (
            isinstance(interval_seconds, bool)
            or not isinstance(interval_seconds, (int, float))
            or not math.isfinite(interval_seconds)
            or interval_seconds <= 0
        ):
            raise ValueError(
                "interval_seconds must be finite and positive."
            )

        if (
            isinstance(clock_history_max_age_seconds, bool)
            or not isinstance(clock_history_max_age_seconds, int)
            or clock_history_max_age_seconds < 1
        ):
            raise ValueError(
                "clock_history_max_age_seconds must be a positive integer."
            )

        self._service = service
        self._interval_seconds = float(interval_seconds)
        self._clock_history_path = Path(service.clock_history_path)
        self._clock_history_max_age_seconds = clock_history_max_age_seconds
        self._running = False
        self._last_snapshot: BrokerOffsetServiceSnapshot | None = None
        self._last_source_freshness: ClockHistoryFreshnessSnapshot | None = None

    @property
    def is_running(self) -> bool:
        return self._running

    def snapshot(self) -> BrokerOffsetServiceSnapshot:
        if self._last_snapshot is not None:
            return self._last_snapshot

        return self._service.snapshot()

    def source_freshness(self) -> ClockHistoryFreshnessSnapshot:
        """Return file freshness without changing evidence or registry state."""
        snapshot = self._read_source_freshness()
        self._last_source_freshness = snapshot
        return snapshot

    def _read_source_freshness(self) -> ClockHistoryFreshnessSnapshot:
        try:
            stat = self._clock_history_path.stat()
        except FileNotFoundError:
            return ClockHistoryFreshnessSnapshot(
                state="MISSING",
                age_seconds=None,
                last_modified_unix=None,
                max_age_seconds=self._clock_history_max_age_seconds,
            )
        except OSError as exc:
            return ClockHistoryFreshnessSnapshot(
                state="UNAVAILABLE",
                age_seconds=None,
                last_modified_unix=None,
                max_age_seconds=self._clock_history_max_age_seconds,
                error=f"{type(exc).__name__}: {exc}",
            )

        age_seconds = max(0.0, time.time() - stat.st_mtime)
        return ClockHistoryFreshnessSnapshot(
            state=(
                "STALE"
                if age_seconds > self._clock_history_max_age_seconds
                else "FRESH"
            ),
            age_seconds=age_seconds,
            last_modified_unix=stat.st_mtime,
            max_age_seconds=self._clock_history_max_age_seconds,
        )

    async def run(self, stop_event: asyncio.Event) -> None:
        if not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event.")

        if self._running:
            raise RuntimeError(
                "Broker offset evidence monitor is already running."
            )

        self._running = True

        logger.info(
            "Broker offset evidence monitor started: "
            "interval_seconds=%s",
            self._interval_seconds,
        )

        try:
            while not stop_event.is_set():
                try:
                    freshness = await asyncio.to_thread(
                        self._read_source_freshness
                    )
                    previous_freshness = self._last_source_freshness
                    self._last_source_freshness = freshness

                    if freshness.state != "FRESH":
                        if (
                            previous_freshness is None
                            or previous_freshness.state != freshness.state
                        ):
                            logger.warning(
                                "MT5 clock history is %s; evidence polling is paused: "
                                "file=%s age_seconds=%s max_age_seconds=%s error=%s",
                                freshness.state,
                                self._clock_history_path,
                                freshness.age_seconds,
                                freshness.max_age_seconds,
                                freshness.error,
                            )
                    else:
                        if (
                            previous_freshness is not None
                            and previous_freshness.state != "FRESH"
                        ):
                            logger.info(
                                "MT5 clock history updates resumed; evidence polling is active: "
                                "file=%s age_seconds=%.3f",
                                self._clock_history_path,
                                freshness.age_seconds or 0.0,
                            )

                        snapshot = await asyncio.to_thread(
                            self._service.poll_once
                        )

                        self._last_snapshot = snapshot

                    snapshot = self.snapshot()

                    if freshness.state == "FRESH" and snapshot.state == "ERROR":
                        logger.error(
                            "Broker offset evidence processing failed: %s",
                            snapshot.error,
                        )
                    elif (
                        freshness.state == "FRESH"
                        and snapshot.state in RECORDED_EVENT_STATES
                    ):
                        logger.info(
                            "Broker offset event recorded: "
                            "state=%s offset_seconds=%s "
                            "run_id=%s identity=%s",
                            snapshot.state,
                            (
                                snapshot.last_record.offset_seconds
                                if snapshot.last_record is not None
                                else None
                            ),
                            (
                                snapshot.last_record.run_id
                                if snapshot.last_record is not None
                                else None
                            ),
                            (
                                snapshot.last_record.identity
                                if snapshot.last_record is not None
                                else None
                            ),
                        )

                except Exception as exc:
                    previous = self._last_snapshot

                    if previous is None:
                        try:
                            previous = self._service.snapshot()
                        except Exception:
                            previous = None

                    self._last_snapshot = BrokerOffsetServiceSnapshot(
                        state="ERROR",
                        samples_processed=(
                            previous.samples_processed
                            if previous is not None
                            else 0
                        ),
                        last_decision=None,
                        last_record=(
                            previous.last_record
                            if previous is not None
                            else None
                        ),
                        error=f"{type(exc).__name__}: {exc}",
                    )

                    logger.exception(
                        "Unexpected broker offset monitor failure."
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
            self._running = False

            logger.info(
                "Broker offset evidence monitor stopped."
            )
