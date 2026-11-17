
"""Periodic runtime monitor for MT5 broker-offset evidence."""

from __future__ import annotations

import asyncio
import logging
import math

from backend.market_data.broker_offset_service import (
    BrokerOffsetEvidenceService,
    BrokerOffsetServiceSnapshot,
)

logger = logging.getLogger(__name__)

DEFAULT_BROKER_OFFSET_POLL_INTERVAL_SECONDS = 1.0


class BrokerOffsetEvidenceMonitor:
    """Poll the evidence service without blocking the application event loop."""

    def __init__(
        self,
        service: BrokerOffsetEvidenceService,
        *,
        interval_seconds: float = (
            DEFAULT_BROKER_OFFSET_POLL_INTERVAL_SECONDS
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

        self._service = service
        self._interval_seconds = float(interval_seconds)
        self._running = False
        self._last_snapshot: BrokerOffsetServiceSnapshot | None = None

    @property
    def is_running(self) -> bool:
        return self._running

    def snapshot(self) -> BrokerOffsetServiceSnapshot:
        if self._last_snapshot is not None:
            return self._last_snapshot

        return self._service.snapshot()

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
                    snapshot = await asyncio.to_thread(
                        self._service.poll_once
                    )

                    self._last_snapshot = snapshot

                    if snapshot.state == "ERROR":
                        logger.error(
                            "Broker offset evidence processing failed: %s",
                            snapshot.error,
                        )
                    elif snapshot.state == "CANDIDATE_RECORDED":
                        logger.info(
                            "Broker offset candidate recorded: "
                            "offset_seconds=%s run_id=%s identity=%s",
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

                except Exception:
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
