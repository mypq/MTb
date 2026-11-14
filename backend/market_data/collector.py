
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from math import isfinite
from typing import TYPE_CHECKING

from backend.market_data.repository import (
    MarketDataRepository,
    MarketDataSaveResult,
)
from backend.mt5.timeframes import (
    normalize_timeframe,
    timeframe_seconds,
)

if TYPE_CHECKING:
    from backend.market_data.service import MarketDataService


logger = logging.getLogger(__name__)


def next_collection_time(
    *,
    timeframe_seconds: int,
    now: datetime | None = None,
    delay_seconds: float = 2.0,
) -> datetime:
    """Return the next candle close time plus a broker-update delay."""

    if (
        isinstance(timeframe_seconds, bool)
        or not isinstance(timeframe_seconds, int)
        or timeframe_seconds <= 0
    ):
        raise ValueError("timeframe_seconds must be a positive integer.")

    if (
        isinstance(delay_seconds, bool)
        or not isinstance(delay_seconds, (int, float))
        or not isfinite(delay_seconds)
        or delay_seconds < 0
    ):
        raise ValueError(
            "delay_seconds must be a non-negative finite number."
        )

    current = now or datetime.now(timezone.utc)

    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware.")

    current = current.astimezone(timezone.utc)

    timestamp = current.timestamp()
    next_close_timestamp = (
        (int(timestamp) // timeframe_seconds) + 1
    ) * timeframe_seconds

    return datetime.fromtimestamp(
        next_close_timestamp + delay_seconds,
        tz=timezone.utc,
    )


class MarketDataCollector:
    """Periodically collects, validates, and persists closed market candles."""

    def __init__(
        self,
        service: MarketDataService,
        repository: MarketDataRepository,
        *,
        symbol: str,
        timeframe: str,
        count: int = 300,
        interval_seconds: float | None = None,
    ) -> None:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must be a non-empty string.")

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("Count must be an integer.")

        if not 1 <= count <= 10000:
            raise ValueError("Count must be between 1 and 10000.")

        normalized_timeframe = normalize_timeframe(timeframe)
        frame_seconds = timeframe_seconds(normalized_timeframe)

        if interval_seconds is None:
            interval = float(frame_seconds)
        else:
            if (
                isinstance(interval_seconds, bool)
                or not isinstance(interval_seconds, (int, float))
                or not isfinite(interval_seconds)
                or interval_seconds <= 0
            ):
                raise ValueError(
                    "interval_seconds must be a positive finite number."
                )

            interval = float(interval_seconds)

        self._service = service
        self._repository = repository
        self._symbol = symbol.strip()
        self._timeframe = normalized_timeframe
        self._count = count
        self._interval_seconds = interval
        self._timeframe_seconds = frame_seconds

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def timeframe(self) -> str:
        return self._timeframe

    @property
    def count(self) -> int:
        return self._count

    def collect_once(self) -> MarketDataSaveResult | None:
        """Collect one batch and persist it only when analysis-eligible."""

        snapshot = self._service.get_snapshot(
            symbol=self._symbol,
            timeframe=self._timeframe,
            count=self._count,
        )

        if not snapshot.analysis_eligible:
            logger.warning(
                "Market data collection rejected: "
                "symbol=%s timeframe=%s issues=%s",
                self._symbol,
                self._timeframe,
                [
                    issue.code.value
                    for issue in snapshot.validation.issues
                ],
            )
            return None

        result = self._repository.save_snapshot(snapshot)

        logger.info(
            "Market data collection completed: "
            "symbol=%s timeframe=%s inserted=%d duplicates=%d",
            self._symbol,
            self._timeframe,
            result.inserted_candles,
            result.duplicate_candles,
        )

        return result

    async def run(self, stop_event: asyncio.Event) -> None:
        """Collect immediately, then align each cycle with candle closes."""

        if not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event.")

        logger.info(
            "Market data collector started: "
            "symbol=%s timeframe=%s",
            self._symbol,
            self._timeframe,
        )

        try:
            while not stop_event.is_set():
                try:
                    await asyncio.to_thread(self.collect_once)
                except Exception:
                    logger.exception(
                        "Market data collection cycle failed: "
                        "symbol=%s timeframe=%s",
                        self._symbol,
                        self._timeframe,
                    )

                if stop_event.is_set():
                    break

                next_run = next_collection_time(
                    timeframe_seconds=self._timeframe_seconds,
                )

                wait_seconds = max(
                    0.0,
                    (
                        next_run - datetime.now(timezone.utc)
                    ).total_seconds(),
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
                "Market data collector stopped: "
                "symbol=%s timeframe=%s",
                self._symbol,
                self._timeframe,
            )
