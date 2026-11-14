
from __future__ import annotations

import asyncio
import logging
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

        if interval_seconds is None:
            interval = float(timeframe_seconds(normalized_timeframe))
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

    @property
    def interval_seconds(self) -> float:
        return self._interval_seconds

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
        """Run collection cycles until the supplied stop event is set."""

        if not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event.")

        logger.info(
            "Market data collector started: "
            "symbol=%s timeframe=%s interval=%.1f seconds",
            self._symbol,
            self._timeframe,
            self._interval_seconds,
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

                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=self._interval_seconds,
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
