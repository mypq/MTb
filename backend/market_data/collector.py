
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from math import isfinite
from typing import TYPE_CHECKING

from backend.market_data.capabilities import (
    CapabilityRegistry,
    CapabilityStatus,
    MarketDataCapability,
)
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
    alignment_offset_seconds: float = 0.0,
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

    if (
        isinstance(alignment_offset_seconds, bool)
        or not isinstance(alignment_offset_seconds, (int, float))
        or not isfinite(alignment_offset_seconds)
        or abs(alignment_offset_seconds) > 14 * 60 * 60
    ):
        raise ValueError(
            "alignment_offset_seconds must be a finite broker offset "
            "within 14 hours."
        )

    current = now or datetime.now(timezone.utc)

    if current.tzinfo is None or current.utcoffset() is None:
        raise ValueError("now must be timezone-aware.")

    current = current.astimezone(timezone.utc)

    # MT5 candle boundaries are based on broker-server time. Applying only
    # the independently verified current offset keeps H4/D1 scheduling
    # aligned without treating raw MT5 timestamps as UTC.
    broker_aligned_timestamp = (
        current.timestamp() + alignment_offset_seconds
    )
    next_close_timestamp = (
        (int(broker_aligned_timestamp) // timeframe_seconds) + 1
    ) * timeframe_seconds

    return datetime.fromtimestamp(
        next_close_timestamp
        - alignment_offset_seconds
        + delay_seconds,
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
        capability_registry: CapabilityRegistry | None = None,
        timeframes: tuple[str, ...] | list[str] | None = None,
        clock_calibrator=None,
    ) -> None:
        if not isinstance(symbol, str) or not symbol.strip():
            raise ValueError("Symbol must be a non-empty string.")

        if isinstance(count, bool) or not isinstance(count, int):
            raise TypeError("Count must be an integer.")

        if not 1 <= count <= 10000:
            raise ValueError("Count must be between 1 and 10000.")

        normalized_timeframe = normalize_timeframe(timeframe)
        frame_seconds = timeframe_seconds(normalized_timeframe)

        self._service = service
        self._repository = repository
        self._symbol = symbol.strip()
        self._timeframe = normalized_timeframe
        self._count = count
        self._timeframe_seconds = frame_seconds
        self._capability_registry = capability_registry
        self._clock_calibrator = clock_calibrator

        configured_timeframes = (
            (normalized_timeframe,)
            if timeframes is None
            else timeframes
        )
        if (
            isinstance(configured_timeframes, (str, bytes))
            or not isinstance(configured_timeframes, (tuple, list))
            or not configured_timeframes
        ):
            raise ValueError(
                "timeframes must be a non-empty tuple or list."
            )

        normalized_timeframes = tuple(
            normalize_timeframe(value)
            for value in configured_timeframes
        )
        if len(set(normalized_timeframes)) != len(normalized_timeframes):
            raise ValueError("timeframes must not contain duplicates.")
        if normalized_timeframe not in normalized_timeframes:
            raise ValueError(
                "The primary timeframe must be included in timeframes."
            )
        if len(normalized_timeframes) > 1 and clock_calibrator is None:
            raise ValueError(
                "A broker candle clock calibrator is required for "
                "multi-timeframe scheduling."
            )

        self._timeframes = normalized_timeframes

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def timeframe(self) -> str:
        return self._timeframe

    @property
    def count(self) -> int:
        return self._count

    @property
    def timeframes(self) -> tuple[str, ...]:
        return self._timeframes

    def collect_once(self) -> MarketDataSaveResult | None:
        """Collect the primary configured timeframe once."""

        return self.collect_timeframe_once(self._timeframe)

    def collect_timeframe_once(
        self,
        timeframe: str,
    ) -> MarketDataSaveResult | None:
        """Collect and persist one explicitly configured timeframe."""

        normalized_timeframe = normalize_timeframe(timeframe)
        if normalized_timeframe not in self._timeframes:
            raise ValueError(
                f"Timeframe {normalized_timeframe} is not configured."
            )

        snapshot = self._service.get_snapshot(
            symbol=self._symbol,
            timeframe=normalized_timeframe,
            count=self._count,
        )

        if not snapshot.analysis_eligible:
            self._record_candle_capability(
                snapshot,
                CapabilityStatus.PARTIAL,
                "MT5 returned candles, but market-data validation did not mark the batch analysis-eligible.",
            )
            logger.warning(
                "Market data collection rejected: "
                "symbol=%s timeframe=%s issues=%s",
                self._symbol,
                normalized_timeframe,
                [
                    issue.code.value
                    for issue in snapshot.validation.issues
                ],
            )
            return None

        result = self._repository.save_snapshot(snapshot)
        self._record_candle_capability(
            snapshot,
            CapabilityStatus.AVAILABLE,
            "MT5 returned clock-verified candles; validation passed and persistence completed.",
        )

        logger.info(
            "Market data collection completed: "
            "symbol=%s timeframe=%s inserted=%d duplicates=%d",
            self._symbol,
            normalized_timeframe,
            result.inserted_candles,
            result.duplicate_candles,
        )

        return result

    def collect_all_once(self) -> dict[str, MarketDataSaveResult | None]:
        """Collect every configured timeframe sequentially."""

        results: dict[str, MarketDataSaveResult | None] = {}
        for timeframe in self._timeframes:
            try:
                results[timeframe] = self.collect_timeframe_once(timeframe)
            except Exception:
                logger.exception(
                    "Market data collection failed: symbol=%s timeframe=%s",
                    self._symbol,
                    timeframe,
                )
                results[timeframe] = None
        return results

    def _verified_alignment_offset(self) -> float | None:
        if self._clock_calibrator is None:
            return 0.0

        try:
            clock, ntp = self._clock_calibrator.trusted_evidence()
        except Exception:
            logger.exception(
                "Unable to read trusted broker clock for collection scheduling."
            )
            return None

        offset = getattr(clock, "offset_seconds", None)
        if (
            getattr(clock, "status", None) != "VERIFIED"
            or ntp is None
            or isinstance(offset, bool)
            or not isinstance(offset, (int, float))
            or not isfinite(offset)
            or abs(offset) > 14 * 60 * 60
        ):
            return None

        return float(offset)

    def _record_candle_capability(
        self,
        snapshot,
        status: CapabilityStatus,
        evidence: str,
    ) -> None:
        if self._capability_registry is None:
            return

        batch = snapshot.batch
        clock = batch.clock_evidence
        if (
            batch.source != "MT5"
            or clock is None
            or clock.verified is not True
        ):
            return

        limitations = ()
        if status is CapabilityStatus.PARTIAL:
            limitations = tuple(
                sorted({issue.code.value for issue in snapshot.validation.issues})
            )

        self._capability_registry.record(MarketDataCapability(
            capability="candle_history",
            status=status,
            source="MT5",
            checked_at=datetime.now(timezone.utc),
            symbol=batch.symbol,
            timeframe=batch.timeframe,
            account_login=clock.account_login,
            broker_server=clock.broker_server,
            evidence=(evidence,),
            limitations=limitations,
        ))

    async def run(self, stop_event: asyncio.Event) -> None:
        """Collect immediately and schedule configured broker timeframes."""

        if not isinstance(stop_event, asyncio.Event):
            raise TypeError("stop_event must be an asyncio.Event.")

        logger.info(
            "Market data collector started: "
            "symbol=%s timeframes=%s",
            self._symbol,
            ",".join(self._timeframes),
        )

        try:
            if len(self._timeframes) == 1:
                await self._run_single_timeframe(stop_event)
            else:
                await self._run_multiple_timeframes(stop_event)
        finally:
            logger.info(
                "Market data collector stopped: "
                "symbol=%s timeframes=%s",
                self._symbol,
                ",".join(self._timeframes),
            )

    async def _run_single_timeframe(
        self,
        stop_event: asyncio.Event,
    ) -> None:
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
                (next_run - datetime.now(timezone.utc)).total_seconds(),
            )
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=wait_seconds,
                )
            except asyncio.TimeoutError:
                continue

    async def _run_multiple_timeframes(
        self,
        stop_event: asyncio.Event,
    ) -> None:
        # Initial history collection is sequential: MT5Adapter serializes all
        # terminal calls through one lock, so parallel collectors add no value.
        await asyncio.to_thread(self.collect_all_once)

        alignment_offset = self._verified_alignment_offset()
        if alignment_offset is None:
            logger.warning(
                "Multi-timeframe scheduler is waiting for trusted broker "
                "clock evidence; no UTC-based fallback will be used."
            )

        next_runs: dict[str, datetime] = {}
        if alignment_offset is not None:
            now = datetime.now(timezone.utc)
            next_runs = {
                timeframe: next_collection_time(
                    timeframe_seconds=timeframe_seconds(timeframe),
                    now=now,
                    alignment_offset_seconds=alignment_offset,
                )
                for timeframe in self._timeframes
            }

        last_alignment_offset = alignment_offset

        while not stop_event.is_set():
            alignment_offset = self._verified_alignment_offset()
            if alignment_offset is None:
                try:
                    await asyncio.wait_for(
                        stop_event.wait(),
                        timeout=5.0,
                    )
                except asyncio.TimeoutError:
                    continue
                break

            now = datetime.now(timezone.utc)
            if (
                last_alignment_offset is None
                or abs(alignment_offset - last_alignment_offset) > 0.5
            ):
                next_runs = {
                    timeframe: next_collection_time(
                        timeframe_seconds=timeframe_seconds(timeframe),
                        now=now,
                        alignment_offset_seconds=alignment_offset,
                    )
                    for timeframe in self._timeframes
                }
                last_alignment_offset = alignment_offset

            due_timeframes = [
                timeframe
                for timeframe, due_at in next_runs.items()
                if due_at <= now
            ]

            for timeframe in due_timeframes:
                if stop_event.is_set():
                    break
                try:
                    await asyncio.to_thread(
                        self.collect_timeframe_once,
                        timeframe,
                    )
                except Exception:
                    logger.exception(
                        "Multi-timeframe collection cycle failed: "
                        "symbol=%s timeframe=%s",
                        self._symbol,
                        timeframe,
                    )

                alignment_offset = self._verified_alignment_offset()
                if alignment_offset is None:
                    next_runs.pop(timeframe, None)
                    continue

                next_runs[timeframe] = next_collection_time(
                    timeframe_seconds=timeframe_seconds(timeframe),
                    now=datetime.now(timezone.utc),
                    alignment_offset_seconds=alignment_offset,
                )
                last_alignment_offset = alignment_offset

            if stop_event.is_set():
                break

            if not next_runs:
                wait_seconds = 5.0
            else:
                now = datetime.now(timezone.utc)
                next_due = min(next_runs.values())
                wait_seconds = min(
                    30.0,
                    max(0.1, (next_due - now).total_seconds()),
                )

            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=wait_seconds,
                )
            except asyncio.TimeoutError:
                continue
