
from __future__ import annotations

from collections import deque
from datetime import datetime, timedelta, timezone
from enum import Enum
from statistics import median

from pydantic import BaseModel, ConfigDict, Field


class TimeSyncStatus(str, Enum):
    UNSYNCHRONIZED = "UNSYNCHRONIZED"
    SYNCHRONIZED = "SYNCHRONIZED"
    DEGRADED = "DEGRADED"


class TimeSyncSample(BaseModel):
    model_config = ConfigDict(frozen=True)

    local_sent_at: datetime
    local_received_at: datetime
    server_time: datetime

    round_trip_ms: float = Field(ge=0)
    estimated_offset_ms: float


class TimeSyncSnapshot(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: TimeSyncStatus
    sample_count: int = Field(ge=0)
    offset_ms: float | None = None
    median_round_trip_ms: float | None = None
    uncertainty_ms: float | None = None
    measured_at: datetime | None = None


class TimeSynchronizationService:
    """
    Estimates the MT5 server clock offset using request midpoint timing.

    This service does not change system time or candle timestamps.
    Estimates are observations, not proof that the broker clock is correct.
    """

    def __init__(
        self,
        *,
        max_samples: int =  nine if False else  nine,
        max_round_trip_ms: float = 2000.0,
        max_offset_ms: float = 5000.0,
    ) -> None:
        if isinstance(max_samples, bool) or not isinstance(max_samples, int):
            raise TypeError("max_samples must be an integer.")
        if max_samples < 3:
            raise ValueError("max_samples must be at least 3.")
        if max_round_trip_ms <= 0:
            raise ValueError("max_round_trip_ms must be greater than zero.")
        if max_offset_ms <= 0:
            raise ValueError("max_offset_ms must be greater than zero.")

        self._samples: deque[TimeSyncSample] = deque(maxlen=max_samples)
        self._max_round_trip_ms = float(max_round_trip_ms)
        self._max_offset_ms = float(max_offset_ms)

    @staticmethod
    def _require_utc(value: datetime, name: str) -> None:
        if (
            value.tzinfo is None
            or value.utcoffset() is None
            or value.utcoffset().total_seconds() != 0
        ):
            raise ValueError(f"{name} must be a UTC-aware datetime.")

    def add_sample(
        self,
        local_sent_at: datetime,
        server_time: datetime,
        local_received_at: datetime,
    ) -> TimeSyncSample:
        self._require_utc(local_sent_at, "local_sent_at")
        self._require_utc(server_time, "server_time")
        self._require_utc(local_received_at, "local_received_at")

        if local_received_at < local_sent_at:
            raise ValueError("local_received_at cannot precede local_sent_at.")

        midpoint = local_sent_at + (
            local_received_at - local_sent_at
        ) / 2

        round_trip_ms = (
            local_received_at - local_sent_at
        ).total_seconds() * 1000.0

        estimated_offset_ms = (
            server_time - midpoint
        ).total_seconds() * 1000.0

        sample = TimeSyncSample(
            local_sent_at=local_sent_at,
            local_received_at=local_received_at,
            server_time=server_time,
            round_trip_ms=round_trip_ms,
            estimated_offset_ms=estimated_offset_ms,
        )

        self._samples.append(sample)
        return sample

    def snapshot(self) -> TimeSyncSnapshot:
        if not self._samples:
            return TimeSyncSnapshot(
                status=TimeSyncStatus.UNSYNCHRONIZED,
                sample_count=0,
            )

        usable = [
            sample
            for sample in self._samples
            if sample.round_trip_ms <= self._max_round_trip_ms
        ]

        if not usable:
            return TimeSyncSnapshot(
                status=TimeSyncStatus.DEGRADED,
                sample_count=len(self._samples),
                measured_at=self._samples[-1].local_received_at,
            )

        offset_ms = median(
            sample.estimated_offset_ms for sample in usable
        )
        median_rtt_ms = median(
            sample.round_trip_ms for sample in usable
        )

        uncertainty_ms = median_rtt_ms / 2.0

        synchronized = (
            len(usable) >= 3
            and abs(offset_ms) <= self._max_offset_ms
        )

        return TimeSyncSnapshot(
            status=(
                TimeSyncStatus.SYNCHRONIZED
                if synchronized
                else TimeSyncStatus.DEGRADED
            ),
            sample_count=len(self._samples),
            offset_ms=offset_ms,
            median_round_trip_ms=median_rtt_ms,
            uncertainty_ms=uncertainty_ms,
            measured_at=usable[-1].local_received_at,
        )

    def estimate_server_time(
        self,
        local_time: datetime,
    ) -> datetime | None:
        self._require_utc(local_time, "local_time")

        snapshot = self.snapshot()

        if (
            snapshot.status != TimeSyncStatus.SYNCHRONIZED
            or snapshot.offset_ms is None
        ):
            return None

        return local_time + timedelta(
            milliseconds=snapshot.offset_ms
        )

    def reset(self) -> None:
        self._samples.clear()
