
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from statistics import median
from typing import Callable

from pydantic import BaseModel, ConfigDict, Field


class TimeSyncStatus(str, Enum):
    UNSYNCHRONIZED = "UNSYNCHRONIZED"
    SYNCHRONIZED = "SYNCHRONIZED"
    DEGRADED = "DEGRADED"


@dataclass(frozen=True)
class TimeSyncRequestStart:
    """Clock readings captured immediately before a server-time request."""

    local_sent_at: datetime
    monotonic_sent_ns: int


class TimeSyncSample(BaseModel):
    model_config = ConfigDict(frozen=True)

    local_sent_at: datetime
    local_received_at: datetime
    server_time: datetime
    monotonic_received_ns: int = Field(ge=0)
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
    Estimates server clock offset using wall-clock and monotonic readings.

    The caller must obtain server_time from a verified server-time source
    and convert it to UTC before completing a sample.

    Monotonic readings measure elapsed time and are not affected by
    Windows wall-clock adjustments.

    Wall-clock changes are detected by comparing UTC elapsed time with
    monotonic elapsed time. A detected clock jump invalidates stored
    samples and prevents synchronized status until fresh samples recover.

    The uncertainty estimate is only half the median request round trip.
    It does not prove that the server clock or local clock is accurate.
    """

    def __init__(
        self,
        *,
        max_samples: int = 9,
        max_round_trip_ms: float = 2000.0,
        max_offset_ms: float = 5000.0,
        max_sample_age_seconds: float = 30.0,
        max_clock_jump_ms: float = 250.0,
        wall_clock: Callable[[], datetime] | None = None,
        monotonic_clock_ns: Callable[[], int] | None = None,
    ) -> None:
        if isinstance(max_samples, bool) or not isinstance(max_samples, int):
            raise TypeError("max_samples must be an integer.")
        if max_samples < 3:
            raise ValueError("max_samples must be at least 3.")

        self._validate_positive_number(
            max_round_trip_ms,
            "max_round_trip_ms",
        )
        self._validate_positive_number(
            max_offset_ms,
            "max_offset_ms",
        )
        self._validate_positive_number(
            max_sample_age_seconds,
            "max_sample_age_seconds",
        )
        self._validate_positive_number(
            max_clock_jump_ms,
            "max_clock_jump_ms",
        )

        self._samples: deque[TimeSyncSample] = deque(maxlen=max_samples)
        self._max_round_trip_ms = float(max_round_trip_ms)
        self._max_offset_ms = float(max_offset_ms)
        self._max_sample_age_ns = int(
            float(max_sample_age_seconds) * 1_000_000_000
        )
        self._max_clock_jump_ms = float(max_clock_jump_ms)

        self._wall_clock = wall_clock or (
            lambda: datetime.now(timezone.utc)
        )
        self._monotonic_clock_ns = (
            monotonic_clock_ns or time.monotonic_ns
        )

        self._clock_untrusted = False

    @staticmethod
    def _validate_positive_number(value: float, name: str) -> None:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value <= 0
        ):
            raise ValueError(
                f"{name} must be a finite positive number."
            )

    @staticmethod
    def _require_utc(value: datetime, name: str) -> None:
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() is None
            or value.utcoffset() != timedelta(0)
        ):
            raise ValueError(
                f"{name} must be a UTC-aware datetime."
            )

    @staticmethod
    def _require_monotonic_ns(value: int, name: str) -> None:
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{name} must be an integer.")
        if value < 0:
            raise ValueError(f"{name} cannot be negative.")

    def begin_sample(self) -> TimeSyncRequestStart:
        """Capture local clocks immediately before requesting server time."""
        local_sent_at = self._wall_clock()
        monotonic_sent_ns = self._monotonic_clock_ns()

        self._require_utc(local_sent_at, "local_sent_at")
        self._require_monotonic_ns(
            monotonic_sent_ns,
            "monotonic_sent_ns",
        )

        return TimeSyncRequestStart(
            local_sent_at=local_sent_at,
            monotonic_sent_ns=monotonic_sent_ns,
        )

    def complete_sample(
        self,
        request_start: TimeSyncRequestStart,
        server_time: datetime,
    ) -> TimeSyncSample | None:
        """
        Complete a sample using the server response and current local clocks.

        Returns None and invalidates stored samples if a wall-clock jump
        is detected during the request.
        """
        if not isinstance(request_start, TimeSyncRequestStart):
            raise TypeError(
                "request_start must be a TimeSyncRequestStart."
            )

        self._require_utc(
            request_start.local_sent_at,
            "request_start.local_sent_at",
        )
        self._require_monotonic_ns(
            request_start.monotonic_sent_ns,
            "request_start.monotonic_sent_ns",
        )
        self._require_utc(server_time, "server_time")

        local_received_at = self._wall_clock()
        monotonic_received_ns = self._monotonic_clock_ns()

        self._require_utc(local_received_at, "local_received_at")
        self._require_monotonic_ns(
            monotonic_received_ns,
            "monotonic_received_ns",
        )

        monotonic_elapsed_ns = (
            monotonic_received_ns - request_start.monotonic_sent_ns
        )

        if monotonic_elapsed_ns < 0:
            self._invalidate_samples()
            raise ValueError(
                "Monotonic clock moved backwards."
            )

        monotonic_elapsed_ms = monotonic_elapsed_ns / 1_000_000

        wall_elapsed_ms = (
            local_received_at - request_start.local_sent_at
        ).total_seconds() * 1000.0

        clock_difference_ms = abs(
            wall_elapsed_ms - monotonic_elapsed_ms
        )

        if (
            wall_elapsed_ms < 0
            or clock_difference_ms > self._max_clock_jump_ms
        ):
            self._invalidate_samples()
            return None

        midpoint = request_start.local_sent_at + timedelta(
            milliseconds=monotonic_elapsed_ms / 2.0
        )

        estimated_offset_ms = (
            server_time - midpoint
        ).total_seconds() * 1000.0

        sample = TimeSyncSample(
            local_sent_at=request_start.local_sent_at,
            local_received_at=local_received_at,
            server_time=server_time,
            monotonic_received_ns=monotonic_received_ns,
            round_trip_ms=monotonic_elapsed_ms,
            estimated_offset_ms=estimated_offset_ms,
        )

        self._samples.append(sample)
        self._clock_untrusted = False

        return sample

    def _invalidate_samples(self) -> None:
        self._samples.clear()
        self._clock_untrusted = True

    def _detect_clock_jump(
        self,
        wall_now: datetime,
        monotonic_now_ns: int,
    ) -> bool:
        if not self._samples:
            return False

        latest = max(
            self._samples,
            key=lambda sample: sample.monotonic_received_ns,
        )

        monotonic_elapsed_ns = (
            monotonic_now_ns - latest.monotonic_received_ns
        )

        if monotonic_elapsed_ns < 0:
            return True

        wall_elapsed_ms = (
            wall_now - latest.local_received_at
        ).total_seconds() * 1000.0

        monotonic_elapsed_ms = (
            monotonic_elapsed_ns / 1_000_000
        )

        return (
            wall_elapsed_ms < 0
            or abs(
                wall_elapsed_ms - monotonic_elapsed_ms
            ) > self._max_clock_jump_ms
        )

    def snapshot(self) -> TimeSyncSnapshot:
        wall_now = self._wall_clock()
        monotonic_now_ns = self._monotonic_clock_ns()

        self._require_utc(wall_now, "wall_clock")
        self._require_monotonic_ns(
            monotonic_now_ns,
            "monotonic_clock_ns",
        )

        if self._detect_clock_jump(
            wall_now,
            monotonic_now_ns,
        ):
            self._invalidate_samples()

        if self._clock_untrusted:
            return TimeSyncSnapshot(
                status=TimeSyncStatus.DEGRADED,
                sample_count=0,
            )

        fresh_samples = [
            sample
            for sample in self._samples
            if (
                0
                <= monotonic_now_ns - sample.monotonic_received_ns
                <= self._max_sample_age_ns
            )
        ]

        usable_samples = [
            sample
            for sample in fresh_samples
            if sample.round_trip_ms <= self._max_round_trip_ms
        ]

        if not usable_samples:
            status = (
                TimeSyncStatus.DEGRADED
                if self._samples
                else TimeSyncStatus.UNSYNCHRONIZED
            )
            return TimeSyncSnapshot(
                status=status,
                sample_count=0,
            )

        offset_ms = median(
            sample.estimated_offset_ms
            for sample in usable_samples
        )

        median_rtt_ms = median(
            sample.round_trip_ms
            for sample in usable_samples
        )

        synchronized = (
            len(usable_samples) >= 3
            and abs(offset_ms) <= self._max_offset_ms
        )

        latest_sample = max(
            usable_samples,
            key=lambda sample: sample.monotonic_received_ns,
        )

        return TimeSyncSnapshot(
            status=(
                TimeSyncStatus.SYNCHRONIZED
                if synchronized
                else TimeSyncStatus.DEGRADED
            ),
            sample_count=len(usable_samples),
            offset_ms=offset_ms,
            median_round_trip_ms=median_rtt_ms,
            uncertainty_ms=median_rtt_ms / 2.0,
            measured_at=latest_sample.local_received_at,
        )

    def estimate_server_time(self) -> datetime | None:
        snapshot = self.snapshot()

        if (
            snapshot.status != TimeSyncStatus.SYNCHRONIZED
            or snapshot.offset_ms is None
        ):
            return None

        local_time = self._wall_clock()
        self._require_utc(local_time, "wall_clock")

        return local_time + timedelta(
            milliseconds=snapshot.offset_ms
        )

    def reset(self) -> None:
        self._samples.clear()
        self._clock_untrusted = False
