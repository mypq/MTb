
"""Runtime coordinator for MT5 clock evidence, offset detection and storage."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from threading import Lock

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDecision,
    BrokerOffsetDetector,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRecord,
    BrokerOffsetRegistry,
)
from backend.market_data.clock_history import (
    ClockHistoryError,
    MT5ClockHistorySample,
    read_clock_history,
)


@dataclass(frozen=True, slots=True)
class BrokerOffsetServiceSnapshot:
    state: str
    samples_processed: int
    last_decision: BrokerOffsetDecision | None
    last_record: BrokerOffsetRecord | None
    error: str | None


class BrokerOffsetEvidenceService:
    """Process new SessionBridge samples and persist valid offset candidates.

    This service records candidates only. It does not authorize an offset
    for market-session calculations and never modifies candle timestamps.
    """

    def __init__(
        self,
        clock_history_path: str | Path,
        detector: BrokerOffsetDetector,
        registry: BrokerOffsetRegistry,
    ) -> None:
        if not isinstance(clock_history_path, (str, Path)):
            raise TypeError(
                "clock_history_path must be a string or Path."
            )

        if not isinstance(detector, BrokerOffsetDetector):
            raise TypeError(
                "detector must be a BrokerOffsetDetector."
            )

        if not isinstance(registry, BrokerOffsetRegistry):
            raise TypeError(
                "registry must be a BrokerOffsetRegistry."
            )

        self._clock_history_path = Path(clock_history_path)
        self._detector = detector
        self._registry = registry

        self._lock = Lock()
        self._processed_count = 0
        self._last_sample_signature: tuple[
            str, int, int
        ] | None = None

        self._last_decision: BrokerOffsetDecision | None = None
        self._last_record: BrokerOffsetRecord | None = None
        self._last_error: str | None = None
        self._state = "NOT_STARTED"

    def poll_once(self) -> BrokerOffsetServiceSnapshot:
        """Read history and process samples not previously processed."""

        with self._lock:
            try:
                samples = read_clock_history(
                    self._clock_history_path
                )

                latest_signature = self._signature(samples[-1])

                history_was_replaced = (
                    self._processed_count > len(samples)
                    or (
                        self._processed_count > 0
                        and len(samples) >= self._processed_count
                        and self._signature(
                            samples[self._processed_count - 1]
                        ) != self._last_sample_signature
                    )
                )

                if self._processed_count == 0 or history_was_replaced:
                    self._detector.reset()
                    self._processed_count = 0
                    self._last_sample_signature = None

                    # Do not replay old history against current NTP.
                    start_index = max(0, len(samples) - 3)
                else:
                    start_index = self._processed_count

                for sample in samples[start_index:]:
                    self._process_sample(sample)

                self._processed_count = len(samples)
                self._last_sample_signature = latest_signature
                self._last_error = None

                if self._last_record is not None:
                    self._state = "CANDIDATE_RECORDED"
                elif self._last_decision is not None:
                    self._state = self._last_decision.status.value
                else:
                    self._state = "COLLECTING"

            except Exception as exc:
                # Do not continue from a partially processed batch.
                self._detector.reset()
                self._processed_count = 0
                self._last_sample_signature = None
                self._last_error = (
                    f"{type(exc).__name__}: {exc}"
                )
                self._state = "ERROR"

            return self._snapshot_unlocked()

    def snapshot(self) -> BrokerOffsetServiceSnapshot:
        """Return the latest service state."""

        with self._lock:
            return self._snapshot_unlocked()

    def _snapshot_unlocked(self) -> BrokerOffsetServiceSnapshot:
        return BrokerOffsetServiceSnapshot(
            state=self._state,
            samples_processed=self._processed_count,
            last_decision=self._last_decision,
            last_record=self._last_record,
            error=self._last_error,
        )

    @staticmethod
    def _signature(
        sample: MT5ClockHistorySample,
    ) -> tuple[str, int, int]:
        return (
            sample.run_id,
            sample.sample_sequence,
            sample.monotonic_ms,
        )

    def _process_sample(
        self,
        sample: MT5ClockHistorySample,
    ) -> None:
        decision = self._detector.observe(sample)
        self._last_decision = decision

        if not decision.candidate_ready:
            return

        if (
            decision.offset_seconds is None
            or decision.identity is None
            or decision.run_id is None
        ):
            self._state = "INVALID_CANDIDATE"
            return

        existing_records = self._registry.load()

        duplicate = any(
            record.run_id == decision.run_id
            and record.identity == decision.identity
            and record.offset_seconds == decision.offset_seconds
            for record in existing_records
        )

        if duplicate:
            self._state = "CANDIDATE_ALREADY_RECORDED"
            return

        self._last_record = self._registry.append_candidate(
            decision
        )
        self._state = "CANDIDATE_RECORDED"
