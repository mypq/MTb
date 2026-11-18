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
    """Process new SessionBridge samples and persist offset transitions.

    This service records evidence only. It does not authorize an offset
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
                    self._last_decision = None

                    # Only the latest three samples are eligible after
                    # startup or file replacement. Old evidence must not
                    # be replayed against a new NTP verification.
                    start_index = max(0, len(samples) - 3)
                else:
                    start_index = self._processed_count

                self._state = "COLLECTING"

                for sample in samples[start_index:]:
                    self._process_sample(sample)

                self._processed_count = len(samples)
                self._last_sample_signature = latest_signature
                self._last_error = None

                if not samples[start_index:]:
                    if self._last_decision is not None:
                        self._state = self._last_decision.status.value
                    else:
                        self._state = "COLLECTING"

            except Exception as exc:
                # A partial batch must never be treated as fully processed.
                self._detector.reset()
                self._processed_count = 0
                self._last_sample_signature = None
                self._last_decision = None
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

    @staticmethod
    def _has_new_independent_evidence(
        decision: BrokerOffsetDecision,
        latest: BrokerOffsetRecord,
    ) -> bool:
        """Reject evidence windows that overlap the latest stored window.

        Three consecutive samples are required for every registry event.
        A later candidate must use a completely new group of samples,
        rather than reusing two samples from the previous event.
        """

        previous_samples = latest.evidence_samples

        # Legacy v1 records do not contain sample-level evidence.
        # Their exact sample window cannot be compared.
        if not previous_samples:
            return True

        candidate_samples = decision.evidence_samples

        if len(candidate_samples) != 3:
            return False

        previous_run_id = latest.run_id
        candidate_run_id = decision.run_id

        # A new EA run has a new sequence space. Its samples are
        # independent of the previous run's sequence numbers.
        if candidate_run_id != previous_run_id:
            return True

        try:
            previous_sequences = [
                item["sample_sequence"]
                for item in previous_samples
            ]
            candidate_sequences = [
                item.sample_sequence
                for item in candidate_samples
            ]
        except (KeyError, AttributeError, TypeError):
            return False

        if (
            len(previous_sequences) != 3
            or len(candidate_sequences) != 3
        ):
            return False

        if not all(
            isinstance(value, int) and not isinstance(value, bool)
            for value in previous_sequences + candidate_sequences
        ):
            return False

        # The first candidate sample must come after the final sample
        # used by the previous registry event.
        return min(candidate_sequences) > max(previous_sequences)

    def _process_sample(
        self,
        sample: MT5ClockHistorySample,
    ) -> None:
        decision = self._detector.observe(sample)
        self._last_decision = decision

        if not decision.candidate_ready:
            self._state = decision.status.value
            return

        if (
            decision.offset_seconds is None
            or decision.identity is None
            or decision.run_id is None
        ):
            self._state = "INVALID_CANDIDATE"
            return

        latest = self._registry.latest(
            identity=decision.identity
        )

        if latest is not None:
            if not self._has_new_independent_evidence(
                decision,
                latest,
            ):
                self._state = "WAITING_FOR_NEW_EVIDENCE"
                return

        self._last_record = self._registry.append_candidate(
            decision
        )

        if self._last_record.event_type == "BASELINE":
            self._state = "BASELINE_RECORDED"
        elif self._last_record.event_type == "CONFIRMATION":
            self._state = "CONFIRMATION_RECORDED"
        elif self._last_record.event_type == "OFFSET_CHANGE":
            self._state = "OFFSET_CHANGE_RECORDED"
        else:
            self._state = "CANDIDATE_RECORDED"
