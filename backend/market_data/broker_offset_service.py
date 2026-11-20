"""Runtime coordinator for incremental MT5 clock evidence processing."""

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
from backend.market_data.clock_history import MT5ClockHistorySample
from backend.market_data.clock_history_reader import (
    ClockHistoryIncrementalReader,
)
from backend.market_data.clock_history_lock import (
    ClockHistoryLockBusyError,
)


@dataclass(frozen=True, slots=True)
class BrokerOffsetServiceSnapshot:
    state: str
    samples_processed: int
    last_decision: BrokerOffsetDecision | None
    last_record: BrokerOffsetRecord | None
    error: str | None


class BrokerOffsetEvidenceService:
    """Process appended SessionBridge evidence and persist offset events."""

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
        self._checkpoint_path = (
            self._clock_history_path.with_suffix(
                ".checkpoint.json"
            )
        )

        self._detector = detector
        self._registry = registry
        self._reader = self._create_reader()

        self._lock = Lock()
        self._processed_count = 0
        self._last_decision: BrokerOffsetDecision | None = None
        self._last_record: BrokerOffsetRecord | None = None
        self._last_error: str | None = None
        self._state = "NOT_STARTED"

    @property
    def clock_history_path(self) -> Path:
        """Path monitored for fresh SessionBridge clock samples."""
        return self._clock_history_path

    def _create_reader(self) -> ClockHistoryIncrementalReader:
        return ClockHistoryIncrementalReader(
            self._clock_history_path,
            self._checkpoint_path,
        )

    def poll_once(self) -> BrokerOffsetServiceSnapshot:
        """Read, process, and commit one incremental evidence batch."""

        with self._lock:
            try:
                batch = self._reader.read_new()

                self._state = "COLLECTING"

                # The NTP monitor verifies asynchronously at startup and
                # periodically. Never advance the durable history checkpoint
                # while no trusted NTP result is available: doing so would
                # permanently discard the samples needed for detection.
                ntp_snapshot = self._detector.trusted_ntp_snapshot

                if batch.samples and ntp_snapshot is None:
                    return self._defer_batch_until_ntp()

                for sample in batch.samples:
                    # Do not mix evidence collected under different NTP
                    # verifications inside one durable source batch.
                    if (
                        self._detector.trusted_ntp_snapshot
                        is not ntp_snapshot
                    ):
                        return self._defer_batch_until_ntp()

                    self._process_sample(sample)

                    if (
                        self._detector.trusted_ntp_snapshot
                        is not ntp_snapshot
                    ):
                        return self._defer_batch_until_ntp()

                # Recheck immediately before committing the source position.
                if (
                    batch.samples
                    and self._detector.trusted_ntp_snapshot is not ntp_snapshot
                ):
                    return self._defer_batch_until_ntp()

                # Commit only after every sample was processed successfully.
                self._reader.commit(batch)

                self._processed_count = (
                    batch.checkpoint.samples_processed
                )
                self._last_error = None

                if not batch.samples:
                    if self._last_record is None:
                        records = self._registry.load()
                        if records:
                            self._last_record = records[-1]

                    if self._last_record is not None:
                        self._state = "WAITING_FOR_NEW_EVIDENCE"
                    else:
                        self._state = "COLLECTING"

            except ClockHistoryLockBusyError:
                # Writer contention is temporary, not evidence corruption.
                # Preserve detector state and resume from the same checkpoint.
                self._state = "WAITING_FOR_WRITER"
                self._last_error = None

            except Exception as exc:
                # Discard the uncommitted reader batch. A new reader
                # resumes from the last durable checkpoint.
                self._detector.reset()
                self._last_decision = None
                self._last_error = (
                    f"{type(exc).__name__}: {exc}"
                )
                self._state = "ERROR"

                try:
                    self._reader = self._create_reader()
                except Exception:
                    # Preserve the original processing error.
                    pass

            return self._snapshot_unlocked()

    def _defer_batch_until_ntp(self) -> BrokerOffsetServiceSnapshot:
        """Discard the in-memory read batch without advancing its checkpoint."""
        self._detector.reset()
        self._reader = self._create_reader()
        self._last_decision = None
        self._last_error = None
        self._state = "WAITING_FOR_NTP"
        return self._snapshot_unlocked()

    def snapshot(self) -> BrokerOffsetServiceSnapshot:
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
    def _has_new_independent_evidence(
        decision: BrokerOffsetDecision,
        latest: BrokerOffsetRecord,
    ) -> bool:
        """Reject candidate windows overlapping a stored evidence window."""

        previous_samples = latest.evidence_samples

        if not previous_samples:
            return True

        candidate_samples = decision.evidence_samples

        if len(candidate_samples) != 3:
            return False

        if decision.run_id != latest.run_id:
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

        # A normal non-overlapping window remains valid, including a
        # genuinely new offset-change window after the previous evidence.
        if min(candidate_sequences) > max(previous_sequences):
            return True

        if decision.offset_seconds != latest.offset_seconds:
            return False

        # A rolling window may advance by one sample only when its two
        # shared samples are byte-for-byte equivalent evidence.
        return (
            decision.run_id == latest.run_id
            and candidate_sequences == [
                previous_sequences[1],
                previous_sequences[2],
                previous_sequences[2] + 1,
            ]
            and tuple(
                BrokerOffsetEvidenceService._detector_sample_dict(item)
                for item in decision.evidence_samples[:2]
            ) == tuple(previous_samples[1:])
        )

    @staticmethod
    def _detector_sample_dict(sample) -> dict:
        """Serialize detector evidence exactly as the registry stores it."""
        return {
            "run_id": sample.run_id,
            "sample_sequence": sample.sample_sequence,
            "monotonic_ms": sample.monotonic_ms,
            "time_gmt_epoch": sample.time_gmt_epoch,
            "time_trade_server_epoch": sample.time_trade_server_epoch,
            "time_current_quote_epoch": sample.time_current_quote_epoch,
            "time_local_epoch": sample.time_local_epoch,
            "server_utc_offset_seconds": sample.server_utc_offset_seconds,
            "account_login": sample.account_login,
            "broker_server": sample.broker_server,
            "symbol": sample.symbol,
            "terminal_build": sample.terminal_build,
            "terminal_connected": sample.terminal_connected,
            "calculated_offset_seconds": sample.calculated_offset_seconds,
            "ntp_offset_seconds": sample.ntp_offset_seconds,
            "ntp_uncertainty_seconds": sample.ntp_uncertainty_seconds,
            "ntp_checked_at_unix": sample.ntp_checked_at_unix,
        }

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
