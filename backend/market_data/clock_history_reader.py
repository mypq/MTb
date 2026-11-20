"""Incremental, integrity-checked reader for MT5 clock history."""

from __future__ import annotations

import csv
import hashlib
import io
import os
from collections import deque
from dataclasses import asdict, dataclass
from pathlib import Path

from .clock_history import (
    CLOCK_HISTORY_COLUMNS,
    ClockHistoryError,
    MT5ClockHistorySample,
    parse_clock_history_row,
)
from .clock_history_checkpoint import (
    ClockHistoryCheckpoint,
    ClockHistoryCheckpointError,
    ClockHistoryCheckpointStore,
)
from .clock_history_lock import ClockHistoryFileLock


class ClockHistoryFileChangedError(ClockHistoryError):
    """The active history file was replaced, truncated, or modified."""


class ClockHistoryReaderError(ClockHistoryError):
    """The incremental reader cannot safely continue."""


_ZERO_HASH = bytes(32)


def _next_hash(previous: bytes, line: bytes) -> bytes:
    return hashlib.sha256(previous + line).digest()


def _sample_from_checkpoint(
    value: dict,
) -> MT5ClockHistorySample:
    try:
        return MT5ClockHistorySample(**value)
    except (TypeError, ValueError) as exc:
        raise ClockHistoryCheckpointError(
            "Checkpoint contains an invalid previous sample."
        ) from exc


def _parse_csv_line(line: bytes, line_number: int) -> MT5ClockHistorySample:
    try:
        decoded = line.decode("utf-8")
        values = next(
            csv.reader(
                io.StringIO(decoded, newline=""),
                delimiter=";",
            )
        )
    except (UnicodeError, csv.Error, StopIteration) as exc:
        raise ClockHistoryError(
            f"Invalid CSV encoding or syntax at line {line_number}."
        ) from exc

    if len(values) != len(CLOCK_HISTORY_COLUMNS):
        raise ClockHistoryError(
            f"Invalid CSV row shape at line {line_number}."
        )

    row = dict(zip(CLOCK_HISTORY_COLUMNS, values))
    return parse_clock_history_row(row)


def _validate_next_sample(
    sample: MT5ClockHistorySample,
    previous: MT5ClockHistorySample | None,
    line_number: int,
) -> None:
    if previous is None:
        return

    if sample.sample_sequence != previous.sample_sequence + 1:
        raise ClockHistoryError(
            f"Sequence discontinuity in run "
            f"{sample.run_id!r} at line {line_number}."
        )

    if sample.identity != previous.identity:
        raise ClockHistoryError(
            f"Identity changed inside run {sample.run_id!r}."
        )

    tick_delta = (
        sample.monotonic_ms - previous.monotonic_ms
    ) & 0xFFFFFFFF

    if tick_delta == 0:
        raise ClockHistoryError(
            f"Monotonic counter did not advance in run "
            f"{sample.run_id!r}."
        )


@dataclass(frozen=True, slots=True)
class ClockHistoryReadBatch:
    """New samples and the checkpoint to persist after processing them."""

    samples: tuple[MT5ClockHistorySample, ...]
    checkpoint: ClockHistoryCheckpoint


class ClockHistoryIncrementalReader:
    """Read complete appended rows without retaining the complete history."""

    def __init__(
        self,
        history_path: str | Path,
        checkpoint_path: str | Path,
        *,
        max_samples_per_read: int = 1000,
    ) -> None:
        if (
            isinstance(max_samples_per_read, bool)
            or not isinstance(max_samples_per_read, int)
            or max_samples_per_read < 1
        ):
            raise ValueError("max_samples_per_read must be positive.")

        self.history_path = Path(history_path)
        self.file_lock = ClockHistoryFileLock(
            self.history_path.with_name(
                "clock_history.writer.lock"
            ),
            timeout_seconds=0.5,
            retry_interval_seconds=0.05,
        )
        self.store = ClockHistoryCheckpointStore(checkpoint_path)
        self.max_samples_per_read = max_samples_per_read
        self._checkpoint = self.store.load()
        self._pending: ClockHistoryReadBatch | None = None
        self._line_number = 1
        self._checkpoint_verified = False

    @staticmethod
    def _stat_identity(stat_result: os.stat_result) -> tuple[int, int]:
        return stat_result.st_dev, stat_result.st_ino

    def _verify_existing_checkpoint(
        self,
        handle,
        stat_result: os.stat_result,
    ) -> tuple[int, bytes, dict[str, MT5ClockHistorySample], int]:
        checkpoint = self._checkpoint
        assert checkpoint is not None

        if self._stat_identity(stat_result) != (
            checkpoint.file_device,
            checkpoint.file_inode,
        ):
            raise ClockHistoryFileChangedError(
                "Clock history file identity changed. "
                "Archive recovery is required before continuing."
            )

        if stat_result.st_size < checkpoint.offset:
            raise ClockHistoryFileChangedError(
                "Clock history file was truncated."
            )

        if not checkpoint.prefix_sha256:
            raise ClockHistoryCheckpointError(
                "Checkpoint has no integrity hash. "
                "A verified rebuild is required."
            )

        previous_by_run = {
            run_id: _sample_from_checkpoint(value)
            for run_id, value in checkpoint.last_by_run.items()
        }

        # The writer is append-only while holding the shared lock. Before
        # reusing the verified prefix digest, verify that the saved offset is
        # still a physical CSV record boundary. This catches rewrites whose
        # changed field lengths shift the old checkpoint into the middle of
        # a row, even when filesystem timestamp resolution is coarse.
        if self._checkpoint_verified and checkpoint.offset:
            handle.seek(checkpoint.offset - 1)
            if handle.read(1) != b"\n":
                raise ClockHistoryFileChangedError(
                    "Clock history prefix was modified before the checkpoint."
                )

        # The saved digest can be reused only while both file size and
        # modification time are unchanged. If either changes, re-verify the
        # complete checkpointed prefix before accepting any appended rows.
        # Checking only same-size rewrites is insufficient: an old row could
        # be modified and new rows appended in the same write operation.
        if (
            self._checkpoint_verified
            and stat_result.st_size == checkpoint.file_size
            and stat_result.st_mtime_ns == checkpoint.file_mtime_ns
        ):
            handle.seek(checkpoint.offset)
            try:
                digest = bytes.fromhex(checkpoint.prefix_sha256)
            except ValueError as exc:
                raise ClockHistoryCheckpointError(
                    "Checkpoint prefix integrity hash is invalid."
                ) from exc

            return (
                checkpoint.samples_processed,
                digest,
                previous_by_run,
                checkpoint.offset,
            )

        # First use, or file metadata changed: verify the complete prefix.
        handle.seek(0)
        remaining = checkpoint.offset
        digest = _ZERO_HASH

        while remaining:
            line = handle.readline(remaining)

            if not line or not line.endswith(b"\n"):
                raise ClockHistoryFileChangedError(
                    "Previously processed clock history was modified "
                    "or the checkpoint ends inside an incomplete CSV line."
                )

            digest = _next_hash(digest, line)
            remaining -= len(line)

        if digest.hex() != checkpoint.prefix_sha256:
            raise ClockHistoryFileChangedError(
                "Previously processed clock history was modified."
            )

        handle.seek(checkpoint.offset)

        return (
            checkpoint.samples_processed,
            digest,
            previous_by_run,
            checkpoint.offset,
        )

    def read_new(self) -> ClockHistoryReadBatch:
        """Read clock history while holding the shared writer lock."""
        with self.file_lock:
            return self._read_new_unlocked()

    def _read_new_unlocked(self) -> ClockHistoryReadBatch:
        """Read a batch. Call commit only after processing it successfully."""

        if self._pending is not None:
            raise ClockHistoryReaderError(
                "The previous batch must be committed before reading again."
            )

        try:
            handle = self.history_path.open("rb")
        except OSError as exc:
            raise ClockHistoryReaderError(
                "Unable to open MT5 clock history."
            ) from exc

        with handle:
            initial_stat = os.fstat(handle.fileno())

            if self._checkpoint is None:
                (
                    samples_processed,
                    digest,
                    previous_by_run,
                    offset,
                ) = self._read_initial_header(handle)
                initial_read = True
            else:
                (
                    samples_processed,
                    digest,
                    previous_by_run,
                    offset,
                ) = self._verify_existing_checkpoint(
                    handle,
                    initial_stat,
                )

                initial_read = False

            output: list[MT5ClockHistorySample] = []
            initial_tail: deque[MT5ClockHistorySample] = deque(maxlen=3)
            rows_read = 0
            line_number = self._line_number

            while (
                initial_read
                or rows_read < self.max_samples_per_read
            ):
                line_start = handle.tell()
                if line_start >= initial_stat.st_size:
                    break

                line = handle.readline(
                    initial_stat.st_size - line_start
                )

                # Never advance over an incomplete trailing row.
                if not line or not line.endswith(b"\n"):
                    break

                line_number += 1
                sample = _parse_csv_line(line, line_number)
                previous = previous_by_run.get(sample.run_id)

                _validate_next_sample(
                    sample,
                    previous,
                    line_number,
                )

                previous_by_run[sample.run_id] = sample
                samples_processed += 1
                rows_read += 1
                offset = handle.tell()
                digest = _next_hash(digest, line)

                if initial_read:
                    initial_tail.append(sample)
                else:
                    output.append(sample)

            if initial_read:
                output = list(initial_tail)

            final_stat = os.fstat(handle.fileno())

            if self._stat_identity(final_stat) != self._stat_identity(
                initial_stat
            ):
                raise ClockHistoryFileChangedError(
                    "Clock history file identity changed during reading."
                )

            if final_stat.st_size < offset:
                raise ClockHistoryFileChangedError(
                    "Clock history file was truncated during reading."
                )

            new_checkpoint = ClockHistoryCheckpoint(
                file_device=final_stat.st_dev,
                file_inode=final_stat.st_ino,
                offset=offset,
                file_size=final_stat.st_size,
                file_mtime_ns=final_stat.st_mtime_ns,
                samples_processed=samples_processed,
                last_by_run={
                    run_id: asdict(sample)
                    for run_id, sample in previous_by_run.items()
                },
                prefix_sha256=digest.hex(),
            )

            batch = ClockHistoryReadBatch(
                samples=tuple(output),
                checkpoint=new_checkpoint,
            )

            self._pending = batch
            self._pending_line_number = line_number
            return batch

    def _read_initial_header(
        self,
        handle,
    ) -> tuple[
        int,
        bytes,
        dict[str, MT5ClockHistorySample],
        int,
    ]:
        header = handle.readline()

        if not header or not header.endswith(b"\n"):
            raise ClockHistoryReaderError(
                "Clock history CSV header is missing or incomplete."
            )

        try:
            decoded = header.decode("utf-8-sig").rstrip("\r\n")
        except UnicodeError as exc:
            raise ClockHistoryError(
                "Clock history CSV header encoding is invalid."
            ) from exc

        try:
            columns = next(
                csv.reader(
                    [decoded],
                    delimiter=";",
                )
            )
        except (csv.Error, StopIteration) as exc:
            raise ClockHistoryError(
                "Clock history CSV header is invalid."
            ) from exc

        if tuple(columns) != CLOCK_HISTORY_COLUMNS:
            raise ClockHistoryError(
                "Clock history CSV header is invalid."
            )

        return 0, _next_hash(_ZERO_HASH, header), {}, handle.tell()

    def commit(self, batch: ClockHistoryReadBatch) -> None:
        """Persist progress only after the caller processed the batch."""

        if self._pending is None or batch is not self._pending:
            raise ClockHistoryReaderError(
                "This batch is not the reader's pending batch."
            )

        self.store.save(batch.checkpoint)
        self._checkpoint = batch.checkpoint
        self._checkpoint_verified = True
        self._line_number = self._pending_line_number
        self._pending = None
