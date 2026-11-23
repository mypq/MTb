"""Durable checkpoint for incremental MT5 clock-history reading."""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


CHECKPOINT_VERSION = 1


class ClockHistoryCheckpointError(ValueError):
    """Invalid or unreadable clock-history checkpoint."""


@dataclass(frozen=True, slots=True)
class ClockHistoryCheckpoint:
    file_device: int
    file_inode: int
    offset: int
    file_size: int
    file_mtime_ns: int
    samples_processed: int
    last_by_run: dict[str, dict[str, Any]]
    prefix_sha256: str = ""

    def __post_init__(self) -> None:
        numeric_fields = (
            self.file_device,
            self.file_inode,
            self.offset,
            self.file_size,
            self.file_mtime_ns,
            self.samples_processed,
        )
        if any(
            isinstance(value, bool) or not isinstance(value, int)
            for value in numeric_fields
        ):
            raise ClockHistoryCheckpointError(
                "Checkpoint numeric fields must be integers."
            )

        if min(numeric_fields) < 0:
            raise ClockHistoryCheckpointError(
                "Checkpoint numeric fields cannot be negative."
            )

        if self.offset > self.file_size:
            raise ClockHistoryCheckpointError(
                "Checkpoint offset exceeds the recorded file size."
            )

        if not isinstance(self.last_by_run, dict):
            raise ClockHistoryCheckpointError(
                "Checkpoint last_by_run must be an object."
            )

        if any(
            not isinstance(run_id, str)
            or not isinstance(sample, dict)
            for run_id, sample in self.last_by_run.items()
        ):
            raise ClockHistoryCheckpointError(
                "Checkpoint last_by_run entries must be objects keyed by run ID."
            )

        if not isinstance(self.prefix_sha256, str):
            raise ClockHistoryCheckpointError(
                "Checkpoint prefix SHA-256 must be a string."
            )

        if self.prefix_sha256 and (
            len(self.prefix_sha256) != 64
            or any(
                character not in "0123456789abcdefABCDEF"
                for character in self.prefix_sha256
            )
        ):
            raise ClockHistoryCheckpointError(
                "Checkpoint prefix SHA-256 must contain exactly 64 hexadecimal characters."
            )


class ClockHistoryCheckpointStore:
    """Atomically persist the reader position and per-run validation state."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def load(self) -> ClockHistoryCheckpoint | None:
        if not self.path.exists():
            return None

        try:
            payload = json.loads(
                self.path.read_text(encoding="utf-8")
            )
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ClockHistoryCheckpointError(
                "Unable to read clock-history checkpoint."
            ) from exc

        if not isinstance(payload, dict):
            raise ClockHistoryCheckpointError(
                "Checkpoint root must be an object."
            )

        if (
            isinstance(payload.get("version"), bool)
            or payload.get("version") != CHECKPOINT_VERSION
        ):
            raise ClockHistoryCheckpointError(
                "Unsupported checkpoint version."
            )

        raw = payload.get("checkpoint")

        if not isinstance(raw, dict):
            raise ClockHistoryCheckpointError(
                "Checkpoint data is missing."
            )

        try:
            return ClockHistoryCheckpoint(
                file_device=raw["file_device"],
                file_inode=raw["file_inode"],
                offset=raw["offset"],
                file_size=raw["file_size"],
                file_mtime_ns=raw["file_mtime_ns"],
                samples_processed=raw["samples_processed"],
                last_by_run=raw["last_by_run"],
                prefix_sha256=raw.get("prefix_sha256", ""),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ClockHistoryCheckpointError(
                "Checkpoint fields are invalid."
            ) from exc

    def save(self, checkpoint: ClockHistoryCheckpoint) -> None:
        if not isinstance(checkpoint, ClockHistoryCheckpoint):
            raise TypeError(
                "checkpoint must be a ClockHistoryCheckpoint."
            )

        self.path.parent.mkdir(parents=True, exist_ok=True)

        payload = {
            "version": CHECKPOINT_VERSION,
            "checkpoint": asdict(checkpoint),
        }

        encoded = (
            json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")

        temporary_path: Path | None = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                prefix=f".{self.path.name}.",
                suffix=".tmp",
                dir=self.path.parent,
                delete=False,
            ) as handle:
                temporary_path = Path(handle.name)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())

            os.replace(temporary_path, self.path)
            temporary_path = None

            # Persist the directory entry where supported.
            if hasattr(os, "O_DIRECTORY"):
                directory_fd = os.open(
                    self.path.parent,
                    os.O_RDONLY | os.O_DIRECTORY,
                )
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)

        except OSError as exc:
            raise ClockHistoryCheckpointError(
                "Unable to persist clock-history checkpoint."
            ) from exc

        finally:
            if temporary_path is not None:
                try:
                    temporary_path.unlink(missing_ok=True)
                except OSError:
                    pass
