
"""Append-only broker offset registry with backward-compatible v1/v2/v3."""

from __future__ import annotations

import json
import math
import os
import sys
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDecision,
    BrokerOffsetStatus,
)


REGISTRY_SCHEMA_VERSION = 3
SUPPORTED_SCHEMA_VERSIONS = {1, 2, 3}
MAX_BROKER_OFFSET_SECONDS = 14 * 60 * 60


class BrokerOffsetRegistryError(ValueError):
    """Invalid broker offset registry data."""


@dataclass(frozen=True, slots=True)
class BrokerOffsetRecord:
    schema_version: int
    record_sequence: int
    recorded_at_utc: datetime
    offset_seconds: int
    uncertainty_seconds: float
    run_id: str
    account_login: int
    broker_server: str
    symbol: str
    terminal_build: int
    evidence_sample_count: int
    evidence_samples: tuple[dict[str, Any], ...] = ()
    ntp_evidence: dict[str, Any] | None = None
    evidence_start_utc: datetime | None = None
    evidence_end_utc: datetime | None = None
    event_type: str | None = None
    previous_offset_seconds: int | None = None
    transition_start_utc: datetime | None = None
    transition_end_utc: datetime | None = None

    @property
    def identity(self) -> tuple[int, str, str, int]:
        return (
            self.account_login,
            self.broker_server,
            self.symbol,
            self.terminal_build,
        )


class BrokerOffsetRegistry:
    """Append-only JSONL history with process-safe append serialization."""

    _V1_FIELDS = {
        "schema_version", "record_sequence", "recorded_at_utc",
        "offset_seconds", "uncertainty_seconds", "run_id",
        "account_login", "broker_server", "symbol", "terminal_build",
        "evidence_sample_count",
    }

    _V2_FIELDS = _V1_FIELDS | {
        "evidence_samples", "ntp_evidence",
        "evidence_start_utc", "evidence_end_utc",
    }

    _V3_FIELDS = _V2_FIELDS | {
        "event_type", "previous_offset_seconds",
        "transition_start_utc", "transition_end_utc",
    }

    def __init__(self, path: str | Path) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("path must be a string or Path.")
        self.path = Path(path)
        self._lock_path = self.path.with_name(
            self.path.name + ".lock"
        )

    @contextmanager
    def _exclusive_file_lock(self) -> Iterator[None]:
        """Serialize registry read/validate/append across processes."""
        self._lock_path.parent.mkdir(parents=True, exist_ok=True)

        with self._lock_path.open("a+b") as lock_handle:
            if os.name == "nt":
                import msvcrt

                lock_handle.seek(0, os.SEEK_END)
                if lock_handle.tell() == 0:
                    lock_handle.write(b"\0")
                    lock_handle.flush()
                lock_handle.seek(0)
                msvcrt.locking(
                    lock_handle.fileno(),
                    msvcrt.LK_LOCK,
                    1,
                )
                try:
                    yield
                finally:
                    lock_handle.seek(0)
                    msvcrt.locking(
                        lock_handle.fileno(),
                        msvcrt.LK_UNLCK,
                        1,
                    )
            else:
                import fcntl

                fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX)
                try:
                    yield
                finally:
                    fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)

    def load(self) -> tuple[BrokerOffsetRecord, ...]:
        """Read and deeply validate the complete registry."""
        return self._load_unlocked()

    def _load_unlocked(self) -> tuple[BrokerOffsetRecord, ...]:
        if not self.path.exists():
            return ()

        try:
            content = self.path.read_text(encoding="utf-8")
        except OSError as exc:
            raise BrokerOffsetRegistryError(
                "Unable to read broker offset registry."
            ) from exc

        if not content:
            return ()

        records: list[BrokerOffsetRecord] = []

        for expected, line in enumerate(content.splitlines(), start=1):
            if not line.strip():
                raise BrokerOffsetRegistryError(
                    f"Empty registry line at record {expected}."
                )

            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                raise BrokerOffsetRegistryError(
                    f"Invalid JSON at record {expected}."
                ) from exc

            record = self._parse_record(data)

            if record.record_sequence != expected:
                raise BrokerOffsetRegistryError(
                    "Registry sequence is missing, duplicated, or out of order."
                )

            if (
                records
                and record.recorded_at_utc < records[-1].recorded_at_utc
            ):
                raise BrokerOffsetRegistryError(
                    "Registry timestamps must be chronological."
                )

            previous = next(
                (
                    item for item in reversed(records)
                    if item.identity == record.identity
                ),
                None,
            )
            self._validate_event_history(record, previous)
            records.append(record)

        return tuple(records)

    def append_candidate(
        self,
        decision: BrokerOffsetDecision,
        *,
        recorded_at_utc: datetime | None = None,
    ) -> BrokerOffsetRecord:
        if not isinstance(decision, BrokerOffsetDecision):
            raise TypeError("decision must be a BrokerOffsetDecision.")

        if (
            decision.status != BrokerOffsetStatus.CANDIDATE_READY
            or decision.candidate_ready is not True
            or decision.required_samples != 3
            or decision.sample_count != 3
        ):
            raise BrokerOffsetRegistryError(
                "Only a three-sample candidate-ready decision can be stored."
            )

        if len(decision.evidence_samples) != 3:
            raise BrokerOffsetRegistryError(
                "Three complete evidence samples are required."
            )

        if not decision.ntp_verified or decision.ntp_sample_count < 3:
            raise BrokerOffsetRegistryError(
                "Verified NTP evidence with at least three samples is required."
            )

        offset = decision.offset_seconds
        uncertainty = decision.uncertainty_seconds

        self._validate_offset(offset, "Candidate offset")
        uncertainty_value = self._finite(
            uncertainty, "Candidate uncertainty"
        )
        if uncertainty_value < 0:
            raise BrokerOffsetRegistryError(
                "Candidate uncertainty cannot be negative."
            )

        if (
            not isinstance(decision.run_id, str)
            or not decision.run_id.strip()
        ):
            raise BrokerOffsetRegistryError("Candidate run_id is missing.")

        identity = decision.identity
        self._validate_identity(identity)

        if (
            decision.ntp_checked_at_unix is None
            or decision.ntp_offset_seconds is None
            or decision.ntp_uncertainty_seconds is None
            or not decision.ntp_servers
        ):
            raise BrokerOffsetRegistryError(
                "NTP evidence details are incomplete."
            )

        ntp_checked = self._finite(
            decision.ntp_checked_at_unix, "ntp_checked_at_unix"
        )
        ntp_offset = self._finite(
            decision.ntp_offset_seconds, "ntp_offset_seconds"
        )
        ntp_uncertainty = self._finite(
            decision.ntp_uncertainty_seconds,
            "ntp_uncertainty_seconds",
        )
        if ntp_uncertainty < 0:
            raise BrokerOffsetRegistryError(
                "NTP uncertainty cannot be negative."
            )

        ntp_servers = self._validate_servers(decision.ntp_servers)

        samples = tuple(
            self._sample_dict(item)
            for item in decision.evidence_samples
        )
        self._validate_samples(
            samples,
            run_id=decision.run_id,
            identity=identity,
            offset=offset,
        )

        first_epoch = (
            samples[0]["time_gmt_epoch"]
            + samples[0]["ntp_offset_seconds"]
        )
        last_epoch = (
            samples[-1]["time_gmt_epoch"]
            + samples[-1]["ntp_offset_seconds"]
        )

        try:
            start = datetime.fromtimestamp(first_epoch, timezone.utc)
            end = datetime.fromtimestamp(last_epoch, timezone.utc)
        except (OverflowError, OSError, ValueError) as exc:
            raise BrokerOffsetRegistryError(
                "Evidence timestamps are outside the supported range."
            ) from exc

        if end <= start:
            raise BrokerOffsetRegistryError(
                "Evidence timestamps must form a positive chronological interval."
            )

        ntp_evidence = {
            "verified": True,
            "sample_count": decision.ntp_sample_count,
            "servers": list(ntp_servers),
            "checked_at_unix": ntp_checked,
            "offset_seconds": ntp_offset,
            "uncertainty_seconds": ntp_uncertainty,
        }

        timestamp = self._utc_datetime(
            recorded_at_utc or datetime.now(timezone.utc),
            "recorded_at_utc",
        )

        # Lock covers the entire read/validate/sequence/event/append operation.
        try:
            with self._exclusive_file_lock():
                existing = self._load_unlocked()

                if existing and timestamp < existing[-1].recorded_at_utc:
                    raise BrokerOffsetRegistryError(
                        "New registry timestamp cannot precede the latest record."
                    )

                previous = next(
                    (
                        record
                        for record in reversed(existing)
                        if record.identity == identity
                    ),
                    None,
                )

                if previous is None:
                    event_type = "BASELINE"
                    previous_offset = None
                    transition_start = None
                    transition_end = None
                elif previous.offset_seconds == offset:
                    event_type = "CONFIRMATION"
                    previous_offset = previous.offset_seconds
                    transition_start = None
                    transition_end = None
                else:
                    event_type = "OFFSET_CHANGE"
                    previous_offset = previous.offset_seconds
                    transition_start = previous.recorded_at_utc
                    transition_end = end

                    if transition_end <= transition_start:
                        raise BrokerOffsetRegistryError(
                            "New evidence does not establish a chronological "
                            "transition interval after the previous record."
                        )

                    if timestamp < transition_end:
                        raise BrokerOffsetRegistryError(
                            "Record time cannot precede the transition evidence end."
                        )

                record = BrokerOffsetRecord(
                    schema_version=3,
                    record_sequence=len(existing) + 1,
                    recorded_at_utc=timestamp,
                    offset_seconds=offset,
                    uncertainty_seconds=uncertainty_value,
                    run_id=decision.run_id,
                    account_login=identity[0],
                    broker_server=identity[1],
                    symbol=identity[2],
                    terminal_build=identity[3],
                    evidence_sample_count=3,
                    evidence_samples=samples,
                    ntp_evidence=ntp_evidence,
                    evidence_start_utc=start,
                    evidence_end_utc=end,
                    event_type=event_type,
                    previous_offset_seconds=previous_offset,
                    transition_start_utc=transition_start,
                    transition_end_utc=transition_end,
                )

                payload = self._serialize_record(record)
                self._append_payload_locked(payload)
                return record

        except BrokerOffsetRegistryError:
            raise
        except OSError as exc:
            raise BrokerOffsetRegistryError(
                "Unable to append broker offset registry record."
            ) from exc

    def _append_payload_locked(self, payload: bytes) -> None:
        """Append all bytes or restore the original file length on failure."""
        self.path.parent.mkdir(parents=True, exist_ok=True)

        descriptor = os.open(
            self.path,
            os.O_RDWR | os.O_CREAT | os.O_APPEND,
            0o600,
        )
        original_size = os.fstat(descriptor).st_size

        try:
            view = memoryview(payload)
            total = 0
            while total < len(view):
                written = os.write(descriptor, view[total:])
                if written <= 0:
                    raise OSError("Registry append made no progress.")
                total += written
            os.fsync(descriptor)
        except OSError:
            try:
                os.ftruncate(descriptor, original_size)
                os.fsync(descriptor)
            except OSError:
                # Preserve the original write failure. A subsequent load
                # will reject any incomplete trailing JSON record.
                pass
            raise
        finally:
            os.close(descriptor)

    def latest(
        self,
        *,
        identity: tuple[int, str, str, int] | None = None,
    ) -> BrokerOffsetRecord | None:
        records = self.load()

        if identity is None:
            return records[-1] if records else None

        self._validate_identity(identity)
        return next(
            (
                record
                for record in reversed(records)
                if record.identity == identity
            ),
            None,
        )

    @staticmethod
    def _validate_identity(
        identity: object,
    ) -> tuple[int, str, str, int]:
        if not isinstance(identity, tuple) or len(identity) != 4:
            raise BrokerOffsetRegistryError(
                "Candidate identity must be a four-item tuple."
            )

        account, server, symbol, build = identity
        if (
            isinstance(account, bool)
            or not isinstance(account, int)
            or account <= 0
            or not isinstance(server, str)
            or not server.strip()
            or not isinstance(symbol, str)
            or not symbol.strip()
            or isinstance(build, bool)
            or not isinstance(build, int)
            or build <= 0
        ):
            raise BrokerOffsetRegistryError(
                "Candidate identity fields are invalid."
            )
        return identity

    @staticmethod
    def _validate_offset(value: object, name: str) -> int:
        if (
            isinstance(value, bool)
            or not isinstance(value, int)
            or abs(value) > MAX_BROKER_OFFSET_SECONDS
        ):
            raise BrokerOffsetRegistryError(f"{name} is invalid.")
        return value

    @staticmethod
    def _validate_servers(value: object) -> tuple[str, ...]:
        if not isinstance(value, (tuple, list)) or len(value) < 3:
            raise BrokerOffsetRegistryError(
                "At least three NTP servers are required."
            )

        if not all(
            isinstance(server, str) and server.strip()
            for server in value
        ):
            raise BrokerOffsetRegistryError(
                "NTP server names must be non-empty strings."
            )

        normalized = tuple(server.strip().lower() for server in value)
        if len(set(normalized)) != len(normalized):
            raise BrokerOffsetRegistryError(
                "NTP server names must be distinct."
            )
        return tuple(server.strip() for server in value)

    @staticmethod
    def _utc_datetime(value: datetime, name: str) -> datetime:
        if (
            not isinstance(value, datetime)
            or value.tzinfo is None
            or value.utcoffset() != timedelta(0)
        ):
            raise BrokerOffsetRegistryError(
                f"{name} must be timezone-aware UTC."
            )
        return value.astimezone(timezone.utc)

    @staticmethod
    def _finite(value: object, name: str) -> float:
        if (
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
        ):
            raise BrokerOffsetRegistryError(
                f"{name} must be finite."
            )
        return float(value)

    @staticmethod
    def _sample_dict(sample: object) -> dict[str, Any]:
        from dataclasses import asdict

        try:
            data = asdict(sample)
        except (TypeError, ValueError) as exc:
            raise BrokerOffsetRegistryError(
                "Invalid evidence sample object."
            ) from exc

        if not isinstance(data, dict):
            raise BrokerOffsetRegistryError(
                "Evidence sample is invalid."
            )
        return data

    @classmethod
    def _validate_samples(
        cls,
        samples: tuple[dict[str, Any], ...],
        *,
        run_id: str,
        identity: tuple[int, str, str, int],
        offset: int,
    ) -> None:
        required = {
            "run_id", "sample_sequence", "monotonic_ms",
            "time_gmt_epoch", "time_trade_server_epoch",
            "time_current_quote_epoch", "time_local_epoch",
            "server_utc_offset_seconds", "account_login",
            "broker_server", "symbol", "terminal_build",
            "terminal_connected", "calculated_offset_seconds",
            "ntp_offset_seconds", "ntp_uncertainty_seconds",
            "ntp_checked_at_unix",
        }

        if len(samples) != 3:
            raise BrokerOffsetRegistryError(
                "Exactly three evidence samples are required."
            )

        sequences: list[int] = []
        evidence_epochs: list[float] = []

        for sample in samples:
            if set(sample) != required:
                raise BrokerOffsetRegistryError(
                    "Evidence sample has missing or unexpected fields."
                )

            if sample["run_id"] != run_id:
                raise BrokerOffsetRegistryError(
                    "Evidence run IDs do not match."
                )

            sample_identity = (
                sample["account_login"],
                sample["broker_server"],
                sample["symbol"],
                sample["terminal_build"],
            )
            if sample_identity != identity:
                raise BrokerOffsetRegistryError(
                    "Evidence identity mismatch."
                )

            if sample["terminal_connected"] is not True:
                raise BrokerOffsetRegistryError(
                    "Disconnected evidence is invalid."
                )

            for name in (
                "sample_sequence", "monotonic_ms",
                "time_gmt_epoch", "time_trade_server_epoch",
                "time_current_quote_epoch", "time_local_epoch",
                "server_utc_offset_seconds", "account_login",
                "terminal_build",
            ):
                value = sample[name]
                if isinstance(value, bool) or not isinstance(value, int):
                    raise BrokerOffsetRegistryError(
                        f"Evidence {name} must be an integer."
                    )

            if (
                sample["sample_sequence"] <= 0
                or not 0 <= sample["monotonic_ms"] <= 0xFFFFFFFF
                or min(
                    sample["time_gmt_epoch"],
                    sample["time_trade_server_epoch"],
                    sample["time_local_epoch"],
                    sample["account_login"],
                    sample["terminal_build"],
                ) <= 0
                or sample["time_current_quote_epoch"] < 0
            ):
                raise BrokerOffsetRegistryError(
                    "Evidence sample contains an invalid integer value."
                )

            measured = (
                sample["time_trade_server_epoch"]
                - sample["time_gmt_epoch"]
            )
            if measured != sample["server_utc_offset_seconds"]:
                raise BrokerOffsetRegistryError(
                    "Evidence raw clock fields do not match server offset."
                )

            if sample["server_utc_offset_seconds"] != offset:
                raise BrokerOffsetRegistryError(
                    "Evidence server offset does not match candidate."
                )

            calculated = cls._finite(
                sample["calculated_offset_seconds"],
                "calculated_offset_seconds",
            )
            ntp_offset = cls._finite(
                sample["ntp_offset_seconds"], "ntp_offset_seconds"
            )
            ntp_uncertainty = cls._finite(
                sample["ntp_uncertainty_seconds"],
                "ntp_uncertainty_seconds",
            )
            ntp_checked = cls._finite(
                sample["ntp_checked_at_unix"],
                "ntp_checked_at_unix",
            )

            if ntp_uncertainty < 0 or ntp_checked <= 0:
                raise BrokerOffsetRegistryError(
                    "Evidence NTP values are invalid."
                )

            if abs(calculated - offset) > 1.5:
                raise BrokerOffsetRegistryError(
                    "Evidence calculated offset does not match candidate."
                )

            sequence = sample["sample_sequence"]
            sequences.append(sequence)
            evidence_epochs.append(
                sample["time_gmt_epoch"] + ntp_offset
            )

        if sequences != list(range(sequences[0], sequences[0] + 3)):
            raise BrokerOffsetRegistryError(
                "Evidence sequences are not consecutive."
            )

        if not (
            evidence_epochs[0] < evidence_epochs[1] < evidence_epochs[2]
        ):
            raise BrokerOffsetRegistryError(
                "Evidence timestamps are not strictly chronological."
            )

    @classmethod
    def _validate_ntp_evidence(
        cls,
        ntp: object,
    ) -> dict[str, Any]:
        required = {
            "verified", "sample_count", "servers",
            "checked_at_unix", "offset_seconds",
            "uncertainty_seconds",
        }
        if not isinstance(ntp, dict) or set(ntp) != required:
            raise BrokerOffsetRegistryError(
                "Stored NTP evidence fields are invalid."
            )

        if ntp["verified"] is not True:
            raise BrokerOffsetRegistryError(
                "Stored NTP evidence is not verified."
            )

        count = ntp["sample_count"]
        if (
            isinstance(count, bool)
            or not isinstance(count, int)
            or count < 3
        ):
            raise BrokerOffsetRegistryError(
                "Stored NTP sample count is invalid."
            )

        servers = cls._validate_servers(ntp["servers"])
        checked = cls._finite(
            ntp["checked_at_unix"], "NTP checked_at_unix"
        )
        offset = cls._finite(
            ntp["offset_seconds"], "NTP offset_seconds"
        )
        uncertainty = cls._finite(
            ntp["uncertainty_seconds"], "NTP uncertainty_seconds"
        )
        if checked <= 0 or uncertainty < 0:
            raise BrokerOffsetRegistryError(
                "Stored NTP evidence values are invalid."
            )

        return {
            "verified": True,
            "sample_count": count,
            "servers": list(servers),
            "checked_at_unix": checked,
            "offset_seconds": offset,
            "uncertainty_seconds": uncertainty,
        }

    @classmethod
    def _validate_event_history(
        cls,
        record: BrokerOffsetRecord,
        previous: BrokerOffsetRecord | None,
    ) -> None:
        # v1/v2 records have no event metadata. They remain readable;
        # the next v3 event is checked against their stored offset.
        if record.schema_version != 3:
            return

        if previous is None:
            expected = "BASELINE"
        elif previous.offset_seconds == record.offset_seconds:
            expected = "CONFIRMATION"
        else:
            expected = "OFFSET_CHANGE"

        if record.event_type != expected:
            raise BrokerOffsetRegistryError(
                "Stored event type conflicts with the previous identity record."
            )

        if expected == "BASELINE":
            if (
                record.previous_offset_seconds is not None
                or record.transition_start_utc is not None
                or record.transition_end_utc is not None
            ):
                raise BrokerOffsetRegistryError(
                    "Baseline event contains transition metadata."
                )
            return

        if expected == "CONFIRMATION":
            if (
                record.previous_offset_seconds != previous.offset_seconds
                or record.transition_start_utc is not None
                or record.transition_end_utc is not None
            ):
                raise BrokerOffsetRegistryError(
                    "Confirmation event metadata is inconsistent."
                )
            return

        if (
            record.previous_offset_seconds != previous.offset_seconds
            or record.transition_start_utc != previous.recorded_at_utc
            or record.transition_end_utc != record.evidence_end_utc
            or record.transition_end_utc <= record.transition_start_utc
            or record.recorded_at_utc < record.transition_end_utc
        ):
            raise BrokerOffsetRegistryError(
                "Offset-change transition metadata is inconsistent."
            )

    @classmethod
    def _serialize_record(cls, record: BrokerOffsetRecord) -> bytes:
        data = {
            "schema_version": 3,
            "record_sequence": record.record_sequence,
            "recorded_at_utc": record.recorded_at_utc.isoformat(),
            "offset_seconds": record.offset_seconds,
            "uncertainty_seconds": record.uncertainty_seconds,
            "run_id": record.run_id,
            "account_login": record.account_login,
            "broker_server": record.broker_server,
            "symbol": record.symbol,
            "terminal_build": record.terminal_build,
            "evidence_sample_count": record.evidence_sample_count,
            "evidence_samples": list(record.evidence_samples),
            "ntp_evidence": record.ntp_evidence,
            "evidence_start_utc": record.evidence_start_utc.isoformat(),
            "evidence_end_utc": record.evidence_end_utc.isoformat(),
            "event_type": record.event_type,
            "previous_offset_seconds": record.previous_offset_seconds,
            "transition_start_utc": (
                record.transition_start_utc.isoformat()
                if record.transition_start_utc is not None else None
            ),
            "transition_end_utc": (
                record.transition_end_utc.isoformat()
                if record.transition_end_utc is not None else None
            ),
        }

        return (
            json.dumps(
                data,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ) + "\n"
        ).encode("utf-8")

    @classmethod
    def _parse_record(cls, data: object) -> BrokerOffsetRecord:
        if not isinstance(data, dict):
            raise BrokerOffsetRegistryError(
                "Registry record must be an object."
            )

        version = data.get("schema_version")
        expected_fields = {
            1: cls._V1_FIELDS,
            2: cls._V2_FIELDS,
            3: cls._V3_FIELDS,
        }.get(version)

        if expected_fields is None or set(data) != expected_fields:
            raise BrokerOffsetRegistryError(
                "Registry record has unsupported schema or invalid fields."
            )

        for name in (
            "schema_version", "record_sequence", "offset_seconds",
            "account_login", "terminal_build", "evidence_sample_count",
        ):
            value = data[name]
            if isinstance(value, bool) or not isinstance(value, int):
                raise BrokerOffsetRegistryError(
                    f"{name} must be an integer."
                )

        if data["schema_version"] not in SUPPORTED_SCHEMA_VERSIONS:
            raise BrokerOffsetRegistryError("Unsupported schema version.")
        if data["record_sequence"] <= 0:
            raise BrokerOffsetRegistryError(
                "record_sequence must be positive."
            )
        cls._validate_offset(data["offset_seconds"], "Stored offset")

        if data["evidence_sample_count"] != 3:
            raise BrokerOffsetRegistryError(
                "Stored evidence count must equal three."
            )

        uncertainty = cls._finite(
            data["uncertainty_seconds"], "uncertainty_seconds"
        )
        if uncertainty < 0:
            raise BrokerOffsetRegistryError(
                "Stored uncertainty cannot be negative."
            )

        for name in ("run_id", "broker_server", "symbol"):
            if (
                not isinstance(data[name], str)
                or not data[name].strip()
            ):
                raise BrokerOffsetRegistryError(f"{name} is invalid.")

        if data["account_login"] <= 0 or data["terminal_build"] <= 0:
            raise BrokerOffsetRegistryError(
                "Stored identity is invalid."
            )

        recorded = cls._parse_timestamp(
            data["recorded_at_utc"], "recorded_at_utc"
        )

        if version == 1:
            return BrokerOffsetRecord(
                schema_version=1,
                record_sequence=data["record_sequence"],
                recorded_at_utc=recorded,
                offset_seconds=data["offset_seconds"],
                uncertainty_seconds=uncertainty,
                run_id=data["run_id"],
                account_login=data["account_login"],
                broker_server=data["broker_server"],
                symbol=data["symbol"],
                terminal_build=data["terminal_build"],
                evidence_sample_count=3,
            )

        samples_raw = data["evidence_samples"]
        if (
            not isinstance(samples_raw, list)
            or len(samples_raw) != 3
            or not all(isinstance(item, dict) for item in samples_raw)
        ):
            raise BrokerOffsetRegistryError(
                "Stored evidence samples are invalid."
            )

        identity = (
            data["account_login"],
            data["broker_server"],
            data["symbol"],
            data["terminal_build"],
        )
        cls._validate_samples(
            tuple(samples_raw),
            run_id=data["run_id"],
            identity=identity,
            offset=data["offset_seconds"],
        )

        ntp = cls._validate_ntp_evidence(data["ntp_evidence"])

        start = cls._parse_timestamp(
            data["evidence_start_utc"], "evidence_start_utc"
        )
        end = cls._parse_timestamp(
            data["evidence_end_utc"], "evidence_end_utc"
        )

        expected_start = (
            samples_raw[0]["time_gmt_epoch"]
            + samples_raw[0]["ntp_offset_seconds"]
        )
        expected_end = (
            samples_raw[-1]["time_gmt_epoch"]
            + samples_raw[-1]["ntp_offset_seconds"]
        )
        if (
            end <= start
            or abs(start.timestamp() - expected_start) > 0.001
            or abs(end.timestamp() - expected_end) > 0.001
        ):
            raise BrokerOffsetRegistryError(
                "Stored evidence time range conflicts with its samples."
            )

        common = dict(
            schema_version=version,
            record_sequence=data["record_sequence"],
            recorded_at_utc=recorded,
            offset_seconds=data["offset_seconds"],
            uncertainty_seconds=uncertainty,
            run_id=data["run_id"],
            account_login=data["account_login"],
            broker_server=data["broker_server"],
            symbol=data["symbol"],
            terminal_build=data["terminal_build"],
            evidence_sample_count=3,
            evidence_samples=tuple(samples_raw),
            ntp_evidence=ntp,
            evidence_start_utc=start,
            evidence_end_utc=end,
        )

        if version == 2:
            return BrokerOffsetRecord(**common)

        event_type = data["event_type"]
        previous_offset = data["previous_offset_seconds"]

        if event_type not in {
            "BASELINE", "CONFIRMATION", "OFFSET_CHANGE"
        }:
            raise BrokerOffsetRegistryError(
                "Stored event_type is invalid."
            )

        if previous_offset is not None:
            cls._validate_offset(
                previous_offset, "Stored previous offset"
            )

        transition_start_value = data["transition_start_utc"]
        transition_end_value = data["transition_end_utc"]

        if event_type == "BASELINE":
            if (
                previous_offset is not None
                or transition_start_value is not None
                or transition_end_value is not None
            ):
                raise BrokerOffsetRegistryError(
                    "Baseline event cannot contain transition metadata."
                )
        elif event_type == "CONFIRMATION":
            if (
                previous_offset != data["offset_seconds"]
                or transition_start_value is not None
                or transition_end_value is not None
            ):
                raise BrokerOffsetRegistryError(
                    "Confirmation event fields are inconsistent."
                )
        elif (
            previous_offset is None
            or previous_offset == data["offset_seconds"]
            or transition_start_value is None
            or transition_end_value is None
        ):
            raise BrokerOffsetRegistryError(
                "Offset-change event fields are incomplete."
            )

        transition_start = (
            cls._parse_timestamp(
                transition_start_value, "transition_start_utc"
            )
            if transition_start_value is not None else None
        )
        transition_end = (
            cls._parse_timestamp(
                transition_end_value, "transition_end_utc"
            )
            if transition_end_value is not None else None
        )

        if event_type == "OFFSET_CHANGE" and (
            transition_start is None
            or transition_end is None
            or transition_end <= transition_start
            or transition_end > end
            or recorded < transition_end
        ):
            raise BrokerOffsetRegistryError(
                "Stored transition interval is invalid."
            )

        return BrokerOffsetRecord(
            **common,
            event_type=event_type,
            previous_offset_seconds=previous_offset,
            transition_start_utc=transition_start,
            transition_end_utc=transition_end,
        )

    @staticmethod
    def _parse_timestamp(value: object, name: str) -> datetime:
        if not isinstance(value, str):
            raise BrokerOffsetRegistryError(
                f"{name} must be an ISO timestamp."
            )
        try:
            result = datetime.fromisoformat(value)
        except ValueError as exc:
            raise BrokerOffsetRegistryError(
                f"{name} is invalid."
            ) from exc

        if result.tzinfo is None or result.utcoffset() != timedelta(0):
            raise BrokerOffsetRegistryError(
                f"{name} must be UTC."
            )
        return result.astimezone(timezone.utc)
