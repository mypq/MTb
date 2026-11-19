"""Append-only broker offset registry with backward-compatible v1/v2/v3."""

from __future__ import annotations

import json
import math
import os
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

                # Lock before initializing the file. Two Windows workers
                # opening a new empty lock file must not both write its first byte.
                lock_handle.seek(0)
                msvcrt.locking(
                    lock_handle.fileno(),
                    msvcrt.LK_LOCK,
                    1,
                )
                try:
                    lock_handle.seek(0, os.SEEK_END)
                    if lock_handle.tell() == 0:
                        lock_handle.write(bytes([0]))
                        lock_handle.flush()
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
        """Read and deeply validate the complete registry under a shared lock."""
        try:
            with self._exclusive_file_lock():
                return self._load_unlocked()
        except BrokerOffsetRegistryError:
            raise
        except OSError as exc:
            raise BrokerOffsetRegistryError(
                "Unable to read broker offset registry."
            ) from exc

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
        latest_evidence_end_by_identity: dict[
            tuple[int, str, str, int],
            datetime,
        ] = {}

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

            latest_evidence_end = (
                latest_evidence_end_by_identity.get(record.identity)
            )

            self._validate_event_history(
                record,
                previous,
                latest_evidence_end,
            )

            if record.evidence_end_utc is not None:
                latest_evidence_end_by_identity[record.identity] = max(
                    latest_evidence_end or record.evidence_end_utc,
                    record.evidence_end_utc,
                )

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

        self._validate_ntp_sample_consistency(
            ntp_evidence,
            samples,
        )

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

                identity_records = [
                    record
                    for record in existing
                    if record.identity == identity
                ]

                previous = identity_records[-1] if identity_records else None

                previous_evidence_ends = [
                    record.evidence_end_utc
                    for record in identity_records
                    if record.evidence_end_utc is not None
                ]

                latest_evidence_end = (
                    max(previous_evidence_ends)
                    if previous_evidence_ends
                    else None
                )

                if (
                    latest_evidence_end is not None
                    and start <= latest_evidence_end
                ):
                    raise BrokerOffsetRegistryError(
                        "New evidence overlaps or precedes previously stored evidence."
                    )

                if (
                    previous is not None
                    and previous.evidence_end_utc is not None
                    and latest_evidence_end != previous.evidence_end_utc
                ):
                    raise BrokerOffsetRegistryError(
                        "Previous registry evidence windows are not chronological."
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

                    if previous.evidence_end_utc is None:
                        # Legacy v1 has no bounded evidence interval.
                        # Do not invent transition boundaries from its record time.
                        transition_start = None
                        transition_end = None
                    else:
                        transition_start = previous.evidence_end_utc
                        transition_end = start

                        if transition_end <= transition_start:
                            raise BrokerOffsetRegistryError(
                                "New evidence overlaps or precedes the previous "
                                "evidence interval; the transition is unresolved."
                            )

                    if timestamp < end:
                        raise BrokerOffsetRegistryError(
                            "Record time cannot precede the new evidence end."
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
            os.O_RDWR | os.O_CREAT | os.O_APPEND | getattr(os, "O_BINARY", 0),
            0o600,
        )
        original_size = os.fstat(descriptor).st_size

        try:
            # Older valid JSONL files may not end with a newline.
            # Separate the next record without modifying existing evidence.
            separator = b""

            if original_size > 0:
                os.lseek(descriptor, original_size - 1, os.SEEK_SET)
                last_byte = os.read(descriptor, 1)

                if last_byte != b"\n":
                    separator = b"\n"

            complete_payload = separator + payload
            view = memoryview(complete_payload)
            total = 0

            while total < len(view):
                written = os.write(descriptor, view[total:])
                if written <= 0:
                    raise OSError("Registry append made no progress.")
                total += written

            os.fsync(descriptor)

        except OSError as write_error:
            try:
                os.ftruncate(descriptor, original_size)
                os.fsync(descriptor)

            except OSError as rollback_error:
                raise BrokerOffsetRegistryError(
                    "Registry append failed and rollback also failed. "
                    "Registry integrity is uncertain; manual recovery is required."
                ) from rollback_error

            raise write_error

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

    def for_time(
        self,
        identity: tuple[int, str, str, int],
        timestamp_utc: datetime,
    ) -> BrokerOffsetRecord | None:
        """Return the offset record whose evidence covers a UTC instant.

        v1 records and times outside explicit evidence bounds are not proof.
        """
        self._validate_identity(identity)
        instant = self._utc_datetime(timestamp_utc, "timestamp_utc")

        matches = [
            record
            for record in self.load()
            if (
                record.identity == identity
                and record.evidence_start_utc is not None
                and record.evidence_end_utc is not None
                and record.evidence_start_utc <= instant <= record.evidence_end_utc
            )
        ]

        if not matches:
            return None

        if len({record.offset_seconds for record in matches}) > 1:
            raise BrokerOffsetRegistryError(
                "Conflicting offset evidence covers the requested UTC time."
            )

        return max(
            matches,
            key=lambda record: (
                record.evidence_end_utc,
                record.record_sequence,
            ),
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
        allow_legacy_calculation: bool = False,
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

            raw_calculated = float(measured)

            legacy_calculated = (
                float(measured) - ntp_offset
            )

            matches_raw = (
                abs(calculated - raw_calculated) <= 0.001
            )

            matches_legacy = (
                abs(calculated - legacy_calculated) <= 0.001
            )

            if not matches_raw and not (
                allow_legacy_calculation and matches_legacy
            ):
                raise BrokerOffsetRegistryError(
                    "Evidence calculated offset conflicts with raw clocks and NTP."
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
    def _validate_ntp_sample_consistency(
        cls,
        ntp: dict[str, Any],
        samples: tuple[dict[str, Any], ...],
    ) -> None:
        """Ensure the NTP summary matches the final evidence sample."""

        if not samples:
            raise BrokerOffsetRegistryError(
                "NTP evidence samples are missing."
            )

        if ntp["sample_count"] != len(ntp["servers"]):
            raise BrokerOffsetRegistryError(
                "NTP sample count does not match the server count."
            )

        latest = samples[-1]

        matching_fields = (
            ("checked_at_unix", "ntp_checked_at_unix"),
            ("offset_seconds", "ntp_offset_seconds"),
            ("uncertainty_seconds", "ntp_uncertainty_seconds"),
        )

        for summary_field, sample_field in matching_fields:
            summary_value = cls._finite(
                ntp[summary_field],
                f"NTP {summary_field}",
            )
            sample_value = cls._finite(
                latest[sample_field],
                f"Evidence {sample_field}",
            )

            if summary_value != sample_value:
                raise BrokerOffsetRegistryError(
                    "NTP summary does not match the final evidence sample."
                )

    @classmethod
    def _validate_event_history(
        cls,
        record: BrokerOffsetRecord,
        previous: BrokerOffsetRecord | None,
        latest_evidence_end_utc: datetime | None = None,
    ) -> None:
        # Evidence windows must never overlap any earlier evidence
        # for the same identity, even when legacy v1 records intervene.
        if (
            latest_evidence_end_utc is not None
            and record.evidence_start_utc is not None
            and record.evidence_start_utc <= latest_evidence_end_utc
        ):
            raise BrokerOffsetRegistryError(
                "Stored evidence overlaps or precedes the previous identity evidence."
            )

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

        if record.previous_offset_seconds != previous.offset_seconds:
            raise BrokerOffsetRegistryError(
                "Offset-change previous offset is inconsistent."
            )

        if previous.evidence_end_utc is None:
            if (
                record.transition_start_utc is not None
                or record.transition_end_utc is not None
            ):
                raise BrokerOffsetRegistryError(
                    "A legacy record without evidence bounds cannot define "
                    "transition boundaries."
                )
            return

        # Retain readability of previously written v3 events.
        # Historical lookup never relies on these legacy transition fields.
        legacy_transition = (
            record.transition_start_utc == previous.recorded_at_utc
            and record.transition_end_utc == record.evidence_end_utc
        )

        evidence_transition = (
            record.transition_start_utc == previous.evidence_end_utc
            and record.transition_end_utc == record.evidence_start_utc
        )

        if not (legacy_transition or evidence_transition):
            raise BrokerOffsetRegistryError(
                "Offset-change transition metadata does not match evidence bounds."
            )

        if (
            record.transition_start_utc is None
            or record.transition_end_utc is None
            or record.transition_end_utc <= record.transition_start_utc
            or record.recorded_at_utc < record.evidence_end_utc
        ):
            raise BrokerOffsetRegistryError(
                "Offset-change transition interval is invalid."
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
            allow_legacy_calculation=True,
        )

        ntp = cls._validate_ntp_evidence(data["ntp_evidence"])

        cls._validate_ntp_sample_consistency(
            ntp,
            tuple(samples_raw),
        )

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
            or (
                (transition_start_value is None)
                != (transition_end_value is None)
            )
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
            (
                transition_start is not None
                and transition_end is not None
                and (
                    transition_end <= transition_start
                    or recorded < end
                )
            )
            or (
                transition_start is None
                and transition_end is not None
            )
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
