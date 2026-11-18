
"""Append-only broker offset registry with backward-compatible schema v1/v2."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDecision,
    BrokerOffsetStatus,
)


REGISTRY_SCHEMA_VERSION = 2
SUPPORTED_SCHEMA_VERSIONS = {1, 2}


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

    @property
    def identity(self) -> tuple[int, str, str, int]:
        return (
            self.account_login,
            self.broker_server,
            self.symbol,
            self.terminal_build,
        )


class BrokerOffsetRegistry:
    """Immutable JSONL history. Existing records are never rewritten."""

    _V1_FIELDS = {
        "schema_version", "record_sequence", "recorded_at_utc",
        "offset_seconds", "uncertainty_seconds", "run_id",
        "account_login", "broker_server", "symbol", "terminal_build",
        "evidence_sample_count",
    }

    _V2_FIELDS = _V1_FIELDS | {
        "evidence_samples",
        "ntp_evidence",
        "evidence_start_utc",
        "evidence_end_utc",
    }

    def __init__(self, path: str | Path) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("path must be a string or Path.")
        self.path = Path(path)

    def load(self) -> tuple[BrokerOffsetRecord, ...]:
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

            if records and record.recorded_at_utc < records[-1].recorded_at_utc:
                raise BrokerOffsetRegistryError(
                    "Registry timestamps must be chronological."
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
                "Three complete evidence samples are required for schema v2."
            )

        if not decision.ntp_verified or decision.ntp_sample_count < 3:
            raise BrokerOffsetRegistryError(
                "Verified NTP evidence with at least three samples is required."
            )

        if (
            decision.ntp_checked_at_unix is None
            or decision.ntp_offset_seconds is None
            or decision.ntp_uncertainty_seconds is None
            or not decision.ntp_servers
        ):
            raise BrokerOffsetRegistryError(
                "NTP evidence details are incomplete."
            )

        offset = decision.offset_seconds
        uncertainty = decision.uncertainty_seconds

        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or abs(offset) > 14 * 60 * 60
        ):
            raise BrokerOffsetRegistryError("Candidate offset is invalid.")

        if (
            isinstance(uncertainty, bool)
            or not isinstance(uncertainty, (int, float))
            or not math.isfinite(uncertainty)
            or uncertainty < 0
        ):
            raise BrokerOffsetRegistryError("Candidate uncertainty is invalid.")

        if not isinstance(decision.run_id, str) or not decision.run_id.strip():
            raise BrokerOffsetRegistryError("Candidate run_id is missing.")

        identity = decision.identity
        if not isinstance(identity, tuple) or len(identity) != 4:
            raise BrokerOffsetRegistryError("Candidate identity is invalid.")

        account, server, symbol, build = identity
        if (
            isinstance(account, bool) or not isinstance(account, int) or account <= 0
            or not isinstance(server, str) or not server.strip()
            or not isinstance(symbol, str) or not symbol.strip()
            or isinstance(build, bool) or not isinstance(build, int) or build <= 0
        ):
            raise BrokerOffsetRegistryError("Candidate identity fields are invalid.")

        timestamp = recorded_at_utc or datetime.now(timezone.utc)
        timestamp = self._utc_datetime(timestamp, "recorded_at_utc")

        samples = tuple(self._sample_dict(item) for item in decision.evidence_samples)
        self._validate_samples(samples, decision, identity, offset)

        first_epoch = samples[0]["time_gmt_epoch"] + samples[0]["ntp_offset_seconds"]
        last_epoch = samples[-1]["time_gmt_epoch"] + samples[-1]["ntp_offset_seconds"]

        start = datetime.fromtimestamp(first_epoch, timezone.utc)
        end = datetime.fromtimestamp(last_epoch, timezone.utc)

        if end < start:
            raise BrokerOffsetRegistryError("Evidence timestamps are not chronological.")

        ntp_evidence = {
            "verified": True,
            "sample_count": decision.ntp_sample_count,
            "servers": list(decision.ntp_servers),
            "checked_at_unix": self._finite(
                decision.ntp_checked_at_unix, "ntp_checked_at_unix"
            ),
            "offset_seconds": self._finite(
                decision.ntp_offset_seconds, "ntp_offset_seconds"
            ),
            "uncertainty_seconds": self._finite(
                decision.ntp_uncertainty_seconds, "ntp_uncertainty_seconds"
            ),
        }

        if ntp_evidence["uncertainty_seconds"] < 0:
            raise BrokerOffsetRegistryError("NTP uncertainty cannot be negative.")

        existing = self.load()
        if existing and timestamp < existing[-1].recorded_at_utc:
            raise BrokerOffsetRegistryError(
                "New registry timestamp cannot precede the latest record."
            )

        record = BrokerOffsetRecord(
            schema_version=2,
            record_sequence=len(existing) + 1,
            recorded_at_utc=timestamp,
            offset_seconds=offset,
            uncertainty_seconds=float(uncertainty),
            run_id=decision.run_id,
            account_login=account,
            broker_server=server,
            symbol=symbol,
            terminal_build=build,
            evidence_sample_count=3,
            evidence_samples=samples,
            ntp_evidence=ntp_evidence,
            evidence_start_utc=start,
            evidence_end_utc=end,
        )

        payload = self._serialize_record(record)

        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            descriptor = os.open(
                self.path,
                os.O_WRONLY | os.O_CREAT | os.O_APPEND,
                0o600,
            )
            try:
                written = os.write(descriptor, payload)
                if written != len(payload):
                    raise OSError("Incomplete registry append.")
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except OSError as exc:
            raise BrokerOffsetRegistryError(
                "Unable to append broker offset registry record."
            ) from exc

        return record

    def latest(
        self,
        *,
        identity: tuple[int, str, str, int] | None = None,
    ) -> BrokerOffsetRecord | None:
        records = self.load()
        if identity is None:
            return records[-1] if records else None
        if not isinstance(identity, tuple) or len(identity) != 4:
            raise TypeError("identity must be a four-item tuple.")
        return next(
            (record for record in reversed(records) if record.identity == identity),
            None,
        )

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
            raise BrokerOffsetRegistryError(f"{name} must be finite.")
        return float(value)

    @staticmethod
    def _sample_dict(sample: object) -> dict[str, Any]:
        try:
            from dataclasses import asdict
            data = asdict(sample)
        except (TypeError, ValueError) as exc:
            raise BrokerOffsetRegistryError(
                "Invalid evidence sample object."
            ) from exc

        if not isinstance(data, dict):
            raise BrokerOffsetRegistryError("Evidence sample is invalid.")
        return data

    @staticmethod
    def _validate_samples(
        samples: tuple[dict[str, Any], ...],
        decision: BrokerOffsetDecision,
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

        sequences = []
        for sample in samples:
            if set(sample) != required:
                raise BrokerOffsetRegistryError(
                    "Evidence sample has missing or unexpected fields."
                )
            if sample["run_id"] != decision.run_id:
                raise BrokerOffsetRegistryError("Evidence run IDs do not match.")
            if (
                sample["account_login"],
                sample["broker_server"],
                sample["symbol"],
                sample["terminal_build"],
            ) != identity:
                raise BrokerOffsetRegistryError("Evidence identity mismatch.")
            if sample["terminal_connected"] is not True:
                raise BrokerOffsetRegistryError("Disconnected evidence is invalid.")
            for key in (
                "calculated_offset_seconds",
                "ntp_offset_seconds",
                "ntp_uncertainty_seconds",
                "ntp_checked_at_unix",
            ):
                BrokerOffsetRegistry._finite(sample[key], key)
            if abs(sample["calculated_offset_seconds"] - offset) > 1.5:
                raise BrokerOffsetRegistryError("Evidence offset does not match candidate.")
            sequences.append(sample["sample_sequence"])

        if sequences != list(range(sequences[0], sequences[0] + 3)):
            raise BrokerOffsetRegistryError("Evidence sequences are not consecutive.")

    @classmethod
    def _serialize_record(cls, record: BrokerOffsetRecord) -> bytes:
        data = {
            "schema_version": 2,
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
        }
        return (
            json.dumps(
                data, sort_keys=True, separators=(",", ":"), allow_nan=False
            ) + "\n"
        ).encode("utf-8")

    @classmethod
    def _parse_record(cls, data: object) -> BrokerOffsetRecord:
        if not isinstance(data, dict):
            raise BrokerOffsetRegistryError("Registry record must be an object.")

        version = data.get("schema_version")
        expected_fields = cls._V1_FIELDS if version == 1 else cls._V2_FIELDS

        if version not in SUPPORTED_SCHEMA_VERSIONS or set(data) != expected_fields:
            raise BrokerOffsetRegistryError(
                "Registry record has unsupported schema or invalid fields."
            )

        integer_fields = (
            "schema_version", "record_sequence", "offset_seconds",
            "account_login", "terminal_build", "evidence_sample_count",
        )
        for name in integer_fields:
            value = data[name]
            if isinstance(value, bool) or not isinstance(value, int):
                raise BrokerOffsetRegistryError(f"{name} must be an integer.")

        if data["record_sequence"] <= 0:
            raise BrokerOffsetRegistryError("record_sequence must be positive.")
        if abs(data["offset_seconds"]) > 14 * 60 * 60:
            raise BrokerOffsetRegistryError("Stored offset is outside supported range.")
        if data["evidence_sample_count"] != 3:
            raise BrokerOffsetRegistryError("Stored evidence count must equal three.")

        uncertainty = cls._finite(data["uncertainty_seconds"], "uncertainty_seconds")
        if uncertainty < 0:
            raise BrokerOffsetRegistryError("Stored uncertainty cannot be negative.")

        for name in ("run_id", "broker_server", "symbol"):
            if not isinstance(data[name], str) or not data[name].strip():
                raise BrokerOffsetRegistryError(f"{name} is invalid.")

        if data["account_login"] <= 0 or data["terminal_build"] <= 0:
            raise BrokerOffsetRegistryError("Stored identity is invalid.")

        recorded = cls._parse_timestamp(data["recorded_at_utc"], "recorded_at_utc")

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

        samples = data["evidence_samples"]
        ntp = data["ntp_evidence"]
        if (
            not isinstance(samples, list) or len(samples) != 3
            or not all(isinstance(item, dict) for item in samples)
            or not isinstance(ntp, dict)
            or ntp.get("verified") is not True
            or not isinstance(ntp.get("servers"), list)
            or ntp.get("sample_count", 0) < 3
        ):
            raise BrokerOffsetRegistryError("Stored v2 evidence is invalid.")

        start = cls._parse_timestamp(data["evidence_start_utc"], "evidence_start_utc")
        end = cls._parse_timestamp(data["evidence_end_utc"], "evidence_end_utc")
        if end < start:
            raise BrokerOffsetRegistryError("Evidence time range is invalid.")

        return BrokerOffsetRecord(
            schema_version=2,
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
            evidence_samples=tuple(samples),
            ntp_evidence=ntp,
            evidence_start_utc=start,
            evidence_end_utc=end,
        )

    @staticmethod
    def _parse_timestamp(value: object, name: str) -> datetime:
        if not isinstance(value, str):
            raise BrokerOffsetRegistryError(f"{name} must be an ISO timestamp.")
        try:
            result = datetime.fromisoformat(value)
        except ValueError as exc:
            raise BrokerOffsetRegistryError(f"{name} is invalid.") from exc
        if result.tzinfo is None or result.utcoffset() != timedelta(0):
            raise BrokerOffsetRegistryError(f"{name} must be UTC.")
        return result.astimezone(timezone.utc)
