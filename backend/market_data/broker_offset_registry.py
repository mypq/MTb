
"""Append-only persistent registry for NTP-supported broker UTC offsets."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDecision,
    BrokerOffsetStatus,
)


REGISTRY_SCHEMA_VERSION = 1


class BrokerOffsetRegistryError(ValueError):
    """Raised when broker offset registry data is invalid."""


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

    @property
    def identity(self) -> tuple[int, str, str, int]:
        return (
            self.account_login,
            self.broker_server,
            self.symbol,
            self.terminal_build,
        )


class BrokerOffsetRegistry:
    """Stores accepted broker offsets as immutable JSON Lines records.

    Every accepted candidate is appended as a new record. Existing records
    are never rewritten or silently replaced.

    A candidate is accepted only when the detector reports exactly three
    consecutive samples from one run and one terminal/account identity.
    """

    _FIELDS = {
        "schema_version",
        "record_sequence",
        "recorded_at_utc",
        "offset_seconds",
        "uncertainty_seconds",
        "run_id",
        "account_login",
        "broker_server",
        "symbol",
        "terminal_build",
        "evidence_sample_count",
    }

    def __init__(self, path: str | Path) -> None:
        if not isinstance(path, (str, Path)):
            raise TypeError("path must be a string or Path.")

        self.path = Path(path)

    def load(self) -> tuple[BrokerOffsetRecord, ...]:
        """Read and validate the complete append-only registry."""

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

        lines = content.splitlines()

        if not lines:
            return ()

        records: list[BrokerOffsetRecord] = []

        for expected_sequence, line in enumerate(lines, start=1):
            if not line.strip():
                raise BrokerOffsetRegistryError(
                    f"Registry contains an empty line at record "
                    f"{expected_sequence}."
                )

            try:
                data = json.loads(line)
            except json.JSONDecodeError as exc:
                raise BrokerOffsetRegistryError(
                    f"Registry record {expected_sequence} is invalid JSON."
                ) from exc

            record = self._parse_record(data)

            if record.record_sequence != expected_sequence:
                raise BrokerOffsetRegistryError(
                    "Registry record sequence is missing, duplicated, "
                    "or out of order."
                )

            if records:
                previous = records[-1]

                if record.recorded_at_utc < previous.recorded_at_utc:
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
        """Validate and persist one detector-approved offset candidate."""

        if not isinstance(decision, BrokerOffsetDecision):
            raise TypeError(
                "decision must be a BrokerOffsetDecision."
            )

        if (
            decision.status != BrokerOffsetStatus.CANDIDATE_READY
            or decision.candidate_ready is not True
        ):
            raise BrokerOffsetRegistryError(
                "Only a candidate-ready detector decision can be stored."
            )

        if (
            decision.required_samples != 3
            or decision.sample_count != 3
        ):
            raise BrokerOffsetRegistryError(
                "Exactly three consecutive evidence samples are required."
            )

        offset = decision.offset_seconds

        if (
            isinstance(offset, bool)
            or not isinstance(offset, int)
            or abs(offset) > 14 * 60 * 60
        ):
            raise BrokerOffsetRegistryError(
                "Candidate offset is missing or outside the supported range."
            )

        uncertainty = decision.uncertainty_seconds

        if (
            isinstance(uncertainty, bool)
            or not isinstance(uncertainty, (int, float))
            or not math.isfinite(uncertainty)
            or uncertainty < 0
        ):
            raise BrokerOffsetRegistryError(
                "Candidate uncertainty must be finite and non-negative."
            )

        if (
            not isinstance(decision.run_id, str)
            or not decision.run_id.strip()
        ):
            raise BrokerOffsetRegistryError(
                "Candidate run_id is missing."
            )

        identity = decision.identity

        if (
            not isinstance(identity, tuple)
            or len(identity) != 4
        ):
            raise BrokerOffsetRegistryError(
                "Candidate terminal identity is invalid."
            )

        account_login, broker_server, symbol, terminal_build = identity

        if (
            isinstance(account_login, bool)
            or not isinstance(account_login, int)
            or account_login <= 0
        ):
            raise BrokerOffsetRegistryError(
                "Candidate account login is invalid."
            )

        if (
            not isinstance(broker_server, str)
            or not broker_server.strip()
            or not isinstance(symbol, str)
            or not symbol.strip()
        ):
            raise BrokerOffsetRegistryError(
                "Candidate broker server or symbol is invalid."
            )

        if (
            isinstance(terminal_build, bool)
            or not isinstance(terminal_build, int)
            or terminal_build <= 0
        ):
            raise BrokerOffsetRegistryError(
                "Candidate terminal build is invalid."
            )

        timestamp = recorded_at_utc or datetime.now(timezone.utc)

        if (
            not isinstance(timestamp, datetime)
            or timestamp.tzinfo is None
            or timestamp.utcoffset() != timedelta(0)
        ):
            raise BrokerOffsetRegistryError(
                "recorded_at_utc must be timezone-aware UTC."
            )

        timestamp = timestamp.astimezone(timezone.utc)

        # Validate the complete existing history before appending.
        existing = self.load()

        if existing and timestamp < existing[-1].recorded_at_utc:
            raise BrokerOffsetRegistryError(
                "New registry timestamp cannot precede the latest record."
            )

        record = BrokerOffsetRecord(
            schema_version=REGISTRY_SCHEMA_VERSION,
            record_sequence=len(existing) + 1,
            recorded_at_utc=timestamp,
            offset_seconds=offset,
            uncertainty_seconds=float(uncertainty),
            run_id=decision.run_id,
            account_login=account_login,
            broker_server=broker_server,
            symbol=symbol,
            terminal_build=terminal_build,
            evidence_sample_count=3,
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
                    raise OSError(
                        "Registry append did not write the complete record."
                    )

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
        """Return the latest record, optionally for one exact identity."""

        records = self.load()

        if identity is None:
            return records[-1] if records else None

        if (
            not isinstance(identity, tuple)
            or len(identity) != 4
        ):
            raise TypeError(
                "identity must be a four-item identity tuple."
            )

        for record in reversed(records):
            if record.identity == identity:
                return record

        return None

    @classmethod
    def _serialize_record(
        cls,
        record: BrokerOffsetRecord,
    ) -> bytes:
        data = {
            "schema_version": record.schema_version,
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
        }

        return (
            json.dumps(
                data,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")

    @classmethod
    def _parse_record(cls, data: object) -> BrokerOffsetRecord:
        if not isinstance(data, dict) or set(data) != cls._FIELDS:
            raise BrokerOffsetRegistryError(
                "Registry record has missing or unexpected fields."
            )

        version = data["schema_version"]
        sequence = data["record_sequence"]
        offset = data["offset_seconds"]
        uncertainty = data["uncertainty_seconds"]
        evidence_count = data["evidence_sample_count"]

        for name, value in (
            ("schema_version", version),
            ("record_sequence", sequence),
            ("offset_seconds", offset),
            ("account_login", data["account_login"]),
            ("terminal_build", data["terminal_build"]),
            ("evidence_sample_count", evidence_count),
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise BrokerOffsetRegistryError(
                    f"{name} must be an integer."
                )

        if version != REGISTRY_SCHEMA_VERSION:
            raise BrokerOffsetRegistryError(
                "Unsupported broker offset registry schema version."
            )

        if sequence <= 0:
            raise BrokerOffsetRegistryError(
                "record_sequence must be positive."
            )

        if abs(offset) > 14 * 60 * 60:
            raise BrokerOffsetRegistryError(
                "Stored broker offset is outside the supported range."
            )

        if (
            isinstance(uncertainty, bool)
            or not isinstance(uncertainty, (int, float))
            or not math.isfinite(uncertainty)
            or uncertainty < 0
        ):
            raise BrokerOffsetRegistryError(
                "Stored uncertainty is invalid."
            )

        if evidence_count != 3:
            raise BrokerOffsetRegistryError(
                "Stored record does not contain exactly three evidence samples."
            )

        run_id = data["run_id"]
        broker_server = data["broker_server"]
        symbol = data["symbol"]

        if (
            not isinstance(run_id, str)
            or not run_id.strip()
            or not isinstance(broker_server, str)
            or not broker_server.strip()
            or not isinstance(symbol, str)
            or not symbol.strip()
        ):
            raise BrokerOffsetRegistryError(
                "Stored run or terminal identity is invalid."
            )

        account_login = data["account_login"]
        terminal_build = data["terminal_build"]

        if account_login <= 0 or terminal_build <= 0:
            raise BrokerOffsetRegistryError(
                "Stored account login or terminal build is invalid."
            )

        timestamp_text = data["recorded_at_utc"]

        if not isinstance(timestamp_text, str):
            raise BrokerOffsetRegistryError(
                "recorded_at_utc must be an ISO timestamp."
            )

        try:
            timestamp = datetime.fromisoformat(timestamp_text)
        except ValueError as exc:
            raise BrokerOffsetRegistryError(
                "recorded_at_utc is not a valid ISO timestamp."
            ) from exc

        if (
            timestamp.tzinfo is None
            or timestamp.utcoffset() != timedelta(0)
        ):
            raise BrokerOffsetRegistryError(
                "Stored timestamp must be timezone-aware UTC."
            )

        return BrokerOffsetRecord(
            schema_version=version,
            record_sequence=sequence,
            recorded_at_utc=timestamp.astimezone(timezone.utc),
            offset_seconds=offset,
            uncertainty_seconds=float(uncertainty),
            run_id=run_id,
            account_login=account_login,
            broker_server=broker_server,
            symbol=symbol,
            terminal_build=terminal_build,
            evidence_sample_count=evidence_count,
        )
