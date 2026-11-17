
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDecision,
    BrokerOffsetStatus,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
    BrokerOffsetRegistryError,
)


UTC = timezone.utc
BASE_TIME = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)

IDENTITY = (
    337260380,
    "XMGlobal-MT5 9",
    "GOLD",
    6316,
)


def make_decision(
    *,
    offset: int = 7200,
    run_id: str = "run-001",
    identity: tuple[int, str, str, int] = IDENTITY,
    status: BrokerOffsetStatus = BrokerOffsetStatus.CANDIDATE_READY,
    candidate_ready: bool = True,
    sample_count: int = 3,
    required_samples: int = 3,
    uncertainty: float = 1.1,
) -> BrokerOffsetDecision:
    return BrokerOffsetDecision(
        status=status,
        candidate_ready=candidate_ready,
        offset_seconds=offset,
        uncertainty_seconds=uncertainty,
        sample_count=sample_count,
        required_samples=required_samples,
        run_id=run_id,
        identity=identity,
        reason="Test candidate.",
    )


def test_append_and_load_candidate(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    loaded = registry.load()

    assert len(loaded) == 1
    assert loaded[0] == record
    assert record.schema_version == 1
    assert record.record_sequence == 1
    assert record.offset_seconds == 7200
    assert record.uncertainty_seconds == 1.1
    assert record.run_id == "run-001"
    assert record.identity == IDENTITY
    assert record.evidence_sample_count == 3
    assert record.recorded_at_utc == BASE_TIME


def test_missing_registry_returns_empty_history(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "missing.jsonl")

    assert registry.load() == ()
    assert registry.latest() is None


def test_new_candidate_appends_without_replacing_old_record(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(offset=7200, run_id="run-001"),
        recorded_at_utc=BASE_TIME,
    )

    second = registry.append_candidate(
        make_decision(offset=10800, run_id="run-002"),
        recorded_at_utc=BASE_TIME + timedelta(minutes=5),
    )

    records = registry.load()

    assert len(records) == 2
    assert records[0] == first
    assert records[1] == second
    assert records[0].offset_seconds == 7200
    assert records[1].offset_seconds == 10800
    assert records[0].record_sequence == 1
    assert records[1].record_sequence == 2


def test_latest_returns_most_recent_record(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    registry.append_candidate(
        make_decision(offset=7200),
        recorded_at_utc=BASE_TIME,
    )

    second = registry.append_candidate(
        make_decision(offset=10800, run_id="run-002"),
        recorded_at_utc=BASE_TIME + timedelta(minutes=1),
    )

    assert registry.latest() == second


def test_latest_can_filter_by_exact_identity(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    other_identity = (
        123456789,
        "Other-Broker",
        "EURUSD",
        6000,
    )

    registry.append_candidate(
        make_decision(identity=IDENTITY),
        recorded_at_utc=BASE_TIME,
    )

    other = registry.append_candidate(
        make_decision(
            offset=3600,
            run_id="run-other",
            identity=other_identity,
        ),
        recorded_at_utc=BASE_TIME + timedelta(minutes=1),
    )

    assert registry.latest(identity=other_identity) == other
    assert registry.latest(identity=IDENTITY).offset_seconds == 7200
    assert registry.latest(identity=(1, "unknown", "X", 1)) is None


@pytest.mark.parametrize(
    "decision",
    [
        make_decision(
            status=BrokerOffsetStatus.COLLECTING,
            candidate_ready=False,
            sample_count=2,
        ),
        make_decision(candidate_ready=False),
        make_decision(sample_count=2),
        make_decision(required_samples=2),
        make_decision(offset=None),  # type: ignore[arg-type]
        make_decision(uncertainty=float("nan")),
        make_decision(uncertainty=float("inf")),
        make_decision(uncertainty=-1.0),
        make_decision(run_id=""),
        make_decision(identity=(0, "Broker", "GOLD", 6316)),
        make_decision(identity=(337260380, "", "GOLD", 6316)),
        make_decision(identity=(337260380, "Broker", "", 6316)),
        make_decision(identity=(337260380, "Broker", "GOLD", 0)),
        make_decision(offset=14 * 60 * 60 + 1),
    ],
)
def test_invalid_candidate_is_rejected(tmp_path, decision) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(
            decision,
            recorded_at_utc=BASE_TIME,
        )

    assert registry.load() == ()


@pytest.mark.parametrize(
    "timestamp",
    [
        datetime(2026, 11, 18, 12, 0),
        datetime(
            2026, 11, 18, 12, 0,
            tzinfo=timezone(timedelta(hours=2)),
        ),
    ],
)
def test_non_utc_or_naive_timestamp_is_rejected(
    tmp_path,
    timestamp,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(
            make_decision(),
            recorded_at_utc=timestamp,
        )

    assert registry.load() == ()


def test_timestamp_cannot_move_backwards(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(
            make_decision(run_id="run-002"),
            recorded_at_utc=BASE_TIME - timedelta(seconds=1),
        )

    assert len(registry.load()) == 1


def test_empty_line_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    path.write_text("\n", encoding="utf-8")

    registry = BrokerOffsetRegistry(path)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_invalid_json_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    path.write_text("{invalid json}\n", encoding="utf-8")

    registry = BrokerOffsetRegistry(path)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_missing_or_unexpected_fields_are_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    path.write_text(
        json.dumps({"schema_version": 1}) + "\n",
        encoding="utf-8",
    )

    registry = BrokerOffsetRegistry(path)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_unsupported_schema_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    data = {
        "schema_version": 2,
        "record_sequence": 1,
        "recorded_at_utc": BASE_TIME.isoformat(),
        "offset_seconds": 7200,
        "uncertainty_seconds": 1.1,
        "run_id": "run-001",
        "account_login": IDENTITY[0],
        "broker_server": IDENTITY[1],
        "symbol": IDENTITY[2],
        "terminal_build": IDENTITY[3],
        "evidence_sample_count": 3,
    }
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")

    registry = BrokerOffsetRegistry(path)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_record_sequence_gap_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    data = json.loads(
        path.read_text(encoding="utf-8").splitlines()[0]
    )
    data["record_sequence"] = 3

    path.write_text(
        json.dumps(data, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_record_timestamp_must_be_utc(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    data = json.loads(
        path.read_text(encoding="utf-8").splitlines()[0]
    )
    data["recorded_at_utc"] = datetime(
        2026, 11, 18, 14, 0,
        tzinfo=timezone(timedelta(hours=2)),
    ).isoformat()

    path.write_text(json.dumps(data) + "\n", encoding="utf-8")

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_non_chronological_registry_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    registry.append_candidate(
        make_decision(run_id="run-002", offset=10800),
        recorded_at_utc=BASE_TIME + timedelta(minutes=2),
    )

    lines = path.read_text(encoding="utf-8").splitlines()
    second = json.loads(lines[1])
    second["recorded_at_utc"] = (
        BASE_TIME - timedelta(minutes=1)
    ).isoformat()
    lines[1] = json.dumps(second)

    path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()
