from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDecision,
    BrokerOffsetDetector,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
    BrokerOffsetRegistryError,
)
from backend.market_data.clock_history import MT5ClockHistorySample
from backend.market_data.ntp_time import NtpSample, NtpTimeVerifier


UTC = timezone.utc
BASE_TIME = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
BASE_EPOCH = int(BASE_TIME.timestamp())

IDENTITY = (337260380, "XMGlobal-MT5 9", "GOLD", 6316)


def make_verifier(
    ntp_offset_seconds: float = 0.0,
) -> NtpTimeVerifier:
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        wall_clock=lambda: float(BASE_EPOCH),
        monotonic_clock=lambda: 100.0,
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=ntp_offset_seconds,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )
    assert verifier.verify().verified
    return verifier


def make_sample(sequence: int) -> MT5ClockHistorySample:
    gmt = BASE_EPOCH + (sequence - 1) * 60
    return MT5ClockHistorySample(
        schema_version=1,
        run_id="run-001",
        sample_sequence=sequence,
        monotonic_ms=1000 + (sequence - 1) * 60000,
        time_gmt_epoch=gmt,
        time_trade_server_epoch=gmt + 7200,
        time_current_quote_epoch=gmt + 7200,
        time_local_epoch=gmt,
        server_utc_offset_seconds=7200,
        account_login=IDENTITY[0],
        broker_server=IDENTITY[1],
        symbol=IDENTITY[2],
        terminal_build=IDENTITY[3],
        terminal_connected=True,
    )


def make_decision(
    ntp_offset_seconds: float = 0.0,
) -> BrokerOffsetDecision:
    detector = BrokerOffsetDetector(
        make_verifier(ntp_offset_seconds)
    )
    detector.observe(make_sample(1))
    detector.observe(make_sample(2))
    result = detector.observe(make_sample(3))
    assert result.candidate_ready
    return result


def decision_with_run_id(
    decision: BrokerOffsetDecision,
    run_id: str,
) -> BrokerOffsetDecision:
    return replace(
        decision,
        run_id=run_id,
        evidence_samples=tuple(
            replace(sample, run_id=run_id)
            for sample in decision.evidence_samples
        ),
    )


def make_v1_record(sequence: int = 1) -> dict:
    return {
        "schema_version": 1,
        "record_sequence": sequence,
        "recorded_at_utc": BASE_TIME.isoformat(),
        "offset_seconds": 7200,
        "uncertainty_seconds": 1.1,
        "run_id": "legacy-run",
        "account_login": IDENTITY[0],
        "broker_server": IDENTITY[1],
        "symbol": IDENTITY[2],
        "terminal_build": IDENTITY[3],
        "evidence_sample_count": 3,
    }


def read_json_lines(path):
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
    ]


def write_json_lines(path, records):
    path.write_text(
        "".join(
            json.dumps(
                item,
                sort_keys=True,
                separators=(",", ":"),
            ) + "\n"
            for item in records
        ),
        encoding="utf-8",
    )


def test_append_and_load_v2_candidate(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    loaded = registry.load()

    assert len(loaded) == 1
    assert loaded[0] == record
    assert record.schema_version == 3
    assert record.event_type == "BASELINE"
    assert record.previous_offset_seconds is None
    assert record.transition_start_utc is None
    assert record.transition_end_utc is None
    assert record.record_sequence == 1
    assert record.offset_seconds == 7200
    assert record.evidence_sample_count == 3
    assert len(record.evidence_samples) == 3
    assert record.ntp_evidence["verified"] is True
    assert record.ntp_evidence["sample_count"] >= 3
    assert record.evidence_start_utc is not None
    assert record.evidence_end_utc is not None
    assert record.evidence_end_utc > record.evidence_start_utc


def test_registry_accepts_nonzero_ntp_offset_correction(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(
        tmp_path / "offsets.jsonl"
    )

    decision = make_decision(
        ntp_offset_seconds=0.25
    )

    record = registry.append_candidate(
        decision,
        recorded_at_utc=BASE_TIME,
    )

    assert record.offset_seconds == 7200
    assert record.ntp_evidence["offset_seconds"] == 0.25

    assert len(record.evidence_samples) == 3

    for sample in record.evidence_samples:
        assert sample["ntp_offset_seconds"] == 0.25
        assert sample["calculated_offset_seconds"] == 7199.75

    loaded = registry.load()

    assert len(loaded) == 1
    assert loaded[0] == record


def test_v1_history_remains_readable_and_unchanged(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    original = json.dumps(
        make_v1_record(),
        sort_keys=True,
        separators=(",", ":"),
    )
    path.write_text(original + "\n", encoding="utf-8")

    registry = BrokerOffsetRegistry(path)
    records = registry.load()

    assert len(records) == 1
    assert records[0].schema_version == 1
    assert records[0].offset_seconds == 7200
    assert path.read_text(encoding="utf-8").strip() == original


def test_v1_and_v3_records_can_coexist(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    legacy = make_v1_record()
    legacy["recorded_at_utc"] = (
        BASE_TIME - timedelta(minutes=5)
    ).isoformat()
    path.write_text(
        json.dumps(legacy, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )

    registry = BrokerOffsetRegistry(path)
    new_record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = registry.load()
    assert len(records) == 2
    assert records[0].schema_version == 1
    assert records[1].schema_version == 3
    assert records[1].event_type == "CONFIRMATION"
    assert records[1] == new_record


def test_new_candidate_appends_without_replacing_old_record(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )
    second_decision = decision_with_run_id(
        make_decision(),
        "run-002",
    )
    second = registry.append_candidate(
        second_decision,
        recorded_at_utc=BASE_TIME + timedelta(minutes=1),
    )

    records = registry.load()
    assert len(records) == 2
    assert records[0] == first
    assert records[1] == second
    assert records[0].record_sequence == 1
    assert records[1].record_sequence == 2
    assert records[1].run_id == "run-002"
    assert all(
        sample["run_id"] == "run-002"
        for sample in records[1].evidence_samples
    )


def test_latest_returns_latest_record(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )
    second = registry.append_candidate(
        decision_with_run_id(make_decision(), "run-002"),
        recorded_at_utc=BASE_TIME + timedelta(minutes=1),
    )

    assert registry.latest() == second
    assert registry.latest(identity=IDENTITY) == second
    assert first.record_sequence == 1


def test_missing_registry_returns_empty_history(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "missing.jsonl")
    assert registry.load() == ()
    assert registry.latest() is None


def test_candidate_without_evidence_is_rejected(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    decision = replace(make_decision(), evidence_samples=())

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(decision, recorded_at_utc=BASE_TIME)


def test_candidate_without_ntp_proof_is_rejected(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    decision = replace(make_decision(), ntp_verified=False)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(decision, recorded_at_utc=BASE_TIME)


def test_incorrect_sample_identity_is_rejected(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    decision = make_decision()
    changed = replace(
        decision.evidence_samples[1],
        broker_server="Other-Broker",
    )
    decision = replace(
        decision,
        evidence_samples=(
            decision.evidence_samples[0],
            changed,
            decision.evidence_samples[2],
        ),
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(decision, recorded_at_utc=BASE_TIME)


def test_incorrect_sample_offset_is_rejected(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    decision = make_decision()
    changed = replace(
        decision.evidence_samples[1],
        calculated_offset_seconds=10800.0,
    )
    decision = replace(
        decision,
        evidence_samples=(
            decision.evidence_samples[0],
            changed,
            decision.evidence_samples[2],
        ),
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(decision, recorded_at_utc=BASE_TIME)


def test_non_utc_timestamp_is_rejected(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    wrong = datetime(
        2026, 11, 18, 14, 0,
        tzinfo=timezone(timedelta(hours=2)),
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(make_decision(), recorded_at_utc=wrong)


def test_timestamp_cannot_move_backwards(tmp_path) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(
            decision_with_run_id(make_decision(), "run-002"),
            recorded_at_utc=BASE_TIME - timedelta(seconds=1),
        )


def test_empty_line_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    path.write_text("\n", encoding="utf-8")

    with pytest.raises(BrokerOffsetRegistryError):
        BrokerOffsetRegistry(path).load()


def test_invalid_json_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    path.write_text("{invalid}\n", encoding="utf-8")

    with pytest.raises(BrokerOffsetRegistryError):
        BrokerOffsetRegistry(path).load()


def test_unknown_schema_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    data = make_v1_record()
    data["schema_version"] = 99
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")

    with pytest.raises(BrokerOffsetRegistryError):
        BrokerOffsetRegistry(path).load()


def test_sequence_gap_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    first = make_v1_record()
    first["record_sequence"] = 2
    path.write_text(json.dumps(first) + "\n", encoding="utf-8")

    with pytest.raises(BrokerOffsetRegistryError):
        BrokerOffsetRegistry(path).load()


def test_non_chronological_history_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    first = make_v1_record()
    second = make_v1_record(2)
    second["recorded_at_utc"] = (
        BASE_TIME - timedelta(minutes=1)
    ).isoformat()

    path.write_text(
        json.dumps(first) + "\n" + json.dumps(second) + "\n",
        encoding="utf-8",
    )

    with pytest.raises(BrokerOffsetRegistryError):
        BrokerOffsetRegistry(path).load()


def test_v2_missing_evidence_fields_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    data = make_v1_record()
    data["schema_version"] = 2
    path.write_text(json.dumps(data) + "\n", encoding="utf-8")

    with pytest.raises(BrokerOffsetRegistryError):
        BrokerOffsetRegistry(path).load()


def test_tampered_sample_identity_is_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["evidence_samples"][1]["account_login"] = 12345
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_tampered_sample_sequence_is_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["evidence_samples"][1]["sample_sequence"] = 99
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_tampered_raw_clock_offset_is_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["evidence_samples"][0]["time_trade_server_epoch"] += 1
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_duplicate_ntp_servers_are_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["ntp_evidence"]["servers"] = [
        "ntp-a",
        "NTP-A",
        "ntp-c",
    ]
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_negative_ntp_uncertainty_is_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["ntp_evidence"]["uncertainty_seconds"] = -1.0
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_nan_ntp_offset_is_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["ntp_evidence"]["offset_seconds"] = float("nan")
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_tampered_evidence_time_range_is_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["evidence_end_utc"] = (
        BASE_TIME + timedelta(days=1)
    ).isoformat()
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_invalid_v3_event_sequence_is_rejected_on_load(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["event_type"] = "CONFIRMATION"
    records[0]["previous_offset_seconds"] = 7200
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_confirmation_must_match_previous_identity_offset(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )
    registry.append_candidate(
        decision_with_run_id(make_decision(), "run-002"),
        recorded_at_utc=BASE_TIME + timedelta(minutes=1),
    )

    records = read_json_lines(path)
    records[1]["previous_offset_seconds"] = 3600
    write_json_lines(path, records)

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_concurrent_appends_keep_unique_sequences(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"

    decisions = [
        decision_with_run_id(make_decision(), "concurrent-001"),
        decision_with_run_id(make_decision(), "concurrent-002"),
    ]

    def append_one(decision):
        registry = BrokerOffsetRegistry(path)
        return registry.append_candidate(
            decision,
            recorded_at_utc=BASE_TIME,
        )

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(append_one, decisions))

    records = BrokerOffsetRegistry(path).load()

    assert len(results) == 2
    assert len(records) == 2
    assert [item.record_sequence for item in records] == [1, 2]
    assert len({item.run_id for item in records}) == 2
    assert records[0].event_type == "BASELINE"
    assert records[1].event_type == "CONFIRMATION"


def test_truncated_trailing_record_is_rejected(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    with path.open("ab") as handle:
        handle.write(b'{"schema_version":3')

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()


def test_v2_history_remains_readable_and_unchanged(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)
    record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    data = read_json_lines(path)[0]
    data["schema_version"] = 2
    for field in (
        "event_type",
        "previous_offset_seconds",
        "transition_start_utc",
        "transition_end_utc",
    ):
        data.pop(field)

    original = json.dumps(
        data,
        sort_keys=True,
        separators=(",", ":"),
    )
    path.write_text(original + "\n", encoding="utf-8")

    loaded = registry.load()

    assert len(loaded) == 1
    assert loaded[0].schema_version == 2
    assert loaded[0].offset_seconds == record.offset_seconds
    assert path.read_text(encoding="utf-8").strip() == original
