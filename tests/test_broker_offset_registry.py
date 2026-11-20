from __future__ import annotations

import json
from concurrent.futures import (
    ProcessPoolExecutor,
    ThreadPoolExecutor,
)
from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest

import backend.market_data.broker_offset_registry as registry_module
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
    BrokerOffsetRegistryError,
)
from backend.market_data.broker_offset_detector import (
    BrokerOffsetDecision,
    BrokerOffsetDetector,
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


def decision_with_time_shift(
    decision: BrokerOffsetDecision,
    shift_seconds: int,
) -> BrokerOffsetDecision:
    shifted_samples = tuple(
        replace(
            sample,
            time_gmt_epoch=sample.time_gmt_epoch + shift_seconds,
            time_trade_server_epoch=(
                sample.time_trade_server_epoch + shift_seconds
            ),
            time_current_quote_epoch=(
                sample.time_current_quote_epoch + shift_seconds
            ),
            time_local_epoch=sample.time_local_epoch + shift_seconds,
        )
        for sample in decision.evidence_samples
    )

    return replace(
        decision,
        evidence_samples=shifted_samples,
    )


def decision_with_identity(
    decision: BrokerOffsetDecision,
    identity: tuple[int, str, str, int],
) -> BrokerOffsetDecision:
    samples = tuple(
        replace(
            sample,
            account_login=identity[0],
            broker_server=identity[1],
            symbol=identity[2],
            terminal_build=identity[3],
        )
        for sample in decision.evidence_samples
    )

    return replace(
        decision,
        identity=identity,
        evidence_samples=samples,
    )


def append_candidate_in_process(args):
    path, decision, recorded_at = args

    registry = BrokerOffsetRegistry(path)

    record = registry.append_candidate(
        decision,
        recorded_at_utc=recorded_at,
    )

    return record.record_sequence, record.run_id


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


def test_registry_uses_raw_offset_and_corrects_evidence_utc(
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
        assert sample["calculated_offset_seconds"] == pytest.approx(
            7199.75
        )

    assert record.evidence_start_utc.timestamp() == pytest.approx(
        BASE_EPOCH + 0.25
    )

    assert record.evidence_end_utc.timestamp() == pytest.approx(
        BASE_EPOCH + 120 + 0.25
    )

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


def test_same_run_sliding_confirmation_extends_contiguous_coverage(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    detector = BrokerOffsetDetector(make_verifier())
    decisions = []
    for sequence in range(1, 5):
        decision = detector.observe(make_sample(sequence))
        if decision.candidate_ready:
            decisions.append(decision)

    assert len(decisions) == 2
    first = registry.append_candidate(
        decisions[0],
        recorded_at_utc=BASE_TIME + timedelta(minutes=2),
    )
    second = registry.append_candidate(
        decisions[1],
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )

    assert second.event_type == "CONFIRMATION"
    assert second.evidence_start_utc == (
        first.evidence_start_utc + timedelta(seconds=60)
    )
    assert second.evidence_end_utc > first.evidence_end_utc
    assert registry.load() == (first, second)


def test_overlapping_confirmation_from_another_run_is_rejected(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )
    unrelated = decision_with_run_id(make_decision(), "different-run-id")

    with pytest.raises(BrokerOffsetRegistryError, match="overlaps or precedes"):
        registry.append_candidate(
            unrelated,
            recorded_at_utc=BASE_TIME + timedelta(minutes=1),
        )
    assert registry.load() == (first,)


def test_load_rejects_overlapping_confirmation_evidence(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )

    # Build a structurally valid second record whose evidence
    # overlaps the first record by 60 seconds.
    second_registry = BrokerOffsetRegistry(
        tmp_path / "second-registry.jsonl"
    )
    second_baseline = second_registry.append_candidate(
        decision_with_time_shift(make_decision(), 60),
        recorded_at_utc=BASE_TIME + timedelta(minutes=5),
    )

    overlapping_confirmation = replace(
        second_baseline,
        record_sequence=2,
        event_type="CONFIRMATION",
        previous_offset_seconds=first.offset_seconds,
    )

    path.write_bytes(
        registry._serialize_record(first)
        + registry._serialize_record(overlapping_confirmation)
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="Stored evidence overlaps or precedes",
    ):
        registry.load()


def test_load_rejects_overlap_across_intervening_v1_record(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )

    legacy = make_v1_record(sequence=2)
    legacy["recorded_at_utc"] = (
        BASE_TIME + timedelta(minutes=4)
    ).isoformat()

    second_registry = BrokerOffsetRegistry(
        tmp_path / "second-registry.jsonl"
    )
    second_baseline = second_registry.append_candidate(
        decision_with_time_shift(make_decision(), 60),
        recorded_at_utc=BASE_TIME + timedelta(minutes=5),
    )

    overlapping_confirmation = replace(
        second_baseline,
        record_sequence=3,
        event_type="CONFIRMATION",
        previous_offset_seconds=first.offset_seconds,
    )

    write_json_lines(
        path,
        [
            json.loads(registry._serialize_record(first)),
            legacy,
            json.loads(
                registry._serialize_record(
                    overlapping_confirmation
                )
            ),
        ],
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="Stored evidence overlaps or precedes",
    ):
        registry.load()


def test_new_candidate_appends_without_replacing_old_record(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )
    second_decision = decision_with_run_id(
        decision_with_time_shift(make_decision(), 180),
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
        decision_with_run_id(
            decision_with_time_shift(make_decision(), 180),
            "run-002",
        ),
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


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("ntp_checked_at_unix", BASE_EPOCH + 1),
        ("ntp_offset_seconds", 0.25),
        ("ntp_uncertainty_seconds", 0.25),
    ],
)
def test_append_rejects_ntp_summary_mismatch_with_final_sample(
    tmp_path,
    field,
    value,
) -> None:
    registry = BrokerOffsetRegistry(
        tmp_path / "offsets.jsonl"
    )

    decision = replace(
        make_decision(),
        **{field: value},
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="NTP summary does not match",
    ):
        registry.append_candidate(
            decision,
            recorded_at_utc=BASE_TIME,
        )

    assert registry.load() == ()


def test_append_rejects_ntp_sample_count_server_mismatch(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(
        tmp_path / "offsets.jsonl"
    )

    decision = replace(
        make_decision(),
        ntp_sample_count=4,
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="NTP sample count does not match",
    ):
        registry.append_candidate(
            decision,
            recorded_at_utc=BASE_TIME,
        )

    assert registry.load() == ()


def test_load_rejects_ntp_summary_mismatch_with_final_sample(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["ntp_evidence"]["offset_seconds"] = 0.25
    write_json_lines(path, records)

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="NTP summary does not match",
    ):
        registry.load()


def test_load_rejects_ntp_sample_count_server_mismatch(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    records = read_json_lines(path)
    records[0]["ntp_evidence"]["sample_count"] = 4
    write_json_lines(path, records)

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="NTP sample count does not match",
    ):
        registry.load()


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
            decision_with_run_id(
                decision_with_time_shift(make_decision(), 180),
                "run-002",
            ),
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
        decision_with_run_id(
            decision_with_time_shift(make_decision(), 180),
            "run-002",
        ),
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
        decision_with_run_id(
            decision_with_identity(
                make_decision(),
                (IDENTITY[0], IDENTITY[1], "GOLD-A", IDENTITY[3]),
            ),
            "concurrent-001",
        ),
        decision_with_run_id(
            decision_with_identity(
                make_decision(),
                (IDENTITY[0], IDENTITY[1], "GOLD-B", IDENTITY[3]),
            ),
            "concurrent-002",
        ),
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
    assert all(
        item.event_type == "BASELINE"
        for item in records
    )


def test_multiprocess_appends_keep_unique_sequences(
    tmp_path,
) -> None:
    path = str(tmp_path / "multiprocess_offsets.jsonl")

    jobs = [
        (
            path,
            decision_with_run_id(
                decision_with_identity(
                    make_decision(),
                    (
                        IDENTITY[0],
                        IDENTITY[1],
                        f"GOLD-{index:03d}",
                        IDENTITY[3],
                    ),
                ),
                f"process-run-{index:03d}",
            ),
            BASE_TIME,
        )
        for index in range(1, 9)
    ]

    with ProcessPoolExecutor(max_workers=4) as executor:
        results = list(
            executor.map(
                append_candidate_in_process,
                jobs,
            )
        )

    records = BrokerOffsetRegistry(path).load()

    assert len(results) == 8
    assert len(records) == 8

    assert sorted(
        sequence for sequence, _ in results
    ) == list(range(1, 9))

    assert [
        record.record_sequence for record in records
    ] == list(range(1, 9))

    assert len({
        record.run_id for record in records
    }) == 8

    assert all(
        record.event_type == "BASELINE"
        for record in records
    )


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


def test_append_repairs_missing_final_newline_without_corrupting_records(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    original_content = path.read_bytes()
    assert original_content.endswith(b"\n")

    # Simulate a valid legacy JSONL file without its final newline.
    path.write_bytes(original_content[:-1])

    second = registry.append_candidate(
        decision_with_time_shift(make_decision(), 180),
        recorded_at_utc=BASE_TIME + timedelta(minutes=6),
    )

    records = registry.load()
    stored_content = path.read_bytes()

    assert len(records) == 2
    assert records[0] == first
    assert records[1] == second

    assert stored_content.startswith(original_content[:-1] + b"\n")
    assert stored_content.endswith(b"\n")
    assert len(stored_content.splitlines()) == 2


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


def test_partial_write_failure_restores_original_file(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    original_content = path.read_bytes()
    original_write = registry_module.os.write
    calls = 0

    def failing_write(descriptor, data):
        nonlocal calls
        calls += 1

        if calls == 1:
            partial_size = max(1, len(data) // 2)
            return original_write(
                descriptor,
                data[:partial_size],
            )

        raise OSError("Simulated partial write failure.")

    monkeypatch.setattr(
        registry_module.os,
        "write",
        failing_write,
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(
            decision_with_run_id(
                decision_with_time_shift(make_decision(), 180),
                "partial-write-failure",
            ),
            recorded_at_utc=BASE_TIME + timedelta(minutes=1),
        )

    assert path.read_bytes() == original_content
    assert registry.load() == (first,)


def test_fsync_failure_restores_original_file(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    original_content = path.read_bytes()
    original_fsync = registry_module.os.fsync
    calls = 0

    def failing_fsync(descriptor):
        nonlocal calls
        calls += 1

        if calls == 1:
            raise OSError("Simulated fsync failure.")

        return original_fsync(descriptor)

    monkeypatch.setattr(
        registry_module.os,
        "fsync",
        failing_fsync,
    )

    with pytest.raises(BrokerOffsetRegistryError):
        registry.append_candidate(
            decision_with_run_id(
                decision_with_time_shift(make_decision(), 180),
                "fsync-failure",
            ),
            recorded_at_utc=BASE_TIME + timedelta(minutes=1),
        )

    assert calls == 2
    assert path.read_bytes() == original_content
    assert registry.load() == (first,)


def test_rollback_failure_reports_uncertain_registry_integrity(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    original_content = path.read_bytes()
    original_write = registry_module.os.write

    def partial_then_fail(descriptor, data):
        partial_size = max(1, len(data) // 2)

        monkeypatch.setattr(
            registry_module.os,
            "write",
            lambda *_args: (_ for _ in ()).throw(
                OSError("Simulated write failure.")
            ),
        )

        return original_write(
            descriptor,
            data[:partial_size],
        )

    def failing_truncate(*_args):
        raise OSError("Simulated rollback failure.")

    monkeypatch.setattr(
        registry_module.os,
        "write",
        partial_then_fail,
    )
    monkeypatch.setattr(
        registry_module.os,
        "ftruncate",
        failing_truncate,
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="rollback also failed",
    ):
        registry.append_candidate(
            decision_with_run_id(
                decision_with_time_shift(make_decision(), 180),
                "rollback-failure",
            ),
            recorded_at_utc=BASE_TIME + timedelta(minutes=1),
        )

    assert path.read_bytes() != original_content

    with pytest.raises(BrokerOffsetRegistryError):
        registry.load()

    assert first.record_sequence == 1


def test_rollback_fsync_failure_reports_uncertain_integrity(
    tmp_path,
    monkeypatch,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    first = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    original_content = path.read_bytes()
    original_write = registry_module.os.write

    def partial_then_fail(descriptor, data):
        partial_size = max(1, len(data) // 2)

        monkeypatch.setattr(
            registry_module.os,
            "write",
            lambda *_args: (_ for _ in ()).throw(
                OSError("Simulated write failure.")
            ),
        )

        return original_write(
            descriptor,
            data[:partial_size],
        )

    def failing_fsync(_descriptor):
        raise OSError("Simulated rollback fsync failure.")

    monkeypatch.setattr(
        registry_module.os,
        "write",
        partial_then_fail,
    )
    monkeypatch.setattr(
        registry_module.os,
        "fsync",
        failing_fsync,
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="rollback also failed",
    ):
        registry.append_candidate(
            decision_with_run_id(
                decision_with_time_shift(make_decision(), 180),
                "rollback-fsync-failure",
            ),
            recorded_at_utc=BASE_TIME + timedelta(minutes=1),
        )

    # The truncate succeeded, but its durability could not be confirmed.
    assert path.read_bytes() == original_content

    # The current process can read the restored contents.
    assert registry.load() == (first,)


def test_v2_history_accepts_new_v3_confirmation(tmp_path) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    original_record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME,
    )

    legacy_data = read_json_lines(path)[0]
    legacy_data["schema_version"] = 2

    for field in (
        "event_type",
        "previous_offset_seconds",
        "transition_start_utc",
        "transition_end_utc",
    ):
        legacy_data.pop(field)

    original_v2 = json.dumps(
        legacy_data,
        sort_keys=True,
        separators=(",", ":"),
    )

    path.write_text(original_v2 + "\n", encoding="utf-8")

    new_record = registry.append_candidate(
        decision_with_time_shift(make_decision(), 180),
        recorded_at_utc=BASE_TIME + timedelta(minutes=1),
    )

    records = registry.load()

    assert len(records) == 2
    assert records[0].schema_version == 2
    assert records[0].offset_seconds == original_record.offset_seconds
    assert records[1].schema_version == 3
    assert records[1].event_type == "CONFIRMATION"
    assert records[1].previous_offset_seconds == records[0].offset_seconds
    assert records[1] == new_record

    stored_lines = path.read_text(encoding="utf-8").splitlines()
    assert stored_lines[0] == original_v2


def decision_with_offset_and_time(
    offset_seconds: int,
    shift_seconds: int,
) -> BrokerOffsetDecision:
    decision = make_decision()
    old_offset = decision.offset_seconds
    offset_delta = offset_seconds - old_offset

    changed_samples = tuple(
        replace(
            sample,
            time_gmt_epoch=(
                sample.time_gmt_epoch + shift_seconds
            ),
            time_trade_server_epoch=(
                sample.time_trade_server_epoch
                + shift_seconds
                + offset_delta
            ),
            time_current_quote_epoch=(
                sample.time_current_quote_epoch
                + shift_seconds
                + offset_delta
            ),
            time_local_epoch=(
                sample.time_local_epoch + shift_seconds
            ),
            server_utc_offset_seconds=offset_seconds,
            calculated_offset_seconds=float(offset_seconds),
        )
        for sample in decision.evidence_samples
    )

    return replace(
        decision,
        offset_seconds=offset_seconds,
        evidence_samples=changed_samples,
    )


def test_for_time_uses_only_explicit_evidence_interval(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )

    assert registry.for_time(
        IDENTITY,
        record.evidence_start_utc,
    ) == record

    assert registry.for_time(
        IDENTITY,
        record.evidence_end_utc,
    ) == record

    assert registry.for_time(
        IDENTITY,
        record.evidence_start_utc - timedelta(microseconds=1),
    ) is None

    assert registry.for_time(
        IDENTITY,
        record.evidence_end_utc + timedelta(microseconds=1),
    ) is None


def test_for_broker_time_uses_only_matching_explicit_evidence(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")
    record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )

    proven_utc = record.evidence_start_utc + timedelta(seconds=30)
    raw_broker_epoch = int(proven_utc.timestamp()) + record.offset_seconds

    assert registry.for_broker_time(
        IDENTITY,
        raw_broker_epoch,
    ) == record

    outside_raw_epoch = (
        int((record.evidence_start_utc - timedelta(seconds=1)).timestamp())
        + record.offset_seconds
    )
    assert registry.for_broker_time(
        IDENTITY,
        outside_raw_epoch,
    ) is None


def test_for_broker_time_rejects_ambiguous_offset_evidence(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    old_record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )
    new_record = registry.append_candidate(
        decision_with_offset_and_time(3600, 3600),
        recorded_at_utc=BASE_TIME + timedelta(hours=2),
    )

    assert new_record.event_type == "OFFSET_CHANGE"

    # The same raw broker epoch maps into both explicit evidence windows
    # when each record's own offset is applied. It must not be guessed.
    raw_epoch = (
        int((old_record.evidence_start_utc + timedelta(seconds=30)).timestamp())
        + old_record.offset_seconds
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="Conflicting historical offset evidence",
    ):
        registry.for_broker_time(IDENTITY, raw_epoch)


@pytest.mark.parametrize(
    "raw_timestamp_epoch",
    [0, -1, True, float("inf"), float("nan"), "123"],
)
def test_for_broker_time_rejects_invalid_raw_timestamps(
    tmp_path,
    raw_timestamp_epoch,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    with pytest.raises(BrokerOffsetRegistryError):
        registry.for_broker_time(
            IDENTITY,
            raw_timestamp_epoch,
        )


def test_for_time_never_uses_v1_record_as_historical_proof(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"

    path.write_text(
        json.dumps(
            make_v1_record(),
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n",
        encoding="utf-8",
    )

    registry = BrokerOffsetRegistry(path)

    assert registry.for_time(IDENTITY, BASE_TIME) is None


def test_offset_change_leaves_unresolved_gap_between_evidence_windows(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    old_record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )

    new_record = registry.append_candidate(
        decision_with_offset_and_time(3600, 600),
        recorded_at_utc=BASE_TIME + timedelta(minutes=15),
    )

    assert new_record.event_type == "OFFSET_CHANGE"

    assert (
        new_record.transition_start_utc
        == old_record.evidence_end_utc
    )

    assert (
        new_record.transition_end_utc
        == new_record.evidence_start_utc
    )

    old_time = (
        old_record.evidence_start_utc + timedelta(seconds=30)
    )

    gap_time = (
        old_record.evidence_end_utc + timedelta(minutes=4)
    )

    new_time = (
        new_record.evidence_start_utc + timedelta(seconds=30)
    )

    assert registry.for_time(
        IDENTITY,
        old_time,
    ).offset_seconds == 7200

    assert registry.for_time(
        IDENTITY,
        gap_time,
    ) is None

    assert registry.for_time(
        IDENTITY,
        new_time,
    ).offset_seconds == 3600


def test_raw_broker_timestamp_in_offset_change_gap_is_unresolved(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(tmp_path / "offsets.jsonl")

    old_record = registry.append_candidate(
        make_decision(),
        recorded_at_utc=BASE_TIME + timedelta(minutes=3),
    )
    new_record = registry.append_candidate(
        decision_with_offset_and_time(3600, 600),
        recorded_at_utc=BASE_TIME + timedelta(minutes=15),
    )

    assert new_record.event_type == "OFFSET_CHANGE"

    gap_utc = old_record.evidence_end_utc + timedelta(minutes=4)
    raw_broker_epoch = int(gap_utc.timestamp()) + old_record.offset_seconds

    assert registry.for_broker_time(
        IDENTITY,
        raw_broker_epoch,
    ) is None

    assert registry.for_broker_times(
        IDENTITY,
        (raw_broker_epoch,),
    ) == (None,)


def test_offset_change_after_v1_does_not_invent_transition_bounds(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"

    path.write_text(
        json.dumps(
            make_v1_record(),
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n",
        encoding="utf-8",
    )

    registry = BrokerOffsetRegistry(path)

    new_record = registry.append_candidate(
        decision_with_offset_and_time(3600, 600),
        recorded_at_utc=BASE_TIME + timedelta(minutes=15),
    )

    assert new_record.event_type == "OFFSET_CHANGE"
    assert new_record.transition_start_utc is None
    assert new_record.transition_end_utc is None

    assert registry.for_time(
        IDENTITY,
        new_record.evidence_start_utc + timedelta(seconds=30),
    ) == new_record

    assert registry.for_time(
        IDENTITY,
        BASE_TIME,
    ) is None


def test_append_rejects_legacy_ntp_adjusted_calculation(
    tmp_path,
) -> None:
    registry = BrokerOffsetRegistry(
        tmp_path / "offsets.jsonl"
    )

    decision = make_decision(
        ntp_offset_seconds=0.25
    )

    legacy_samples = tuple(
        replace(
            sample,
            calculated_offset_seconds=(
                sample.calculated_offset_seconds - 0.25
            ),
        )
        for sample in decision.evidence_samples
    )

    legacy_decision = replace(
        decision,
        evidence_samples=legacy_samples,
    )

    with pytest.raises(
        BrokerOffsetRegistryError,
        match="calculated offset conflicts",
    ):
        registry.append_candidate(
            legacy_decision,
            recorded_at_utc=BASE_TIME,
        )

    assert registry.load() == ()


def test_load_preserves_legacy_ntp_adjusted_records_without_rewriting(
    tmp_path,
) -> None:
    path = tmp_path / "offsets.jsonl"
    registry = BrokerOffsetRegistry(path)

    decision = make_decision(
        ntp_offset_seconds=0.25
    )

    record = registry.append_candidate(
        decision,
        recorded_at_utc=BASE_TIME,
    )

    stored = read_json_lines(path)

    for sample in stored[0]["evidence_samples"]:
        sample["calculated_offset_seconds"] -= 0.25

    write_json_lines(path, stored)

    original_content = path.read_text(encoding="utf-8")

    loaded = registry.load()

    assert len(loaded) == 1
    assert loaded[0].offset_seconds == 7200
    assert loaded[0].record_sequence == record.record_sequence

    assert path.read_text(encoding="utf-8") == original_content
