from __future__ import annotations

import csv
from datetime import datetime, timedelta, timezone

import backend.market_data.broker_offset_registry as registry_module

from backend.market_data.broker_offset_detector import (
    BrokerOffsetDetector,
)
from backend.market_data.broker_offset_registry import (
    BrokerOffsetRegistry,
)
from backend.market_data.clock_history import CLOCK_HISTORY_COLUMNS
from backend.market_data.broker_offset_service import (
    BrokerOffsetEvidenceService,
)
from backend.market_data.ntp_time import NtpSample, NtpTimeVerifier
from backend.market_data.clock_history_lock import (
    ClockHistoryFileLock,
)


UTC = timezone.utc
REAL_DATETIME = datetime
BASE_TIME = datetime.now(UTC).replace(microsecond=0) - timedelta(minutes=10)
BASE_EPOCH = int(BASE_TIME.timestamp())

ACCOUNT = 337260380
SERVER = "XMGlobal-MT5 9"
SYMBOL = "GOLD"
BUILD = 6316
IDENTITY = (ACCOUNT, SERVER, SYMBOL, BUILD)


def install_chronological_registry_clock(monkeypatch):
    """Give each registry append a deterministic UTC timestamp."""

    class ControlledDateTime(REAL_DATETIME):
        calls = 0

        @classmethod
        def now(cls, tz=None):
            cls.calls += 1
            value = BASE_TIME + timedelta(minutes=3 * cls.calls)

            if tz is None:
                return cls(
                    value.year,
                    value.month,
                    value.day,
                    value.hour,
                    value.minute,
                    value.second,
                    value.microsecond,
                )

            value = value.astimezone(tz)

            return cls(
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.microsecond,
                tzinfo=value.tzinfo,
                fold=value.fold,
            )

    monkeypatch.setattr(
        registry_module,
        "datetime",
        ControlledDateTime,
    )


def make_verifier() -> NtpTimeVerifier:
    verifier = NtpTimeVerifier(
        servers=("ntp-a", "ntp-b", "ntp-c"),
        wall_clock=lambda: float(BASE_EPOCH),
        monotonic_clock=lambda: 100.0,
        query=lambda server: NtpSample(
            server=server,
            offset_seconds=0.0,
            round_trip_seconds=0.02,
            root_dispersion_seconds=0.01,
        ),
    )
    assert verifier.verify().verified
    return verifier


def make_row(sequence: int, offset: int) -> dict[str, str]:
    gmt = BASE_EPOCH + (sequence - 1) * 60

    return {
        "schema_version": "1",
        "run_id": "service-test-run",
        "sample_sequence": str(sequence),
        "monotonic_ms": str(1000 + (sequence - 1) * 60000),
        "time_gmt_epoch": str(gmt),
        "time_trade_server_epoch": str(gmt + offset),
        "time_current_quote_epoch": str(gmt + offset),
        "time_local_epoch": str(gmt),
        "server_utc_offset_seconds": str(offset),
        "clock_values_valid": "true",
        "account_login": str(ACCOUNT),
        "broker_server": SERVER,
        "symbol": SYMBOL,
        "terminal_build": str(BUILD),
        "terminal_connected": "true",
    }


def write_history(path, rows: list[dict[str, str]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=CLOCK_HISTORY_COLUMNS,
            delimiter=";",
            lineterminator="\n",
        )
        writer.writeheader()
        writer.writerows(rows)


def append_rows(path, rows: list[dict[str, str]]) -> None:
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=CLOCK_HISTORY_COLUMNS,
            delimiter=";",
            lineterminator="\n",
        )
        writer.writerows(rows)


def make_service(tmp_path, initial_rows):
    history_path = tmp_path / "clock_history.csv"
    registry_path = tmp_path / "offsets.jsonl"

    write_history(history_path, initial_rows)

    detector = BrokerOffsetDetector(make_verifier())
    registry = BrokerOffsetRegistry(registry_path)

    service = BrokerOffsetEvidenceService(
        history_path,
        detector,
        registry,
    )

    return service, registry, history_path


def test_first_three_supported_samples_record_baseline(tmp_path):
    rows = [make_row(sequence, 7200) for sequence in range(1, 4)]
    service, registry, _ = make_service(tmp_path, rows)

    snapshot = service.poll_once()
    records = registry.load()

    assert snapshot.error is None
    assert snapshot.samples_processed == 3
    assert len(records) == 1
    assert records[0].schema_version == 3
    assert records[0].event_type == "BASELINE"
    assert records[0].offset_seconds == 7200
    assert records[0].previous_offset_seconds is None
    assert records[0].evidence_sample_count == 3
    assert records[0].ntp_evidence["verified"] is True


def test_polling_without_new_samples_does_not_duplicate_record(tmp_path):
    rows = [make_row(sequence, 7200) for sequence in range(1, 4)]
    service, registry, _ = make_service(tmp_path, rows)

    first = service.poll_once()
    second = service.poll_once()

    assert first.error is None
    assert second.error is None
    assert len(registry.load()) == 1


def test_overlapping_same_offset_candidate_does_not_record_confirmation(
    tmp_path,
):
    rows = [make_row(sequence, 7200) for sequence in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first = service.poll_once()
    assert first.error is None
    assert len(registry.load()) == 1

    # Sequence 4 creates a sliding candidate window of 2, 3, 4.
    # It overlaps the baseline window 1, 2, 3 and must not be stored.
    append_rows(history_path, [make_row(4, 7200)])

    second = service.poll_once()
    records = registry.load()

    assert second.error is None
    assert second.state == "WAITING_FOR_NEW_EVIDENCE"
    assert len(records) == 1
    assert records[0].event_type == "BASELINE"


def test_three_independent_same_offset_samples_record_confirmation(
    tmp_path,
):
    rows = [make_row(sequence, 7200) for sequence in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first = service.poll_once()
    assert first.error is None
    assert len(registry.load()) == 1

    # The next two samples complete the independent window 4, 5, 6.
    append_rows(
        history_path,
        [
            make_row(4, 7200),
            make_row(5, 7200),
            make_row(6, 7200),
        ],
    )

    snapshot = service.poll_once()
    records = registry.load()

    assert snapshot.error is None
    assert snapshot.state == "CONFIRMATION_RECORDED"
    assert len(records) == 2

    baseline, confirmation = records

    assert baseline.event_type == "BASELINE"
    assert baseline.offset_seconds == 7200

    assert confirmation.event_type == "CONFIRMATION"
    assert confirmation.offset_seconds == 7200
    assert confirmation.previous_offset_seconds == 7200
    assert confirmation.evidence_sample_count == 3

    sequences = [
        item["sample_sequence"]
        for item in confirmation.evidence_samples
    ]
    assert sequences == [4, 5, 6]


def test_three_new_samples_record_offset_change(tmp_path, monkeypatch):
    install_chronological_registry_clock(monkeypatch)

    rows = [make_row(sequence, 7200) for sequence in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first_snapshot = service.poll_once()
    assert first_snapshot.error is None

    append_rows(
        history_path,
        [make_row(sequence, 10800) for sequence in range(4, 7)],
    )

    snapshot = service.poll_once()
    records = registry.load()

    assert snapshot.error is None
    assert len(records) == 2

    baseline, change = records

    assert baseline.event_type == "BASELINE"
    assert baseline.offset_seconds == 7200

    assert change.event_type == "OFFSET_CHANGE"
    assert change.previous_offset_seconds == 7200
    assert change.offset_seconds == 10800
    assert change.transition_start_utc == baseline.recorded_at_utc
    assert change.transition_end_utc is not None
    assert change.transition_end_utc > change.transition_start_utc
    assert change.recorded_at_utc >= change.transition_end_utc


def test_return_to_previous_offset_is_recorded_as_new_change(
    tmp_path,
    monkeypatch,
):
    install_chronological_registry_clock(monkeypatch)

    rows = [make_row(sequence, 7200) for sequence in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first_snapshot = service.poll_once()
    assert first_snapshot.error is None

    append_rows(
        history_path,
        [make_row(sequence, 10800) for sequence in range(4, 7)],
    )
    second_snapshot = service.poll_once()
    assert second_snapshot.error is None

    append_rows(
        history_path,
        [make_row(sequence, 7200) for sequence in range(7, 10)],
    )
    snapshot = service.poll_once()

    records = registry.load()

    assert snapshot.error is None
    assert len(records) == 3

    baseline, first_change, return_change = records

    assert baseline.event_type == "BASELINE"
    assert baseline.offset_seconds == 7200

    assert first_change.event_type == "OFFSET_CHANGE"
    assert first_change.previous_offset_seconds == 7200
    assert first_change.offset_seconds == 10800

    assert return_change.event_type == "OFFSET_CHANGE"
    assert return_change.previous_offset_seconds == 10800
    assert return_change.offset_seconds == 7200

    assert return_change.record_sequence == 3
    assert return_change.transition_start_utc == first_change.recorded_at_utc
    assert return_change.transition_end_utc is not None
    assert return_change.transition_end_utc > return_change.transition_start_utc
    assert return_change.recorded_at_utc >= return_change.transition_end_utc


def test_incomplete_history_does_not_create_candidate(tmp_path):
    rows = [make_row(sequence, 7200) for sequence in range(1, 3)]
    service, registry, _ = make_service(tmp_path, rows)

    snapshot = service.poll_once()

    assert snapshot.error is None
    assert len(registry.load()) == 0


def test_corrupt_history_reports_error_without_recording(tmp_path):
    history_path = tmp_path / "clock_history.csv"
    registry_path = tmp_path / "offsets.jsonl"

    history_path.write_text(
        "invalid;clock;history\nbroken\n",
        encoding="utf-8",
    )

    service = BrokerOffsetEvidenceService(
        history_path,
        BrokerOffsetDetector(make_verifier()),
        BrokerOffsetRegistry(registry_path),
    )

    snapshot = service.poll_once()

    assert snapshot.state == "ERROR"
    assert snapshot.error is not None
    assert len(BrokerOffsetRegistry(registry_path).load()) == 0


def test_corrupt_history_clears_stale_decision_but_preserves_record(
    tmp_path,
):
    rows = [
        make_row(sequence, 7200)
        for sequence in range(1, 4)
    ]
    service, registry, history_path = make_service(
        tmp_path,
        rows,
    )

    first = service.poll_once()

    assert first.state == "BASELINE_RECORDED"
    assert first.last_decision is not None
    assert first.last_record is not None
    assert len(registry.load()) == 1

    history_path.write_text(
        "invalid;clock;history\nbroken\n",
        encoding="utf-8",
    )

    failed = service.poll_once()

    assert failed.state == "ERROR"
    assert failed.error is not None
    assert failed.last_decision is None

    # Historical registry data must remain intact.
    assert failed.last_record == first.last_record
    assert len(registry.load()) == 1


def test_modified_old_history_sample_is_rejected(
    tmp_path,
):
    rows = [
        make_row(sequence, 7200)
        for sequence in range(1, 4)
    ]
    service, registry, history_path = make_service(
        tmp_path,
        rows,
    )

    first = service.poll_once()

    assert first.state == "BASELINE_RECORDED"
    assert len(registry.load()) == 1

    modified_rows = [dict(row) for row in rows]
    modified_rows[0]["time_trade_server_epoch"] = str(
        int(modified_rows[0]["time_gmt_epoch"]) + 10800
    )
    modified_rows[0]["time_current_quote_epoch"] = str(
        int(modified_rows[0]["time_gmt_epoch"]) + 10800
    )
    modified_rows[0]["server_utc_offset_seconds"] = "10800"

    write_history(history_path, modified_rows)

    second = service.poll_once()

    assert second.state == "ERROR"
    assert second.error is not None
    assert "modified" in second.error.lower()
    assert second.last_decision is None

    # The original registry evidence must remain untouched.
    assert len(registry.load()) == 1
    assert second.last_record == first.last_record


def test_new_mt5_account_identity_gets_independent_baseline(
    tmp_path,
    monkeypatch,
):
    install_chronological_registry_clock(monkeypatch)

    original_rows = [
        make_row(sequence, 7200)
        for sequence in range(1, 4)
    ]
    service, registry, history_path = make_service(
        tmp_path,
        original_rows,
    )

    first = service.poll_once()

    assert first.state == "BASELINE_RECORDED"
    assert len(registry.load()) == 1

    # Simulate a new EA run connected to a different MT5 account.
    new_account_rows = []

    for sequence in range(1, 4):
        row = make_row(sequence, 7200)
        row["run_id"] = "different-account-run"
        row["account_login"] = str(ACCOUNT + 1)
        row["monotonic_ms"] = str(5000 + sequence * 60000)
        row["time_gmt_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60
        )
        row["time_trade_server_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60 + 7200
        )
        row["time_current_quote_epoch"] = row[
            "time_trade_server_epoch"
        ]
        new_account_rows.append(row)

    append_rows(history_path, new_account_rows)

    second = service.poll_once()
    records = registry.load()

    assert second.state == "BASELINE_RECORDED"
    assert len(records) == 2

    old_record, new_record = records

    assert old_record.event_type == "BASELINE"
    assert old_record.identity == IDENTITY

    assert new_record.event_type == "BASELINE"
    assert new_record.identity == (
        ACCOUNT + 1,
        SERVER,
        SYMBOL,
        BUILD,
    )

    assert new_record.run_id == "different-account-run"
    assert all(
        sample["account_login"] == ACCOUNT + 1
        for sample in new_record.evidence_samples
    )


def test_new_broker_server_identity_gets_independent_baseline(
    tmp_path,
    monkeypatch,
):
    install_chronological_registry_clock(monkeypatch)

    original_rows = [
        make_row(sequence, 7200)
        for sequence in range(1, 4)
    ]
    service, registry, history_path = make_service(
        tmp_path,
        original_rows,
    )

    first = service.poll_once()

    assert first.state == "BASELINE_RECORDED"
    assert len(registry.load()) == 1

    new_server_rows = []

    for sequence in range(1, 4):
        row = make_row(sequence, 7200)
        row["run_id"] = "different-server-run"
        row["broker_server"] = "Different-Broker-Server"
        row["monotonic_ms"] = str(5000 + sequence * 60000)
        row["time_gmt_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60
        )
        row["time_trade_server_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60 + 7200
        )
        row["time_current_quote_epoch"] = row[
            "time_trade_server_epoch"
        ]
        new_server_rows.append(row)

    append_rows(history_path, new_server_rows)

    second = service.poll_once()
    records = registry.load()

    assert second.state == "BASELINE_RECORDED"
    assert len(records) == 2

    old_record, new_record = records

    assert old_record.identity == IDENTITY
    assert old_record.event_type == "BASELINE"

    assert new_record.identity == (
        ACCOUNT,
        "Different-Broker-Server",
        SYMBOL,
        BUILD,
    )
    assert new_record.event_type == "BASELINE"
    assert new_record.run_id == "different-server-run"

    assert all(
        sample["broker_server"] == "Different-Broker-Server"
        for sample in new_record.evidence_samples
    )


def test_new_symbol_identity_gets_independent_baseline(
    tmp_path,
    monkeypatch,
):
    install_chronological_registry_clock(monkeypatch)

    rows = [make_row(i, 7200) for i in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first = service.poll_once()
    assert first.state == "BASELINE_RECORDED"

    new_rows = []
    for sequence in range(1, 4):
        row = make_row(sequence, 7200)
        row["run_id"] = "different-symbol-run"
        row["symbol"] = "EURUSD"
        row["monotonic_ms"] = str(5000 + sequence * 60000)
        row["time_gmt_epoch"] = str(BASE_EPOCH + 300 + sequence * 60)
        row["time_trade_server_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60 + 7200
        )
        row["time_current_quote_epoch"] = row["time_trade_server_epoch"]
        new_rows.append(row)

    append_rows(history_path, new_rows)
    second = service.poll_once()
    records = registry.load()

    assert second.state == "BASELINE_RECORDED"
    assert len(records) == 2
    assert records[0].identity == IDENTITY
    assert records[1].identity == (ACCOUNT, SERVER, "EURUSD", BUILD)
    assert records[1].event_type == "BASELINE"
    assert all(s["symbol"] == "EURUSD" for s in records[1].evidence_samples)


def test_new_terminal_build_identity_gets_independent_baseline(
    tmp_path,
    monkeypatch,
):
    install_chronological_registry_clock(monkeypatch)

    rows = [make_row(i, 7200) for i in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first = service.poll_once()
    assert first.state == "BASELINE_RECORDED"

    new_rows = []
    for sequence in range(1, 4):
        row = make_row(sequence, 7200)
        row["run_id"] = "different-build-run"
        row["terminal_build"] = "6400"
        row["monotonic_ms"] = str(5000 + sequence * 60000)
        row["time_gmt_epoch"] = str(BASE_EPOCH + 300 + sequence * 60)
        row["time_trade_server_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60 + 7200
        )
        row["time_current_quote_epoch"] = row["time_trade_server_epoch"]
        new_rows.append(row)

    append_rows(history_path, new_rows)
    second = service.poll_once()
    records = registry.load()

    assert second.state == "BASELINE_RECORDED"
    assert len(records) == 2
    assert records[0].identity == IDENTITY
    assert records[1].identity == (ACCOUNT, SERVER, SYMBOL, 6400)
    assert records[1].event_type == "BASELINE"
    assert all(
        s["terminal_build"] == 6400
        for s in records[1].evidence_samples
    )


def test_completely_changed_identity_gets_independent_baseline(
    tmp_path,
    monkeypatch,
):
    install_chronological_registry_clock(monkeypatch)

    rows = [make_row(i, 7200) for i in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first = service.poll_once()
    assert first.state == "BASELINE_RECORDED"

    new_rows = []
    for sequence in range(1, 4):
        row = make_row(sequence, 7200)
        row["run_id"] = "completely-new-identity"
        row["account_login"] = str(ACCOUNT + 1)
        row["broker_server"] = "Another-Broker"
        row["symbol"] = "EURUSD"
        row["terminal_build"] = "6400"
        row["monotonic_ms"] = str(5000 + sequence * 60000)
        row["time_gmt_epoch"] = str(BASE_EPOCH + 300 + sequence * 60)
        row["time_trade_server_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60 + 7200
        )
        row["time_current_quote_epoch"] = row["time_trade_server_epoch"]
        new_rows.append(row)

    append_rows(history_path, new_rows)
    second = service.poll_once()
    records = registry.load()

    expected_identity = (
        ACCOUNT + 1,
        "Another-Broker",
        "EURUSD",
        6400,
    )

    assert second.state == "BASELINE_RECORDED"
    assert len(records) == 2
    assert records[0].identity == IDENTITY
    assert records[1].identity == expected_identity
    assert records[1].event_type == "BASELINE"
    assert all(
        (
            s["account_login"],
            s["broker_server"],
            s["symbol"],
            s["terminal_build"],
        ) == expected_identity
        for s in records[1].evidence_samples
    )


def test_offset_change_is_tracked_independently_per_identity(
    tmp_path,
    monkeypatch,
):
    install_chronological_registry_clock(monkeypatch)

    rows = [make_row(i, 7200) for i in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first = service.poll_once()
    assert first.state == "BASELINE_RECORDED"

    # A different symbol gets its own baseline.
    second_identity_rows = []

    for sequence in range(1, 4):
        row = make_row(sequence, 10800)
        row["run_id"] = "second-identity-run"
        row["symbol"] = "EURUSD"
        row["monotonic_ms"] = str(5000 + sequence * 60000)
        row["time_gmt_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60
        )
        row["time_trade_server_epoch"] = str(
            BASE_EPOCH + 300 + sequence * 60 + 10800
        )
        row["time_current_quote_epoch"] = row[
            "time_trade_server_epoch"
        ]
        second_identity_rows.append(row)

    append_rows(history_path, second_identity_rows)

    second = service.poll_once()

    assert second.state == "BASELINE_RECORDED"
    assert len(registry.load()) == 2

    # A new run for EURUSD reports a changed offset.
    # Keep its evidence interval after the previous record
    # and before the next registry recording timestamp.
    changed_rows = []

    for sequence in range(1, 4):
        row = make_row(sequence, 3600)
        row["run_id"] = "third-offset-run"
        row["symbol"] = "EURUSD"
        row["monotonic_ms"] = str(9000 + sequence * 60000)
        row["time_gmt_epoch"] = str(
            BASE_EPOCH + 360 + sequence * 30
        )
        row["time_trade_server_epoch"] = str(
            BASE_EPOCH + 360 + sequence * 30 + 3600
        )
        row["time_current_quote_epoch"] = row[
            "time_trade_server_epoch"
        ]
        changed_rows.append(row)

    append_rows(history_path, changed_rows)

    third = service.poll_once()
    records = registry.load()

    assert third.state == "OFFSET_CHANGE_RECORDED"
    assert len(records) == 3

    original_baseline, second_baseline, offset_change = records

    assert original_baseline.identity == IDENTITY
    assert original_baseline.offset_seconds == 7200
    assert original_baseline.event_type == "BASELINE"

    assert second_baseline.identity == (
        ACCOUNT, SERVER, "EURUSD", BUILD
    )
    assert second_baseline.offset_seconds == 10800
    assert second_baseline.event_type == "BASELINE"

    assert offset_change.identity == (
        ACCOUNT, SERVER, "EURUSD", BUILD
    )
    assert offset_change.offset_seconds == 3600
    assert offset_change.previous_offset_seconds == 10800
    assert offset_change.event_type == "OFFSET_CHANGE"


def test_service_restart_preserves_registry_and_requires_new_evidence(
    tmp_path,
):
    rows = [make_row(i, 7200) for i in range(1, 4)]
    service, registry, history_path = make_service(tmp_path, rows)

    first = service.poll_once()

    assert first.state == "BASELINE_RECORDED"
    original_records = registry.load()
    assert len(original_records) == 1

    # Recreate the service as if the backend process restarted.
    restarted_service = BrokerOffsetEvidenceService(
        history_path,
        BrokerOffsetDetector(make_verifier()),
        BrokerOffsetRegistry(registry.path),
    )

    after_restart = restarted_service.poll_once()

    # Previously stored evidence must not be recorded again.
    assert after_restart.state == "WAITING_FOR_NEW_EVIDENCE"
    assert len(registry.load()) == 1
    assert registry.load()[0] == original_records[0]

    # Supply three genuinely new samples after restart.
    append_rows(
        history_path,
        [make_row(i, 7200) for i in range(4, 7)],
    )

    after_new_evidence = restarted_service.poll_once()
    records = registry.load()

    assert after_new_evidence.state == "CONFIRMATION_RECORDED"
    assert len(records) == 2
    assert records[0] == original_records[0]
    assert records[1].event_type == "CONFIRMATION"
    assert records[1].offset_seconds == 7200
    assert [
        sample["sample_sequence"]
        for sample in records[1].evidence_samples
    ] == [4, 5, 6]


def test_service_restart_uses_incremental_checkpoint(
    tmp_path,
):
    rows = [
        make_row(sequence, 7200)
        for sequence in range(1, 4)
    ]
    service, registry, history_path = make_service(
        tmp_path,
        rows,
    )

    first = service.poll_once()

    assert first.state == "BASELINE_RECORDED"
    assert first.samples_processed == 3

    checkpoint_path = history_path.with_suffix(
        ".checkpoint.json"
    )
    assert checkpoint_path.exists()

    restarted_service = BrokerOffsetEvidenceService(
        history_path,
        BrokerOffsetDetector(make_verifier()),
        BrokerOffsetRegistry(registry.path),
    )

    after_restart = restarted_service.poll_once()

    assert after_restart.error is None
    assert after_restart.state == "WAITING_FOR_NEW_EVIDENCE"
    assert after_restart.samples_processed == 3
    assert len(registry.load()) == 1

    append_rows(
        history_path,
        [
            make_row(sequence, 7200)
            for sequence in range(4, 7)
        ],
    )

    after_new_samples = restarted_service.poll_once()

    assert after_new_samples.error is None
    assert after_new_samples.state == "CONFIRMATION_RECORDED"
    assert after_new_samples.samples_processed == 6

    records = registry.load()
    assert len(records) == 2
    assert records[1].event_type == "CONFIRMATION"
    assert [
        item["sample_sequence"]
        for item in records[1].evidence_samples
    ] == [4, 5, 6]


def test_registry_append_is_not_duplicated_when_checkpoint_save_fails(
    tmp_path,
    monkeypatch,
):
    rows = [
        make_row(sequence, 7200)
        for sequence in range(1, 4)
    ]
    service, registry, history_path = make_service(
        tmp_path,
        rows,
    )

    checkpoint_path = history_path.with_suffix(
        ".checkpoint.json"
    )

    fail_once = True

    def fail_checkpoint_save(checkpoint):
        nonlocal fail_once

        if fail_once:
            fail_once = False
            raise OSError(
                "Simulated checkpoint persistence failure"
            )

    monkeypatch.setattr(
        service._reader.store,
        "save",
        fail_checkpoint_save,
    )

    # Registry append succeeds, but checkpoint persistence fails.
    first = service.poll_once()

    assert first.state == "ERROR"
    assert "checkpoint persistence failure" in first.error.lower()

    records_after_failure = registry.load()

    assert len(records_after_failure) == 1
    assert records_after_failure[0].event_type == "BASELINE"
    assert not checkpoint_path.exists()

    # Retry must replay the samples without duplicating the event.
    second = service.poll_once()

    assert second.error is None
    assert second.state == "WAITING_FOR_NEW_EVIDENCE"
    assert second.samples_processed == 3
    assert checkpoint_path.exists()

    final_records = registry.load()

    assert len(final_records) == 1
    assert final_records[0] == records_after_failure[0]


def test_writer_lock_contention_preserves_detector_and_resumes(
    tmp_path,
):
    rows = [
        make_row(1, 7200),
        make_row(2, 7200),
    ]
    service, registry, history_path = make_service(
        tmp_path,
        rows,
    )

    first = service.poll_once()

    assert first.state == "COLLECTING"
    assert first.samples_processed == 2
    assert first.last_decision is not None
    assert len(registry.load()) == 0

    writer_lock = ClockHistoryFileLock(
        history_path.with_name("clock_history.writer.lock"),
        timeout_seconds=0,
    )
    writer_lock.acquire()

    try:
        append_rows(
            history_path,
            [make_row(3, 7200)],
        )

        waiting = service.poll_once()

        assert waiting.state == "WAITING_FOR_WRITER"
        assert waiting.error is None
        assert waiting.samples_processed == 2
        assert waiting.last_decision == first.last_decision
        assert len(registry.load()) == 0
    finally:
        writer_lock.release()

    resumed = service.poll_once()

    assert resumed.state == "BASELINE_RECORDED"
    assert resumed.error is None
    assert resumed.samples_processed == 3
    assert len(registry.load()) == 1
    assert registry.load()[0].event_type == "BASELINE"
