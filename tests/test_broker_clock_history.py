from __future__ import annotations

import csv
import importlib.util
import os
import sys
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = (
    PROJECT_ROOT
    / "scripts"
    / "diagnostics"
    / "append_broker_clock_history.py"
)

spec = importlib.util.spec_from_file_location(
    "append_broker_clock_history",
    SCRIPT_PATH,
)
history_module = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = history_module
spec.loader.exec_module(history_module)

SOURCE_FIELDS = sorted(history_module.SOURCE_FIELDS)
HISTORY_FIELDS = history_module.HISTORY_FIELDS


def valid_source_row(**overrides):
    row = {
        "cycle_end_local_utc": "2026-11-16T20:44:17.258084+00:00",
        "account_login": "337260380",
        "broker_server": "XMGlobal-MT5 9",
        "symbol": "GOLD",
        "ntp_verified": "true",
        "ntp_checked_at_unix": "1794861856.2580843",
        "ntp_offset_seconds": "0.0721954107",
        "ntp_uncertainty_seconds": "0.0469955205",
        "ntp_sample_count": "3",
        "ntp_age_seconds": "1.0",
        "tick_time_epoch_raw": "1794869056.659",
        "tick_update_status": "UPDATED",
        "tick_corrected_read_midpoint_utc": (
            "2026-11-16T20:44:17.330279+00:00"
        ),
        "tick_minus_corrected_utc_seconds": "7199.3287205",
        "m1_latest_closed_open_raw": "1794868980",
        "m1_update_status": "UPDATED",
        "m1_corrected_read_midpoint_utc": (
            "2026-11-16T20:44:17.330279+00:00"
        ),
        "m1_latest_closed_open_minus_corrected_utc_seconds": (
            "7122.6697206"
        ),
        "m5_latest_closed_open_raw": "1794868500",
        "m5_update_status": "UPDATED",
        "m5_corrected_read_midpoint_utc": (
            "2026-11-16T20:44:17.330279+00:00"
        ),
        "m5_latest_closed_open_minus_corrected_utc_seconds": (
            "6642.6697206"
        ),
        "read_error": "",
    }
    row.update(overrides)
    return row


def write_source_csv(path, rows, fields=None):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(fields or SOURCE_FIELDS),
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def read_history(path):
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def run_archiver(monkeypatch, source, output, capsys):
    monkeypatch.setattr(
        sys,
        "argv",
        [
            str(SCRIPT_PATH),
            "--input",
            str(source),
            "--output",
            str(output),
        ],
    )
    result = history_module.main()
    captured = capsys.readouterr()
    return result, captured.out


def active_source_row(**overrides):
    row = valid_source_row(
        tick_update_status="UPDATED",
        m1_update_status="UNCHANGED",
        m5_update_status="UNCHANGED",
        m1_active_open_raw="1794869040",
        m1_active_read_status="OK",
        m1_active_corrected_read_midpoint_utc=(
            "2026-11-16T20:44:17.330279+00:00"
        ),
        m1_active_open_minus_expected_utc_boundary_seconds="7200.0",
        m5_active_open_raw="1794868800",
        m5_active_read_status="OK",
        m5_active_corrected_read_midpoint_utc=(
            "2026-11-16T20:44:17.330279+00:00"
        ),
        m5_active_open_minus_expected_utc_boundary_seconds="7200.0",
    )
    row.update(overrides)
    return row


def test_active_candles_require_a_fresh_tick_in_same_cycle():
    for tick_status in ("INITIAL", "UNCHANGED", "NO_DATA", "READ_ERROR"):
        row = active_source_row(tick_update_status=tick_status)
        observations, stats = history_module.build_observations(
            [row], "evidence.csv"
        )
        assert not any(
            item["evidence_kind"] in {
                "m1_active_candle", "m5_active_candle"
            }
            for item in observations
        )
        assert stats["skipped_stale_active"] == 2


def test_active_candles_are_archived_when_tick_updated():
    row = active_source_row()
    observations, stats = history_module.build_observations(
        [row], "evidence.csv"
    )
    assert {
        item["evidence_kind"] for item in observations
    } == {"tick", "m1_active_candle", "m5_active_candle"}
    assert stats["skipped_stale_active"] == 0


def test_active_candle_streams_are_independently_validated():
    row = active_source_row(m1_active_read_status="READ_ERROR")
    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )
    assert {
        item["evidence_kind"] for item in observations
    } == {"tick", "m5_active_candle"}


def test_archives_all_three_updated_streams(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    write_source_csv(source, [valid_source_row()])

    result, stdout = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 0
    rows = read_history(output)
    assert len(rows) == 3
    assert {row["evidence_kind"] for row in rows} == {
        "tick",
        "m1_closed_candle",
        "m5_closed_candle",
    }
    assert all(row["account_login"] == "337260380" for row in rows)
    assert "NOT a broker UTC offset" in stdout


def test_accepts_initial_as_baseline(tmp_path):
    row = valid_source_row(
        tick_update_status="INITIAL",
        m1_update_status="INITIAL",
        m5_update_status="INITIAL",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert len(observations) == 3


def test_tick_updated_only_archives_tick(tmp_path):
    row = valid_source_row(
        m1_update_status="UNCHANGED",
        m5_update_status="UNCHANGED",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert len(observations) == 1
    assert observations[0]["evidence_kind"] == "tick"


def test_m1_updated_only_archives_m1():
    row = valid_source_row(
        tick_update_status="UNCHANGED",
        m1_update_status="UPDATED",
        m5_update_status="UNCHANGED",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert len(observations) == 1
    assert observations[0]["evidence_kind"] == "m1_closed_candle"


def test_tick_and_m1_updated_excludes_unchanged_m5():
    row = valid_source_row(
        tick_update_status="UPDATED",
        m1_update_status="UPDATED",
        m5_update_status="UNCHANGED",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert {item["evidence_kind"] for item in observations} == {
        "tick",
        "m1_closed_candle",
    }


def test_all_unchanged_produces_no_observations():
    row = valid_source_row(
        tick_update_status="UNCHANGED",
        m1_update_status="UNCHANGED",
        m5_update_status="UNCHANGED",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert observations == []


def test_no_data_is_not_archived():
    row = valid_source_row(
        tick_update_status="NO_DATA",
        m1_update_status="NO_DATA",
        m5_update_status="NO_DATA",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert observations == []


def test_read_error_on_tick_does_not_discard_m1_and_m5():
    row = valid_source_row(
        tick_update_status="READ_ERROR",
        m1_update_status="UPDATED",
        m5_update_status="UPDATED",
        read_error="tick: RuntimeError: test failure",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert {item["evidence_kind"] for item in observations} == {
        "m1_closed_candle",
        "m5_closed_candle",
    }


def test_global_read_error_does_not_discard_successful_streams():
    row = valid_source_row(
        read_error="m5: RuntimeError: simulated read failure",
        m5_update_status="READ_ERROR",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert {item["evidence_kind"] for item in observations} == {
        "tick",
        "m1_closed_candle",
    }


def test_unknown_status_is_not_archived():
    row = valid_source_row(
        tick_update_status="SOMETHING_NEW",
        m1_update_status="UPDATED",
        m5_update_status="UPDATED",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert len(observations) == 2
    assert all(item["evidence_kind"] != "tick" for item in observations)


def test_skips_unverified_ntp_rows(tmp_path, monkeypatch, capsys):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [
            valid_source_row(),
            valid_source_row(
                cycle_end_local_utc="2026-11-16T20:44:18+00:00",
                ntp_verified="false",
            ),
        ],
    )

    result, _ = run_archiver(monkeypatch, source, output, capsys)

    assert result == 0
    assert len(read_history(output)) == 3


def test_rejects_missing_status_columns(tmp_path, monkeypatch, capsys):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    fields = [
        field for field in SOURCE_FIELDS
        if field != "tick_update_status"
    ]
    row = valid_source_row()
    row.pop("tick_update_status")

    write_source_csv(source, [row], fields=fields)

    result, stdout = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 3
    assert "tick_update_status" in stdout
    assert not output.exists()


def test_rejects_missing_input_file(tmp_path, monkeypatch, capsys):
    source = tmp_path / "missing.csv"
    output = tmp_path / "history.csv"

    result, stdout = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 2
    assert "not found" in stdout.lower()


def test_rejects_invalid_identity():
    row = valid_source_row(account_login="")

    observations, stats = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert observations == []
    assert stats["rejected_identity"] == 1


def test_rejects_invalid_ntp_metadata():
    row = valid_source_row(ntp_offset_seconds="NaN")

    observations, stats = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert observations == []
    assert stats["skipped_untrusted_ntp"] == 1


def test_rejects_negative_ntp_uncertainty():
    row = valid_source_row(ntp_uncertainty_seconds="-1")

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert observations == []


def test_rejects_invalid_observation_timestamp():
    row = valid_source_row(cycle_end_local_utc="not-a-date")

    observations, stats = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert observations == []
    assert stats["rejected_timestamps"] == 1


def test_rejects_timezone_naive_observation_timestamp():
    row = valid_source_row(cycle_end_local_utc="2026-11-16T20:44:17")

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert observations == []


@pytest.mark.parametrize(
    "invalid_value",
    ["", "not-a-number", "NaN", "Infinity", "-Infinity"],
)
def test_invalid_tick_numeric_does_not_discard_other_streams(
    invalid_value,
):
    row = valid_source_row(
        tick_minus_corrected_utc_seconds=invalid_value,
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert {item["evidence_kind"] for item in observations} == {
        "m1_closed_candle",
        "m5_closed_candle",
    }


def test_invalid_comparison_timestamp_rejects_only_its_stream():
    row = valid_source_row(
        tick_corrected_read_midpoint_utc="invalid",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert {item["evidence_kind"] for item in observations} == {
        "m1_closed_candle",
        "m5_closed_candle",
    }


def test_invalid_raw_unix_timestamp_rejects_only_its_stream():
    row = valid_source_row(
        tick_time_epoch_raw="Infinity",
    )

    observations, _ = history_module.build_observations(
        [row], "evidence.csv"
    )

    assert len(observations) == 2
    assert all(item["evidence_kind"] != "tick" for item in observations)


def test_skips_duplicate_rows_inside_same_input(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    row = valid_source_row()

    write_source_csv(source, [row, row])

    result, stdout = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 0
    assert len(read_history(output)) == 3
    assert "Duplicates skipped: 3" in stdout


def test_skips_duplicates_when_input_is_reprocessed(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    write_source_csv(source, [valid_source_row()])

    first_result, _ = run_archiver(
        monkeypatch, source, output, capsys
    )
    second_result, stdout = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert first_result == 0
    assert second_result == 0
    assert len(read_history(output)) == 3
    assert "Duplicates skipped: 3" in stdout


def test_preserves_old_rows_when_adding_new_session(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    write_source_csv(source, [valid_source_row()])

    result, _ = run_archiver(
        monkeypatch, source, output, capsys
    )
    assert result == 0

    second = valid_source_row(
        cycle_end_local_utc="2026-11-17T20:44:17+00:00",
        tick_time_epoch_raw="1794955456.659",
        m1_latest_closed_open_raw="1794955380",
        m5_latest_closed_open_raw="1794954900",
        ntp_checked_at_unix="1794948256.2580843",
        ntp_age_seconds="1.0",
    )
    write_source_csv(source, [second])

    result, _ = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 0
    assert len(read_history(output)) == 6


def test_rejects_history_schema_mismatch(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    write_source_csv(source, [valid_source_row()])
    output.write_text(
        "wrong_column\nold_value\n",
        encoding="utf-8",
    )

    result, stdout = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 6
    assert "schema mismatch" in stdout.lower()


def test_history_uses_existing_schema(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    write_source_csv(source, [valid_source_row()])

    result, _ = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 0

    with output.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames == HISTORY_FIELDS


def test_observations_are_not_marked_verified(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    write_source_csv(source, [valid_source_row()])

    result, stdout = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 0
    rows = read_history(output)

    assert all("verified" not in row for row in rows)
    assert "NOT a broker UTC offset" in stdout


def test_lock_blocks_second_writer(tmp_path):
    lock_path = tmp_path / "history.csv.lock"

    with history_module.exclusive_file_lock(lock_path):
        assert lock_path.exists()

        with pytest.raises(FileExistsError):
            descriptor = os.open(
                str(lock_path),
                os.O_CREAT | os.O_EXCL | os.O_WRONLY,
            )
            os.close(descriptor)


def test_lock_timeout_is_reported(tmp_path, monkeypatch):
    lock_path = tmp_path / "history.csv.lock"
    lock_path.write_text("held by another process", encoding="utf-8")

    monkeypatch.setattr(
        history_module, "LOCK_TIMEOUT_SECONDS", 0.01
    )
    monkeypatch.setattr(
        history_module, "LOCK_RETRY_SECONDS", 0.001
    )

    with pytest.raises(TimeoutError):
        with history_module.exclusive_file_lock(lock_path):
            pass


def test_history_keeps_existing_rows_unchanged(
    tmp_path, monkeypatch, capsys
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"
    write_source_csv(source, [valid_source_row()])

    result, _ = run_archiver(
        monkeypatch, source, output, capsys
    )
    assert result == 0

    original_rows = read_history(output)

    second = valid_source_row(
        cycle_end_local_utc="2026-11-17T20:44:17+00:00",
        tick_time_epoch_raw="1794955456.659",
        m1_latest_closed_open_raw="1794955380",
        m5_latest_closed_open_raw="1794954900",
        ntp_checked_at_unix="1794948256.2580843",
        ntp_age_seconds="1.0",
    )
    write_source_csv(source, [second])

    result, _ = run_archiver(
        monkeypatch, source, output, capsys
    )

    assert result == 0
    final_rows = read_history(output)
    assert final_rows[:3] == original_rows
    assert len(final_rows) == 6
@pytest.mark.parametrize(
    "reported_age, expected",
    [
        ("-0.0000005", True),
        ("-0.0000009", True),
        ("-0.000001", True),
        ("-0.0000011", False),
        ("-0.000002", False),
        ("0", True),
        ("1.5", True),
        ("21601", False),
    ],
)
def test_ntp_metadata_age_precision_tolerance(
    reported_age, expected
):
    row = valid_source_row(ntp_age_seconds=reported_age)

    assert (
        history_module.valid_ntp_metadata(row)
        is expected
    )


@pytest.mark.parametrize(
    "age_difference, expected",
    [
        (-0.0000005, True),
        (-0.0000009, True),
        (-0.000001, True),
        (-0.0000011, False),
        (-0.000002, False),
        (0.0, True),
        (1.0, True),
        (21601.0, False),
    ],
)
def test_ntp_observation_age_precision_tolerance(
    age_difference, expected
):
    observed_at = "2026-11-16T20:44:17.258084+00:00"
    observed_epoch = history_module.iso_epoch(observed_at)

    row = valid_source_row(
        ntp_checked_at_unix=str(
            observed_epoch - age_difference
        ),
        ntp_age_seconds="0",
    )

    assert (
        history_module.valid_ntp_at_observation(
            row, observed_at
        )
        is expected
    )



def test_lock_recovers_file_owned_by_dead_process(tmp_path, monkeypatch):
    lock_path = tmp_path / "history.csv.lock"
    lock_path.write_text("pid=424242\\n", encoding="ascii")

    def dead_process(pid):
        assert pid == 424242
        return False

    monkeypatch.setattr(
        history_module, "_process_is_running", dead_process
    )

    with history_module.exclusive_file_lock(lock_path):
        assert lock_path.read_text(encoding="ascii") == (
            f"pid={os.getpid()}\\n"
        )

    assert not lock_path.exists()


def test_lock_does_not_recover_file_owned_by_running_process(
    tmp_path, monkeypatch
):
    lock_path = tmp_path / "history.csv.lock"
    lock_path.write_text("pid=424242\\n", encoding="ascii")

    monkeypatch.setattr(
        history_module, "_process_is_running", lambda pid: True
    )
    monkeypatch.setattr(
        history_module, "LOCK_TIMEOUT_SECONDS", 0.01
    )
    monkeypatch.setattr(
        history_module, "LOCK_RETRY_SECONDS", 0.001
    )

    with pytest.raises(TimeoutError):
        with history_module.exclusive_file_lock(lock_path):
            pass

    assert lock_path.read_text(encoding="ascii") == "pid=424242\\n"


def test_lock_does_not_recover_unreadable_or_unknown_metadata(
    tmp_path, monkeypatch
):
    lock_path = tmp_path / "history.csv.lock"
    lock_path.write_text("unknown owner metadata", encoding="ascii")
    monkeypatch.setattr(
        history_module, "LOCK_TIMEOUT_SECONDS", 0.01
    )
    monkeypatch.setattr(
        history_module, "LOCK_RETRY_SECONDS", 0.001
    )

    with pytest.raises(TimeoutError):
        with history_module.exclusive_file_lock(lock_path):
            pass

    assert lock_path.read_text(encoding="ascii") == (
        "unknown owner metadata"
    )
