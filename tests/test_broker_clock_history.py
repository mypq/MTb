
from __future__ import annotations

import csv
import importlib.util
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
        "ntp_checked_at_unix": "1794861857.2580843",
        "ntp_offset_seconds": "0.0721954107",
        "ntp_uncertainty_seconds": "0.0469955205",
        "tick_time_epoch_raw": "1794869056.659",
        "tick_corrected_read_midpoint_utc": (
            "2026-11-16T20:44:17.330279+00:00"
        ),
        "tick_minus_corrected_utc_seconds": "7199.3287205",
        "m1_latest_closed_open_raw": "1794868980",
        "m1_corrected_read_midpoint_utc": (
            "2026-11-16T20:44:17.330279+00:00"
        ),
        "m1_latest_closed_open_minus_corrected_utc_seconds": (
            "7122.6697206"
        ),
        "m5_latest_closed_open_raw": "1794868500",
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
    fields = fields or SOURCE_FIELDS

    with path.open(
        "w",
        newline="",
        encoding="utf-8",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=fields,
        )
        writer.writeheader()
        writer.writerows(rows)


def read_history(path):
    with path.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as handle:
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


def test_archives_tick_m1_and_m5(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [valid_source_row()],
    )

    result, stdout = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0

    rows = read_history(output)

    assert len(rows) == 3
    assert {
        row["evidence_kind"] for row in rows
    } == {
        "tick",
        "m1_closed_candle",
        "m5_closed_candle",
    }

    assert all(
        row["account_login"] == "337260380"
        for row in rows
    )

    assert all(
        row["broker_server"] == "XMGlobal-MT5 9"
        for row in rows
    )

    assert "not verified offsets" in stdout


def test_skips_unverified_ntp_rows(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [
            valid_source_row(),
            valid_source_row(
                cycle_end_local_utc=(
                    "2026-11-16T20:44:18+00:00"
                ),
                ntp_verified="false",
            ),
        ],
    )

    result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0
    assert len(read_history(output)) == 3


def test_skips_rows_with_read_errors(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [
            valid_source_row(),
            valid_source_row(
                cycle_end_local_utc=(
                    "2026-11-16T20:44:18+00:00"
                ),
                read_error="MT5 read failed",
            ),
        ],
    )

    result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0
    assert len(read_history(output)) == 3


def test_rejects_missing_source_columns(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    fields = [
        field
        for field in SOURCE_FIELDS
        if field != "ntp_verified"
    ]

    write_source_csv(
        source,
        [valid_source_row()],
        fields=fields,
    )

    result, stdout = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 3
    assert "missing" in stdout.lower()
    assert not output.exists()


def test_rejects_missing_input_file(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "missing.csv"
    output = tmp_path / "history.csv"

    result, stdout = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 2
    assert "not found" in stdout.lower()


def test_skips_duplicate_rows_inside_same_input(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    row = valid_source_row()

    write_source_csv(
        source,
        [row, row],
    )

    result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0

    rows = read_history(output)

    assert len(rows) == 3


def test_skips_duplicates_when_input_is_reprocessed(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [valid_source_row()],
    )

    first_result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    second_result, stdout = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert first_result == 0
    assert second_result == 0
    assert len(read_history(output)) == 3
    assert "Duplicates skipped: 3" in stdout


def test_preserves_old_rows_when_adding_new_session(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    first = valid_source_row()

    write_source_csv(
        source,
        [first],
    )

    result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0

    second = valid_source_row(
        cycle_end_local_utc=(
            "2026-11-17T20:44:17+00:00"
        ),
        tick_time_epoch_raw="1794955456.659",
        m1_latest_closed_open_raw="1794955380",
        m5_latest_closed_open_raw="1794954900",
    )

    write_source_csv(
        source,
        [second],
    )

    result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0
    assert len(read_history(output)) == 6


def test_rejects_history_schema_mismatch(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [valid_source_row()],
    )

    output.write_text(
        "wrong_column\nold_value\n",
        encoding="utf-8",
    )

    result, stdout = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 6
    assert "schema mismatch" in stdout.lower()


@pytest.mark.parametrize(
    "invalid_value",
    [
        "",
        "not-a-number",
        "NaN",
        "Infinity",
        "-Infinity",
    ],
)
def test_skips_invalid_numeric_observations(
    tmp_path,
    monkeypatch,
    capsys,
    invalid_value,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    row = valid_source_row(
        tick_minus_corrected_utc_seconds=invalid_value,
    )

    write_source_csv(
        source,
        [row],
    )

    result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0

    rows = read_history(output)

    assert len(rows) == 2
    assert {
        row["evidence_kind"] for row in rows
    } == {
        "m1_closed_candle",
        "m5_closed_candle",
    }


def test_rejects_invalid_ntp_metadata(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [
            valid_source_row(
                ntp_offset_seconds="NaN",
            )
        ],
    )

    result, stdout = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 5
    assert "no complete" in stdout.lower()
    assert not output.exists()


def test_history_uses_expected_schema(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [valid_source_row()],
    )

    result, _ = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0

    with output.open(
        "r",
        newline="",
        encoding="utf-8",
    ) as handle:
        reader = csv.DictReader(handle)

        assert reader.fieldnames == HISTORY_FIELDS


def test_observations_are_not_marked_verified(
    tmp_path,
    monkeypatch,
    capsys,
):
    source = tmp_path / "evidence.csv"
    output = tmp_path / "history.csv"

    write_source_csv(
        source,
        [valid_source_row()],
    )

    result, stdout = run_archiver(
        monkeypatch,
        source,
        output,
        capsys,
    )

    assert result == 0

    rows = read_history(output)

    assert all(
        "verified" not in row
        for row in rows
    )

    assert "not verified offsets" in stdout
