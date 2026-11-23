
import csv

import pytest

from backend.market_data.clock_history import (
    CLOCK_HISTORY_COLUMNS,
    ClockHistoryError,
    parse_clock_history_row,
    read_clock_history,
)


def make_row(**overrides):
    row = {
        "schema_version": "1",
        "run_id": "test-run-001",
        "sample_sequence": "1",
        "monotonic_ms": "1000",
        "time_gmt_epoch": "1794952458",
        "time_trade_server_epoch": "1794959658",
        "time_current_quote_epoch": "1794959658",
        "time_local_epoch": "1794963258",
        "server_utc_offset_seconds": "7200",
        "clock_values_valid": "true",
        "account_login": "337260380",
        "broker_server": "XMGlobal-MT5 9",
        "symbol": "GOLD",
        "terminal_build": "6316",
        "terminal_connected": "true",
    }
    row.update(overrides)
    return row


def write_history(path, rows):
    with path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=CLOCK_HISTORY_COLUMNS,
            delimiter=";",
        )
        writer.writeheader()
        writer.writerows(rows)


def test_parse_valid_clock_history_row():
    sample = parse_clock_history_row(make_row())

    assert sample.run_id == "test-run-001"
    assert sample.sample_sequence == 1
    assert sample.server_utc_offset_seconds == 7200
    assert sample.identity == (
        337260380,
        "XMGlobal-MT5 9",
        "GOLD",
        6316,
    )


def test_reject_unsupported_schema_version():
    with pytest.raises(ClockHistoryError):
        parse_clock_history_row(
            make_row(schema_version="2")
        )


def test_reject_invalid_clock_values_flag():
    with pytest.raises(ClockHistoryError):
        parse_clock_history_row(
            make_row(clock_values_valid="false")
        )


def test_reject_offset_mismatch():
    with pytest.raises(ClockHistoryError):
        parse_clock_history_row(
            make_row(server_utc_offset_seconds="3600")
        )


def test_reject_missing_broker_identity():
    with pytest.raises(ClockHistoryError):
        parse_clock_history_row(
            make_row(broker_server="")
        )


def test_read_two_sequential_samples(tmp_path):
    first = make_row()

    second = make_row(
        sample_sequence="2",
        monotonic_ms="61000",
        time_gmt_epoch="1794952518",
        time_trade_server_epoch="1794959718",
        time_current_quote_epoch="1794959718",
        time_local_epoch="1794963318",
    )

    path = tmp_path / "clock_history.csv"
    write_history(path, [first, second])

    samples = read_clock_history(path)

    assert len(samples) == 2
    assert samples[0].sample_sequence == 1
    assert samples[1].sample_sequence == 2


def test_reject_sequence_gap(tmp_path):
    first = make_row()

    second = make_row(
        sample_sequence="3",
        monotonic_ms="61000",
        time_gmt_epoch="1794952518",
        time_trade_server_epoch="1794959718",
        time_current_quote_epoch="1794959718",
        time_local_epoch="1794963318",
    )

    path = tmp_path / "clock_history.csv"
    write_history(path, [first, second])

    with pytest.raises(ClockHistoryError):
        read_clock_history(path)


def test_reject_identity_change_inside_same_run(tmp_path):
    first = make_row()

    second = make_row(
        sample_sequence="2",
        monotonic_ms="61000",
        account_login="123456789",
        time_gmt_epoch="1794952518",
        time_trade_server_epoch="1794959718",
        time_current_quote_epoch="1794959718",
        time_local_epoch="1794963318",
    )

    path = tmp_path / "clock_history.csv"
    write_history(path, [first, second])

    with pytest.raises(ClockHistoryError):
        read_clock_history(path)


def test_accept_monotonic_counter_wraparound(tmp_path):
    first = make_row(
        monotonic_ms="4294967290",
    )

    second = make_row(
        sample_sequence="2",
        monotonic_ms="20",
        time_gmt_epoch="1794952518",
        time_trade_server_epoch="1794959718",
        time_current_quote_epoch="1794959718",
        time_local_epoch="1794963318",
    )

    path = tmp_path / "clock_history.csv"
    write_history(path, [first, second])

    samples = read_clock_history(path)

    assert len(samples) == 2


def test_reject_wrong_csv_header(tmp_path):
    path = tmp_path / "clock_history.csv"
    path.write_text(
        "wrong;header\nvalue;value\n",
        encoding="utf-8",
    )

    with pytest.raises(ClockHistoryError):
        read_clock_history(path)


def test_reject_empty_history(tmp_path):
    path = tmp_path / "clock_history.csv"
    write_history(path, [])

    with pytest.raises(ClockHistoryError):
        read_clock_history(path)



@pytest.mark.parametrize(
    "field,value",
    [
        ("run_id", None),
        ("run_id", 123),
        ("broker_server", None),
        ("symbol", []),
    ],
)
def test_reject_non_text_identity_fields(field, value):
    with pytest.raises(ClockHistoryError):
        parse_clock_history_row(make_row(**{field: value}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("clock_values_valid", None),
        ("clock_values_valid", 1),
        ("terminal_connected", None),
        ("terminal_connected", []),
    ],
)
def test_reject_non_text_boolean_fields(field, value):
    with pytest.raises(ClockHistoryError):
        parse_clock_history_row(make_row(**{field: value}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("schema_version", None),
        ("sample_sequence", "0"),
        ("sample_sequence", "-1"),
        ("monotonic_ms", "-1"),
        ("monotonic_ms", "4294967296"),
        ("time_current_quote_epoch", "-1"),
    ],
)
def test_reject_invalid_clock_history_numeric_boundaries(field, value):
    with pytest.raises(ClockHistoryError):
        parse_clock_history_row(make_row(**{field: value}))


@pytest.mark.parametrize("offset", [50400, -50400])
def test_accept_supported_broker_offset_boundaries(offset):
    row = make_row(
        server_utc_offset_seconds=str(offset),
        time_trade_server_epoch=str(1794952458 + offset),
    )

    sample = parse_clock_history_row(row)

    assert sample.server_utc_offset_seconds == offset


@pytest.mark.parametrize("offset", [50401, -50401])
def test_reject_broker_offset_outside_supported_boundaries(offset):
    row = make_row(
        server_utc_offset_seconds=str(offset),
        time_trade_server_epoch=str(1794952458 + offset),
    )

    with pytest.raises(ClockHistoryError, match="supported range"):
        parse_clock_history_row(row)


def test_reject_csv_row_with_missing_boolean_field(tmp_path):
    path = tmp_path / "clock_history.csv"
    path.write_text(
        ";".join(CLOCK_HISTORY_COLUMNS)
        + chr(10)
        + ";".join(make_row()[column] for column in CLOCK_HISTORY_COLUMNS[:9])
        + chr(10),
        encoding="utf-8",
    )

    with pytest.raises(ClockHistoryError):
        read_clock_history(path)


def test_accept_interleaved_independent_runs(tmp_path):
    first_run_first = make_row(run_id="run-a")
    second_run_first = make_row(
        run_id="run-b",
        sample_sequence="1",
        monotonic_ms="2000",
    )
    first_run_second = make_row(
        run_id="run-a",
        sample_sequence="2",
        monotonic_ms="61000",
        time_gmt_epoch="1794952518",
        time_trade_server_epoch="1794959718",
        time_current_quote_epoch="1794959718",
        time_local_epoch="1794963318",
    )

    path = tmp_path / "clock_history.csv"
    write_history(
        path,
        [first_run_first, second_run_first, first_run_second],
    )

    samples = read_clock_history(path)

    assert [sample.run_id for sample in samples] == [
        "run-a",
        "run-b",
        "run-a",
    ]
