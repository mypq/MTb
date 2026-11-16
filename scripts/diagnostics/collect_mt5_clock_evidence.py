
from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from time import perf_counter_ns

from backend.config import get_settings
from backend.market_data.ntp_time import (
    NtpTimeVerifier,
    NtpVerification,
)
from backend.mt5 import MT5Adapter, MT5ConnectionState


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Read-only MT5 clock and candle evidence collector."
    )
    parser.add_argument("--symbol", default=None)
    parser.add_argument("--duration-seconds", type=int, default=900)
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--ntp-interval-seconds", type=int, default=60)
    parser.add_argument("--output", default=None)

    args = parser.parse_args()
    if args.duration_seconds < 60:
        parser.error("--duration-seconds must be at least 60.")
    if not 0.2 <= args.interval_seconds <= 30:
        parser.error("--interval-seconds must be between 0.2 and 30.")
    if args.ntp_interval_seconds < 30:
        parser.error("--ntp-interval-seconds must be at least 30.")

    return args


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def json_epochs(rates) -> str:
    if rates is None:
        return "[]"
    return json.dumps(
        [int(rate["time"]) for rate in rates],
        separators=(",", ":"),
    )


def timed_call(function):
    start_utc = utc_now()
    start_mono = perf_counter_ns()

    result = function()

    end_mono = perf_counter_ns()
    end_utc = utc_now()

    return {
        "result": result,
        "start_utc": start_utc,
        "end_utc": end_utc,
        "start_mono_ns": start_mono,
        "end_mono_ns": end_mono,
        "duration_seconds": (
            end_mono - start_mono
        ) / 1_000_000_000,
    }


def corrected_epoch(local_epoch, ntp_result):
    if (
        local_epoch is None
        or ntp_result is None
        or not ntp_result.verified
        or ntp_result.offset_seconds is None
    ):
        return None

    return float(local_epoch) + float(ntp_result.offset_seconds)


def track_update(
    previous_value,
    current_value,
    now_monotonic,
    previous_change_monotonic,
):
    if current_value is None:
        return "NO_DATA", previous_change_monotonic, ""

    if previous_value is None:
        return "INITIAL", now_monotonic, 0.0

    if current_value != previous_value:
        return "UPDATED", now_monotonic, 0.0

    changed_at = (
        previous_change_monotonic
        if previous_change_monotonic is not None
        else now_monotonic
    )

    return (
        "UNCHANGED",
        changed_at,
        max(0.0, now_monotonic - changed_at),
    )


def resolve_ntp_state(
    verifier: NtpTimeVerifier,
    attempt_result: NtpVerification | None,
    attempt_error: str | None = None,
) -> tuple[NtpVerification | None, bool, str]:
    """
    Return the last successful NTP verification while it remains trusted.

    A failed refresh must not replace a still-valid previous verification.
    The latest refresh error is returned separately for CSV diagnostics.
    """
    if attempt_result is not None and attempt_result.verified:
        candidate = attempt_result
    else:
        candidate = verifier.last_verification()

    trusted = (
        candidate is not None
        and candidate.verified
        and verifier.is_trusted()
    )

    if not trusted:
        candidate = None

    error = attempt_error or (
        attempt_result.error
        if attempt_result is not None and not attempt_result.verified
        else ""
    )

    return candidate, trusted, error or ""


def read_stream(function):
    """
    Read one MT5 stream independently.

    Returns:
        call: timed call result, or None
        value: returned data, or None
        status: INITIAL/UPDATED/UNCHANGED is assigned later;
                NO_DATA for an empty result;
                READ_ERROR for an exception.
        error: error description, if any.
    """
    try:
        call = timed_call(function)
        value = call["result"]

        if value is None:
            return call, None, "NO_DATA", ""

        return call, value, "", ""

    except Exception as exc:
        return (
            None,
            None,
            "READ_ERROR",
            f"{type(exc).__name__}: {exc}",
        )


def apply_read_timing(row, prefix, call):
    if call is None:
        return

    row[f"{prefix}_read_start_local_utc"] = (
        call["start_utc"].isoformat()
    )
    row[f"{prefix}_read_end_local_utc"] = (
        call["end_utc"].isoformat()
    )
    row[f"{prefix}_read_start_monotonic_ns"] = (
        call["start_mono_ns"]
    )
    row[f"{prefix}_read_end_monotonic_ns"] = (
        call["end_mono_ns"]
    )
    row[f"{prefix}_read_duration_seconds"] = (
        call["duration_seconds"]
    )


def apply_stream_tracking(
    row,
    prefix,
    current_value,
    previous_value,
    previous_change_monotonic,
    now_monotonic,
    read_status="",
):
    if read_status == "READ_ERROR":
        row[f"{prefix}_update_status"] = "READ_ERROR"
        row[f"{prefix}_unchanged_seconds"] = ""
        return previous_value, previous_change_monotonic

    if read_status == "NO_DATA":
        row[f"{prefix}_update_status"] = "NO_DATA"
        row[f"{prefix}_unchanged_seconds"] = ""
        return previous_value, previous_change_monotonic

    status, changed_at, unchanged_seconds = track_update(
        previous_value=previous_value,
        current_value=current_value,
        now_monotonic=now_monotonic,
        previous_change_monotonic=previous_change_monotonic,
    )

    row[f"{prefix}_update_status"] = status
    row[f"{prefix}_unchanged_seconds"] = unchanged_seconds

    return current_value, changed_at


def main() -> int:
    args = parse_args()
    settings = get_settings()
    symbol = (args.symbol or settings.MT5_SYMBOL or "GOLD").strip()

    if not symbol:
        print("ERROR: MT5 symbol is empty.", file=sys.stderr)
        return 2

    output = Path(
        args.output
        or (
            "diagnostics/mt5_clock_evidence_"
            + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            + ".csv"
        )
    )
    output.parent.mkdir(parents=True, exist_ok=True)

    adapter = MT5Adapter(settings=settings)
    status = adapter.connect()

    if status.state != MT5ConnectionState.CONNECTED:
        print(
            f"ERROR: MT5 connection failed: {status.message}",
            file=sys.stderr,
        )
        return 3

    try:
        import MetaTrader5 as mt5
    except ImportError:
        adapter.shutdown()
        print(
            "ERROR: MetaTrader5 package is not installed.",
            file=sys.stderr,
        )
        return 4

    if not mt5.symbol_select(symbol, True):
        adapter.shutdown()
        print(
            f"ERROR: Cannot select {symbol}: {mt5.last_error()}",
            file=sys.stderr,
        )
        return 5

    verifier = NtpTimeVerifier()
    last_ntp_check = 0.0
    started_mono = time.monotonic()

    previous_tick = None
    previous_m1 = None
    previous_m5 = None

    tick_last_change = None
    m1_last_change = None
    m5_last_change = None

    fields = [
        "cycle_start_local_utc",
        "cycle_end_local_utc",
        "cycle_start_monotonic_ns",
        "cycle_end_monotonic_ns",
        "capture_elapsed_seconds",
        "account_login",
        "broker_server",
        "symbol",
        "ntp_verified",
        "ntp_checked_at_unix",
        "ntp_age_seconds",
        "ntp_offset_seconds",
        "ntp_uncertainty_seconds",
        "ntp_sample_count",
        "ntp_servers",
        "ntp_error",
        "tick_read_start_local_utc",
        "tick_read_end_local_utc",
        "tick_read_start_monotonic_ns",
        "tick_read_end_monotonic_ns",
        "tick_read_duration_seconds",
        "tick_time_msc_raw",
        "tick_time_epoch_raw",
        "tick_update_status",
        "tick_unchanged_seconds",
        "tick_corrected_read_midpoint_utc",
        "tick_minus_corrected_utc_seconds",
        "m1_read_start_local_utc",
        "m1_read_end_local_utc",
        "m1_read_start_monotonic_ns",
        "m1_read_end_monotonic_ns",
        "m1_read_duration_seconds",
        "m1_closed_open_epochs_raw",
        "m1_latest_closed_open_raw",
        "m1_update_status",
        "m1_unchanged_seconds",
        "m1_corrected_read_midpoint_utc",
        "m1_latest_closed_open_minus_corrected_utc_seconds",
        "m1_predicted_active_open_minus_corrected_utc_seconds",
        "m5_read_start_local_utc",
        "m5_read_end_local_utc",
        "m5_read_start_monotonic_ns",
        "m5_read_end_monotonic_ns",
        "m5_read_duration_seconds",
        "m5_closed_open_epochs_raw",
        "m5_latest_closed_open_raw",
        "m5_update_status",
        "m5_unchanged_seconds",
        "m5_corrected_read_midpoint_utc",
        "m5_latest_closed_open_minus_corrected_utc_seconds",
        "m5_predicted_active_open_minus_corrected_utc_seconds",
        "read_error",
    ]

    print(f"Read-only collection started: symbol={symbol}")
    print(f"Duration: {args.duration_seconds}s; output: {output}")
    print("NTP correction is recorded separately from raw local time.")
    print("Market update tracking records changes only; it does not classify stale data.")

    try:
        with output.open(
            "w",
            newline="",
            encoding="utf-8",
        ) as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fields,
            )
            writer.writeheader()

            while (
                time.monotonic() - started_mono
                < args.duration_seconds
            ):
                loop_started = time.monotonic()
                cycle_start_utc = utc_now()
                cycle_start_mono = perf_counter_ns()

                row = {field: "" for field in fields}

                row.update({
                    "cycle_start_local_utc": cycle_start_utc.isoformat(),
                    "cycle_start_monotonic_ns": cycle_start_mono,
                    "symbol": symbol,
                    "ntp_verified": False,
                    "m1_closed_open_epochs_raw": "[]",
                    "m5_closed_open_epochs_raw": "[]",
                    "read_error": "",
                })

                errors = []

                # NTP verification
                ntp_attempt = None
                ntp_attempt_error = ""

                if (
                    loop_started - last_ntp_check
                    >= args.ntp_interval_seconds
                ):
                    try:
                        ntp_attempt = verifier.verify()
                    except Exception as exc:
                        ntp_attempt_error = (
                            f"{type(exc).__name__}: {exc}"
                        )

                    last_ntp_check = loop_started

                ntp_result, ntp_trusted, ntp_error = resolve_ntp_state(
                    verifier,
                    ntp_attempt,
                    ntp_attempt_error,
                )

                row["ntp_verified"] = ntp_trusted
                row["ntp_error"] = ntp_error

                if ntp_result is not None:
                    row["ntp_checked_at_unix"] = (
                        ntp_result.checked_at_unix or ""
                    )
                    row["ntp_offset_seconds"] = (
                        ntp_result.offset_seconds
                        if ntp_result.offset_seconds is not None
                        else ""
                    )
                    row["ntp_uncertainty_seconds"] = (
                        ntp_result.uncertainty_seconds
                        if ntp_result.uncertainty_seconds is not None
                        else ""
                    )
                    row["ntp_sample_count"] = ntp_result.sample_count
                    row["ntp_servers"] = ";".join(ntp_result.servers)

                # Account information
                try:
                    account_call = timed_call(
                        lambda: mt5.account_info()
                    )
                    account = account_call["result"]

                    if account is None:
                        errors.append(
                            f"account_info: {mt5.last_error()}"
                        )
                    else:
                        row["account_login"] = int(account.login)
                        row["broker_server"] = str(account.server)

                except Exception as exc:
                    errors.append(
                        f"account_info: {type(exc).__name__}: {exc}"
                    )

                # Tick read
                tick_call, tick, tick_status, tick_error = read_stream(
                    lambda: mt5.symbol_info_tick(symbol)
                )

                apply_read_timing(row, "tick", tick_call)

                if tick_error:
                    errors.append(f"tick: {tick_error}")

                tick_value = None

                if tick is not None:
                    tick_value = int(tick.time_msc)
                    row["tick_time_msc_raw"] = tick_value
                    row["tick_time_epoch_raw"] = (
                        tick_value / 1000.0
                    )

                tracking_now = time.monotonic()

                previous_tick, tick_last_change = apply_stream_tracking(
                    row=row,
                    prefix="tick",
                    current_value=tick_value,
                    previous_value=previous_tick,
                    previous_change_monotonic=tick_last_change,
                    now_monotonic=tracking_now,
                    read_status=tick_status,
                )

                # M1 read
                m1_call, rates_m1, m1_status, m1_error = read_stream(
                    lambda: mt5.copy_rates_from_pos(
                        symbol, mt5.TIMEFRAME_M1, 1, 3
                    )
                )

                apply_read_timing(row, "m1", m1_call)

                if m1_error:
                    errors.append(f"m1: {m1_error}")

                m1_latest = None

                if rates_m1 is not None:
                    row["m1_closed_open_epochs_raw"] = (
                        json_epochs(rates_m1)
                    )

                    m1_epochs = [
                        int(rate["time"]) for rate in rates_m1
                    ]

                    if m1_epochs:
                        m1_latest = max(m1_epochs)
                        row["m1_latest_closed_open_raw"] = m1_latest
                    else:
                        m1_status = "NO_DATA"

                tracking_now = time.monotonic()

                previous_m1, m1_last_change = apply_stream_tracking(
                    row=row,
                    prefix="m1",
                    current_value=m1_latest,
                    previous_value=previous_m1,
                    previous_change_monotonic=m1_last_change,
                    now_monotonic=tracking_now,
                    read_status=m1_status,
                )

                # M5 read
                m5_call, rates_m5, m5_status, m5_error = read_stream(
                    lambda: mt5.copy_rates_from_pos(
                        symbol, mt5.TIMEFRAME_M5, 1, 3
                    )
                )

                apply_read_timing(row, "m5", m5_call)

                if m5_error:
                    errors.append(f"m5: {m5_error}")

                m5_latest = None

                if rates_m5 is not None:
                    row["m5_closed_open_epochs_raw"] = (
                        json_epochs(rates_m5)
                    )

                    m5_epochs = [
                        int(rate["time"]) for rate in rates_m5
                    ]

                    if m5_epochs:
                        m5_latest = max(m5_epochs)
                        row["m5_latest_closed_open_raw"] = m5_latest
                    else:
                        m5_status = "NO_DATA"

                tracking_now = time.monotonic()

                previous_m5, m5_last_change = apply_stream_tracking(
                    row=row,
                    prefix="m5",
                    current_value=m5_latest,
                    previous_value=previous_m5,
                    previous_change_monotonic=m5_last_change,
                    now_monotonic=tracking_now,
                    read_status=m5_status,
                )

                # Time correction calculations remain independent
                # from market update tracking.
                if ntp_trusted and ntp_result is not None:
                    if tick_call is not None and tick is not None:
                        tick_mid_local = (
                            tick_call["start_utc"].timestamp()
                            + tick_call["end_utc"].timestamp()
                        ) / 2.0

                        tick_mid_corrected = corrected_epoch(
                            tick_mid_local, ntp_result
                        )

                        if tick_mid_corrected is not None:
                            row["tick_corrected_read_midpoint_utc"] = (
                                datetime.fromtimestamp(
                                    tick_mid_corrected,
                                    tz=timezone.utc,
                                ).isoformat()
                            )
                            row["tick_minus_corrected_utc_seconds"] = (
                                row["tick_time_epoch_raw"]
                                - tick_mid_corrected
                            )

                    if (
                        m1_call is not None
                        and rates_m1 is not None
                        and m1_latest is not None
                    ):
                        m1_mid_local = (
                            m1_call["start_utc"].timestamp()
                            + m1_call["end_utc"].timestamp()
                        ) / 2.0

                        m1_mid_corrected = corrected_epoch(
                            m1_mid_local, ntp_result
                        )

                        if m1_mid_corrected is not None:
                            row["m1_corrected_read_midpoint_utc"] = (
                                datetime.fromtimestamp(
                                    m1_mid_corrected,
                                    tz=timezone.utc,
                                ).isoformat()
                            )
                            row[
                                "m1_latest_closed_open_minus_corrected_utc_seconds"
                            ] = m1_latest - m1_mid_corrected
                            row[
                                "m1_predicted_active_open_minus_corrected_utc_seconds"
                            ] = m1_latest + 60 - m1_mid_corrected

                    if (
                        m5_call is not None
                        and rates_m5 is not None
                        and m5_latest is not None
                    ):
                        m5_mid_local = (
                            m5_call["start_utc"].timestamp()
                            + m5_call["end_utc"].timestamp()
                        ) / 2.0

                        m5_mid_corrected = corrected_epoch(
                            m5_mid_local, ntp_result
                        )

                        if m5_mid_corrected is not None:
                            row["m5_corrected_read_midpoint_utc"] = (
                                datetime.fromtimestamp(
                                    m5_mid_corrected,
                                    tz=timezone.utc,
                                ).isoformat()
                            )
                            row[
                                "m5_latest_closed_open_minus_corrected_utc_seconds"
                            ] = m5_latest - m5_mid_corrected
                            row[
                                "m5_predicted_active_open_minus_corrected_utc_seconds"
                            ] = m5_latest + 300 - m5_mid_corrected

                row["read_error"] = " | ".join(errors)

                cycle_end_mono = perf_counter_ns()
                cycle_end_utc = utc_now()

                row["cycle_end_local_utc"] = (
                    cycle_end_utc.isoformat()
                )
                row["cycle_end_monotonic_ns"] = cycle_end_mono
                row["capture_elapsed_seconds"] = (
                    time.monotonic() - started_mono
                )

                if (
                    ntp_trusted
                    and ntp_result is not None
                    and ntp_result.checked_at_unix is not None
                ):
                    row["ntp_age_seconds"] = (
                        cycle_end_utc.timestamp()
                        - ntp_result.checked_at_unix
                    )

                writer.writerow(row)
                handle.flush()

                remaining = args.interval_seconds - (
                    time.monotonic() - loop_started
                )

                if remaining > 0:
                    time.sleep(remaining)

    except KeyboardInterrupt:
        print("Collection interrupted by user.")

    finally:
        adapter.shutdown()

    print(
        f"Collection finished. Evidence saved to: {output.resolve()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
