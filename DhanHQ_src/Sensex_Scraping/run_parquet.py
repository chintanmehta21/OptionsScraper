"""Run SENSEX loop expiries for a year, output to a single parquet file.

Resilient design (mirrors NIFTY's loop_expiries/run_parquet.py):
  - Saves progress to a local staging parquet after EVERY expiry
  - Tracks completed expiries in a JSON sidecar for resume support
  - Final output copied to GDrive only after all expiries are done
  - If the process dies, re-run the same command to resume

Usage:
    python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025
    python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025 --reset
    python -m DhanHQ_src.Sensex_Scraping.run_parquet --year 2025 --test
"""

import argparse
import json
import logging
import os
import shutil
import sys
import time
from datetime import datetime, timezone, timedelta

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DhanHQ_src.auth import get_access_token
from DhanHQ_src.fetcher import create_dhan_client
from DhanHQ_src.loop_expiries.scraper import (
    scrape_single_expiry,
    _fmt_duration,
    _progress_bar,
)
from DhanHQ_src.Sensex_Scraping.expiry_fetcher import fetch_all_expiry_dates
from DhanHQ_src.Sensex_Scraping.config import (
    SENSEX_SYMBOL_CFG,
    GDRIVE_BASE,
    PARQUET_FILENAME_TEMPLATE,
    PROGRESS_FILENAME_TEMPLATE,
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_TOKEN_MAX_AGE_S = 20 * 3600
STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging")


def _staging_paths(year: int):
    base = os.path.abspath(STAGING_DIR)
    return (
        os.path.join(base, PARQUET_FILENAME_TEMPLATE.format(year=year)),
        os.path.join(base, PROGRESS_FILENAME_TEMPLATE.format(year=year)),
    )


def _load_progress(progress_path: str) -> dict:
    if os.path.exists(progress_path):
        with open(progress_path, "r") as f:
            return json.load(f)
    return {
        "completed_keys": [],
        "stats": {"completed": 0, "failed": 0, "skipped": 0, "total_rows": 0},
    }


def _save_progress(progress_path: str, progress: dict):
    with open(progress_path, "w") as f:
        json.dump(progress, f)


def _append_to_parquet(staging_parquet: str, rows: list[dict]):
    new_df = pd.DataFrame(rows)
    if os.path.exists(staging_parquet):
        existing_df = pd.read_parquet(staging_parquet)
        combined = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined = new_df
    combined.to_parquet(staging_parquet, index=False, engine="pyarrow")


def _print_progress(i, total, exp_date, exp_flag, status, rows, dur, eta_s):
    pct = i / total * 100 if total > 0 else 0
    bar = _progress_bar(i, total)
    eta = _fmt_duration(eta_s) if eta_s > 0 else "--"
    flag = "W" if exp_flag == "WEEK" else "M"
    if status == "skipped":
        detail = "SKIP"
    elif status == "failed":
        detail = "FAIL"
    elif status == "resumed":
        detail = "DONE"
    else:
        detail = f"{rows:,}r"
    line = (
        f"  {pct:5.1f}% {bar} {i}/{total} | {exp_date} {flag} | "
        f"{detail} | {_fmt_duration(dur)} | ETA {eta}"
    )
    print(f"\r{line}", end="", flush=True)


def run_to_parquet(year: int, output_path: str, test_mode: bool = False, reset: bool = False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    staging_parquet, progress_path = _staging_paths(year)
    os.makedirs(os.path.dirname(staging_parquet), exist_ok=True)

    if reset:
        for f in (staging_parquet, progress_path):
            if os.path.exists(f):
                os.remove(f)
        print("  Reset: cleared staging data.", flush=True)

    token = get_access_token()
    token_time = time.time()
    dhan = create_dhan_client(token)

    expiries = fetch_all_expiry_dates(year)
    if not expiries:
        print(f"ERROR: No SENSEX expiry dates found for {year}.")
        sys.exit(1)

    if test_mode:
        expiries = expiries[:2]
        print("  TEST MODE: scraping first 2 expiries only", flush=True)

    total = len(expiries)
    weeks = sum(1 for e in expiries if e["expiry_flag"] == "WEEK")
    months = sum(1 for e in expiries if e["expiry_flag"] == "MONTH")

    progress = _load_progress(progress_path)
    done_keys = set(tuple(k) for k in progress["completed_keys"])
    stats = progress["stats"]
    resumed_count = len(done_keys)

    if resumed_count > 0:
        print(
            f"  Resuming: {resumed_count} expiries already done, "
            f"{total - resumed_count} remaining",
            flush=True,
        )

    print(
        f"  SENSEX Loop Expiries -> Parquet -- {year} | "
        f"{total} expiries ({weeks}W + {months}M)",
        flush=True,
    )

    start_time = time.time()

    for idx, expiry in enumerate(expiries, 1):
        exp_date = expiry["expiry_date"]
        exp_flag = expiry["expiry_flag"]
        key = (exp_date, exp_flag)

        if key in done_keys:
            _print_progress(idx, total, exp_date, exp_flag, "resumed", 0, 0, 0)
            continue

        if time.time() - token_time > _TOKEN_MAX_AGE_S:
            token = get_access_token()
            token_time = time.time()
            dhan = create_dhan_client(token)

        expiry_start = time.time()
        try:
            rows, api_calls, empty_count = scrape_single_expiry(
                dhan, expiry, SENSEX_SYMBOL_CFG
            )
            dur = time.time() - expiry_start

            pending = total - idx
            done_in_session = idx - resumed_count
            elapsed = time.time() - start_time
            avg = elapsed / max(done_in_session, 1)
            eta_s = avg * pending

            if empty_count == api_calls:
                stats["skipped"] += 1
                _print_progress(idx, total, exp_date, exp_flag, "skipped", 0, dur, eta_s)
            else:
                for r in rows:
                    r["expiry_flag"] = exp_flag
                _append_to_parquet(staging_parquet, rows)
                stats["completed"] += 1
                stats["total_rows"] += len(rows)
                _print_progress(idx, total, exp_date, exp_flag, "ok", len(rows), dur, eta_s)

            done_keys.add(key)
            progress["completed_keys"] = [list(k) for k in done_keys]
            progress["stats"] = stats
            _save_progress(progress_path, progress)

        except Exception as e:
            dur = time.time() - expiry_start
            pending = total - idx
            done_in_session = idx - resumed_count
            elapsed = time.time() - start_time
            avg = elapsed / max(done_in_session, 1)
            eta_s = avg * pending
            stats["failed"] += 1
            _print_progress(idx, total, exp_date, exp_flag, "failed", 0, dur, eta_s)
            logger.error("FAILED %s %s: %s", exp_date, exp_flag, e)
            progress["stats"] = stats
            _save_progress(progress_path, progress)

    print()
    elapsed_total = time.time() - start_time

    if not os.path.exists(staging_parquet):
        print("No data collected -- nothing to write.")
        sys.exit(1)

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    shutil.copy2(staging_parquet, output_path)

    size_mb = os.path.getsize(output_path) / 1024 / 1024
    df = pd.read_parquet(output_path)

    print(
        f"  DONE: {stats['completed']}ok {stats['failed']}fail {stats['skipped']}skip | "
        f"{stats['total_rows']:,} rows | {_fmt_duration(elapsed_total)}"
    )
    print(f"  Output: {output_path} ({size_mb:.1f} MB)")
    print(
        f"  Unique expiry dates: {df['expiry_date'].nunique()} | "
        f"Columns: {list(df.columns)}"
    )

    os.remove(staging_parquet)
    os.remove(progress_path)
    print("  Staging files cleaned up.", flush=True)


def main():
    parser = argparse.ArgumentParser(description="SENSEX Loop Expiries -> Parquet")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument(
        "--output",
        type=str,
        default=None,
        help=f"Output path (default: {GDRIVE_BASE}/sensex_options_{{year}}.parquet)",
    )
    parser.add_argument(
        "--test", action="store_true", help="Test mode: scrape first 2 expiries only"
    )
    parser.add_argument(
        "--reset", action="store_true", help="Wipe staging progress and start fresh"
    )
    args = parser.parse_args()

    output = args.output or os.path.join(
        GDRIVE_BASE, PARQUET_FILENAME_TEMPLATE.format(year=args.year)
    )
    run_to_parquet(args.year, output, test_mode=args.test, reset=args.reset)


if __name__ == "__main__":
    main()
