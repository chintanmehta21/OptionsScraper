"""Update the SENSEX year parquet with missing and incomplete expiries.

Mirrors DhanHQ_src/loop_expiries/update_2026_parquet.py with SENSEX
paths and symbol_cfg. Algorithm and resume semantics are identical:
  1. Load existing parquet (or staging if a prior run crashed).
  2. Build set of (expiry_date, expiry_flag) already present and
     per-key actual_end_date from the stored rows.
  3. Fetch all SENSEX expiries for the target year from BSE Bhavcopy.
  4. Classify into missing / refresh / done / future.
  5. For each refresh expiry, drop existing rows then re-scrape.
  6. For each missing expiry, scrape and append.
  7. After every successful expiry, flush staging parquet (crash-safe).
  8. On completion, copy staging -> GDrive and clean up staging.

Usage:
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet --dry-run
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet --year 2026
    python -m DhanHQ_src.Sensex_Scraping.update_2026_parquet --no-refresh
"""

import argparse
import logging
import os
import shutil
import sys
import time
from datetime import date, timezone, timedelta

if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Optional: pick up DhanHQ creds from project-local .env when invoked
# without env vars pre-set in the shell. Same pattern as NIFTY updater.
try:
    from dotenv import load_dotenv

    _env_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    )
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
except ImportError:
    pass

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
)

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_TOKEN_MAX_AGE_S = 20 * 3600
_SCRAPE_LOOKBACK_DAYS = 14
STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging")


def _parquet_path(year: int) -> str:
    return os.path.join(GDRIVE_BASE, PARQUET_FILENAME_TEMPLATE.format(year=year))


def _staging_path(year: int) -> str:
    base = os.path.abspath(STAGING_DIR)
    return os.path.join(
        base, PARQUET_FILENAME_TEMPLATE.format(year=year).replace(".parquet", "_update.parquet")
    )


def _print_progress(i, total, exp_date, exp_flag, mode, status, rows, dur, eta_s):
    pct = i / total * 100 if total > 0 else 0
    bar = _progress_bar(i, total)
    eta = _fmt_duration(eta_s) if eta_s > 0 else "--"
    flag = "W" if exp_flag == "WEEK" else "M"
    tag = "NEW" if mode == "missing" else "RFR"
    if status == "skipped":
        detail = "SKIP"
    elif status == "failed":
        detail = "FAIL"
    elif status == "no-change":
        detail = "SAME"
    else:
        detail = f"{rows:,}r"
    line = (
        f"  {pct:5.1f}% {bar} {i}/{total} | {exp_date} {flag} {tag} | "
        f"{detail} | {_fmt_duration(dur)} | ETA {eta}"
    )
    print(f"\r{line}", end="", flush=True)


def _classify_expiries(all_expiries, present_keys, today, actual_end_by_key=None):
    """Same logic as NIFTY's updater — split into missing / refresh / done / future."""
    missing, refresh, done, future = [], [], [], []
    window = timedelta(days=_SCRAPE_LOOKBACK_DAYS)
    actual_end_by_key = actual_end_by_key or {}
    for e in all_expiries:
        key = (e["expiry_date"], e["expiry_flag"])
        exp_dt = date.fromisoformat(e["expiry_date"])
        if key in present_keys:
            expected_end = min(exp_dt, today)
            actual_end_str = actual_end_by_key.get(key)
            actual_end = (
                date.fromisoformat(actual_end_str) if actual_end_str else None
            )
            if exp_dt >= today or (
                actual_end is not None and actual_end < expected_end
            ):
                refresh.append(e)
            else:
                done.append(e)
        else:
            if exp_dt - window <= today:
                missing.append(e)
            else:
                future.append(e)
    return missing, refresh, done, future


def update_parquet(year: int, dry_run: bool = False, no_refresh: bool = False):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    parquet_path = _parquet_path(year)
    staging_path = _staging_path(year)
    os.makedirs(os.path.dirname(staging_path), exist_ok=True)

    if not os.path.exists(parquet_path):
        print(f"ERROR: {parquet_path} does not exist. Run run_parquet.py first.")
        sys.exit(1)

    if os.path.exists(staging_path):
        print(f"  Resuming from existing staging file: {staging_path}", flush=True)
        source_path = staging_path
        staging_is_fresh = False
    else:
        source_path = parquet_path
        staging_is_fresh = True

    print(f"  Loading existing data from: {source_path}", flush=True)
    existing_df = pd.read_parquet(source_path)

    present_keys = set(
        zip(
            existing_df["expiry_date"].astype(str),
            existing_df["expiry_flag"].astype(str),
        )
    )
    _end_series = (
        existing_df.assign(
            _k=list(
                zip(
                    existing_df["expiry_date"].astype(str),
                    existing_df["expiry_flag"].astype(str),
                )
            ),
            _d=existing_df["date"].astype(str),
        )
        .groupby("_k")["_d"]
        .max()
    )
    actual_end_by_key = _end_series.to_dict()

    latest_date = existing_df["expiry_date"].astype(str).max()
    print(
        f"  Existing: {len(existing_df):,} rows | {len(present_keys)} (date,flag) combos "
        f"| latest expiry: {latest_date}",
        flush=True,
    )

    print(f"  Fetching SENSEX expiry dates for {year} from BSE Bhavcopy...", flush=True)
    all_expiries = fetch_all_expiry_dates(year)
    if not all_expiries:
        print(f"ERROR: No SENSEX expiry dates found for {year}.")
        sys.exit(1)

    today = date.today()
    missing, refresh, done, future = _classify_expiries(
        all_expiries, present_keys, today, actual_end_by_key=actual_end_by_key
    )
    if no_refresh:
        refresh = []

    print(
        f"  Classification: {len(missing)} missing, {len(refresh)} refresh, "
        f"{len(done)} done, {len(future)} future (window not open)",
        flush=True,
    )

    work = [("refresh", e) for e in refresh] + [("missing", e) for e in missing]

    if not work:
        print("  Parquet already up to date -- nothing to scrape.", flush=True)
        if os.path.exists(staging_path) and not staging_is_fresh:
            os.remove(staging_path)
        return

    total = len(work)
    w_miss = sum(1 for e in missing if e["expiry_flag"] == "WEEK")
    m_miss = sum(1 for e in missing if e["expiry_flag"] == "MONTH")
    w_ref = sum(1 for e in refresh if e["expiry_flag"] == "WEEK")
    m_ref = sum(1 for e in refresh if e["expiry_flag"] == "MONTH")
    print(
        f"  {total} expiries to process "
        f"(NEW: {w_miss}W+{m_miss}M | RFR: {w_ref}W+{m_ref}M):",
        flush=True,
    )
    for mode, e in work:
        tag = "NEW" if mode == "missing" else "RFR"
        print(f"    - {e['expiry_date']} {e['expiry_flag']} [{tag}]", flush=True)

    if dry_run:
        print("  Dry-run mode -- exiting without scraping.", flush=True)
        return

    if staging_is_fresh:
        print(f"  Copying baseline parquet to staging: {staging_path}", flush=True)
        shutil.copy2(parquet_path, staging_path)

    token = get_access_token()
    token_time = time.time()
    dhan = create_dhan_client(token)

    combined_df = existing_df
    stats = {
        "new_ok": 0,
        "refresh_ok": 0,
        "refresh_unchanged": 0,
        "failed": 0,
        "skipped": 0,
        "rows_added": 0,
        "rows_refreshed_delta": 0,
    }
    start_time = time.time()

    for idx, (mode, expiry) in enumerate(work, 1):
        exp_date = expiry["expiry_date"]
        exp_flag = expiry["expiry_flag"]

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

            elapsed = time.time() - start_time
            avg = elapsed / max(idx, 1)
            eta_s = avg * (total - idx)

            if empty_count == api_calls:
                stats["skipped"] += 1
                _print_progress(idx, total, exp_date, exp_flag, mode, "skipped", 0, dur, eta_s)
            else:
                for r in rows:
                    r["expiry_flag"] = exp_flag
                new_df = pd.DataFrame(rows)

                if mode == "refresh":
                    mask = ~(
                        (combined_df["expiry_date"].astype(str) == exp_date)
                        & (combined_df["expiry_flag"].astype(str) == exp_flag)
                    )
                    old_count = (~mask).sum()
                    combined_df = combined_df[mask].reset_index(drop=True)
                    combined_df = pd.concat(
                        [combined_df, new_df], ignore_index=True
                    )
                    delta = len(new_df) - int(old_count)
                    if delta == 0:
                        stats["refresh_unchanged"] += 1
                        status = "no-change"
                    else:
                        stats["refresh_ok"] += 1
                        stats["rows_refreshed_delta"] += delta
                        status = "ok"
                    combined_df.to_parquet(staging_path, index=False, engine="pyarrow")
                    _print_progress(
                        idx, total, exp_date, exp_flag, mode, status,
                        len(new_df), dur, eta_s,
                    )
                else:  # missing
                    combined_df = pd.concat(
                        [combined_df, new_df], ignore_index=True
                    )
                    combined_df.to_parquet(staging_path, index=False, engine="pyarrow")
                    stats["new_ok"] += 1
                    stats["rows_added"] += len(new_df)
                    _print_progress(
                        idx, total, exp_date, exp_flag, mode, "ok",
                        len(new_df), dur, eta_s,
                    )

        except Exception as e:
            dur = time.time() - expiry_start
            elapsed = time.time() - start_time
            avg = elapsed / max(idx, 1)
            eta_s = avg * (total - idx)
            stats["failed"] += 1
            _print_progress(idx, total, exp_date, exp_flag, mode, "failed", 0, dur, eta_s)
            logger.error("FAILED %s %s: %s", exp_date, exp_flag, e)

    print()
    elapsed_total = time.time() - start_time

    any_change = (
        stats["new_ok"] > 0
        or stats["refresh_ok"] > 0
        or stats["rows_refreshed_delta"] != 0
    )
    if not any_change:
        print("  No changes to parquet (all skipped / unchanged).", flush=True)
        if staging_is_fresh and os.path.exists(staging_path):
            os.remove(staging_path)
        return

    print(f"  Copying updated staging -> {parquet_path}", flush=True)
    shutil.copy2(staging_path, parquet_path)

    size_mb = os.path.getsize(parquet_path) / 1024 / 1024
    final_df = pd.read_parquet(parquet_path)

    print(
        f"  DONE: NEW {stats['new_ok']}ok (+{stats['rows_added']:,} rows) | "
        f"RFR {stats['refresh_ok']}ok {stats['refresh_unchanged']}same "
        f"(delta {stats['rows_refreshed_delta']:+,} rows) | "
        f"{stats['failed']}fail {stats['skipped']}skip | "
        f"{_fmt_duration(elapsed_total)}"
    )
    print(
        f"  Output: {parquet_path} ({size_mb:.1f} MB, {len(final_df):,} total rows)"
    )
    print(
        f"  Unique expiry dates: {final_df['expiry_date'].nunique()} | "
        f"Latest: {final_df['expiry_date'].astype(str).max()}"
    )

    os.remove(staging_path)
    print("  Staging file cleaned up.", flush=True)


def main():
    parser = argparse.ArgumentParser(
        description="Update SENSEX year parquet with missing expiries (default: 2026)"
    )
    parser.add_argument(
        "--year", type=int, default=2026, help="Year to update (default: 2026)"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="List work items without scraping"
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Skip re-scraping in-parquet future expiries (only add missing ones)",
    )
    args = parser.parse_args()
    update_parquet(args.year, dry_run=args.dry_run, no_refresh=args.no_refresh)


if __name__ == "__main__":
    main()
