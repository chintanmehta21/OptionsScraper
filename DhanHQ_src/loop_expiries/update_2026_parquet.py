"""Update the 2026 parquet with missing and incomplete expiries.

Algorithm:
  1. Load the existing 2026 parquet from GDrive (or local staging if a prior
     update crashed mid-run -- automatic resume).
  2. Build the set of (expiry_date, expiry_flag) combos already present.
  3. Fetch all 2026 expiry dates from NSE Bhavcopy.
  4. Classify each bhavcopy expiry into one of four buckets:
       - DONE     : already in parquet AND expiry_date < today AND the parquet
                    already holds data through expiry_date -> skip
       - REFRESH  : already in parquet AND (expiry_date >= today OR the last
                    trade date for this (date,flag) in the parquet is BEFORE
                    min(expiry_date, today)). Covers both active contracts and
                    past contracts that were scraped before they expired and
                    therefore have a stale tail of missing days.
       - MISSING  : not in parquet, 14-day scrape window has opened -> scrape
       - FUTURE   : not in parquet, scrape window hasn't opened -> skip
  5. For each REFRESH expiry, drop its existing rows from the in-memory
     dataframe, then re-scrape and append. REFRESH replacements only apply
     if the fresh scrape is non-empty -- on skip/fail the old rows stay.
  6. For each MISSING expiry, scrape and append.
  7. After every successful expiry, the combined dataframe is flushed to a
     local staging parquet (safe against crashes).
  8. On completion, copy staging -> GDrive and clean up staging.

Usage:
    python -m DhanHQ_src.loop_expiries.update_2026_parquet
    python -m DhanHQ_src.loop_expiries.update_2026_parquet --dry-run
    python -m DhanHQ_src.loop_expiries.update_2026_parquet --year 2026
    python -m DhanHQ_src.loop_expiries.update_2026_parquet --no-refresh
"""

import argparse
import logging
import os
import shutil
import sys
import time
from datetime import date, timezone, timedelta

# Force UTF-8 stdout on Windows to handle progress bar Unicode chars
if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

# Load project-local .env so DhanHQ credentials are picked up when the script
# is invoked without them being pre-set in the shell.
try:
    from dotenv import load_dotenv

    _env_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    )
    if os.path.exists(_env_path):
        load_dotenv(_env_path)
except ImportError:
    # python-dotenv not installed; assume env vars are set externally.
    pass

from DhanHQ_src.auth import get_access_token
from DhanHQ_src.fetcher import create_dhan_client
from DhanHQ_src.loop_expiries.expiry_fetcher import fetch_all_expiry_dates
from DhanHQ_src.loop_expiries.scraper import (
    scrape_single_expiry,
    _fmt_duration,
    _progress_bar,
)
from DhanHQ_src.loop_expiries.config import NIFTY_SYMBOL_CFG

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_TOKEN_MAX_AGE_S = 20 * 3600
_SCRAPE_LOOKBACK_DAYS = 14  # matches from_date window in expiry_fetcher
GDRIVE_BASE = "G:/My Drive/OptionsData/NIFTY"
STAGING_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "data", "staging")


def _parquet_path(year: int) -> str:
    return f"{GDRIVE_BASE}/nifty_options_{year}.parquet"


def _staging_path(year: int) -> str:
    base = os.path.abspath(STAGING_DIR)
    return os.path.join(base, f"nifty_options_{year}_update.parquet")


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
    """Split bhavcopy expiries into (missing, refresh, done, future) buckets.

    An in-parquet expiry is classified as REFRESH if EITHER:
      - expiry_date >= today (active contract — data may still be growing), OR
      - expected_end > actual_end_in_parquet, where
            expected_end = min(expiry_date, today)
        (past-but-incomplete — was scraped before the expiry happened).

    ``actual_end_by_key`` is a dict mapping (expiry_date, expiry_flag) ->
    latest trade ``date`` in parquet (as an ISO string). If omitted, the
    incomplete-past check is disabled and only the active-contract branch is
    used.
    """
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
            # Active contract OR past-but-incomplete -> REFRESH
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

    # Prefer staging if it exists (resume a prior interrupted update).
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
    # Per-expiry actual_end = latest trade date present for that (date,flag).
    # Used to detect past expiries that were scraped before they expired and
    # therefore have a stale tail of missing days.
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

    # Fetch all expiries for the target year from NSE Bhavcopy.
    print(f"  Fetching expiry dates for {year} from NSE Bhavcopy...", flush=True)
    all_expiries = fetch_all_expiry_dates(year)
    if not all_expiries:
        print(f"ERROR: No expiry dates found for {year}.")
        sys.exit(1)

    # Classify each bhavcopy expiry into missing / refresh / done / future.
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

    # Build work list: refresh first (so their old rows are replaced before
    # we start appending new ones), then missing.
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

    # Seed the staging file from GDrive if we don't already have one.
    if staging_is_fresh:
        print(f"  Copying baseline parquet to staging: {staging_path}", flush=True)
        shutil.copy2(parquet_path, staging_path)

    # Authenticate.
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

        # Refresh stale token.
        if time.time() - token_time > _TOKEN_MAX_AGE_S:
            token = get_access_token()
            token_time = time.time()
            dhan = create_dhan_client(token)

        expiry_start = time.time()
        try:
            rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry, NIFTY_SYMBOL_CFG)
            dur = time.time() - expiry_start

            elapsed = time.time() - start_time
            avg = elapsed / max(idx, 1)
            eta_s = avg * (total - idx)

            if empty_count == api_calls:
                # Empty scrape: skip for MISSING, leave RFR rows untouched.
                stats["skipped"] += 1
                _print_progress(
                    idx, total, exp_date, exp_flag, mode, "skipped", 0, dur, eta_s
                )
            else:
                for r in rows:
                    r["expiry_flag"] = exp_flag
                new_df = pd.DataFrame(rows)

                if mode == "refresh":
                    # Drop old rows for this (date, flag) before appending.
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
                    # Flush to staging immediately for crash safety.
                    combined_df.to_parquet(
                        staging_path, index=False, engine="pyarrow"
                    )
                    _print_progress(
                        idx, total, exp_date, exp_flag, mode, status,
                        len(new_df), dur, eta_s,
                    )
                else:  # missing
                    combined_df = pd.concat(
                        [combined_df, new_df], ignore_index=True
                    )
                    combined_df.to_parquet(
                        staging_path, index=False, engine="pyarrow"
                    )
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
            _print_progress(
                idx, total, exp_date, exp_flag, mode, "failed", 0, dur, eta_s
            )
            logger.error("FAILED %s %s: %s", exp_date, exp_flag, e)

    print()  # Newline after progress
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

    # Copy updated parquet back to GDrive.
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
        description="Update year parquet with missing expiries (default: 2026)"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2026,
        help="Year to update (default: 2026)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List work items without scraping",
    )
    parser.add_argument(
        "--no-refresh",
        action="store_true",
        help="Skip re-scraping of in-parquet future expiries (only add missing ones)",
    )
    args = parser.parse_args()
    update_parquet(args.year, dry_run=args.dry_run, no_refresh=args.no_refresh)


if __name__ == "__main__":
    main()
