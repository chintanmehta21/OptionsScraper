"""Core loop: iterate expiries, fetch raw candles, track progress."""

import os
import sys
import time
import logging
from datetime import datetime, timezone, timedelta, date as dt_date

from DhanHQ_src.auth import get_access_token
from DhanHQ_src.fetcher import (
    create_dhan_client,
    fetch_with_retry,
    parse_api_response,
    build_raw_rows,
)
from DhanHQ_src.config import (
    NIFTY_SECURITY_ID,
    EXCHANGE_SEGMENT,
    INSTRUMENT_TYPE,
    REQUIRED_DATA,
    API_DELAY_SECONDS,
)
from DhanHQ_src.loop_expiries.config import (
    LOOP_STRIKES,
    LOOP_OPTION_TYPES,
)
from DhanHQ_src.loop_expiries.expiry_fetcher import fetch_all_expiry_dates
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_CI = os.environ.get("GITHUB_ACTIONS") == "true"
_TOKEN_MAX_AGE_S = 20 * 3600


def _fmt_duration(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def _progress_bar(current: int, total: int, width: int = 30) -> str:
    filled = int(width * current / total) if total > 0 else 0
    return "\u2588" * filled + "\u2591" * (width - filled)


def _print_progress(i: int, total: int, exp_date: str, exp_flag: str,
                     status: str, rows: int, dur: float, eta_s: float):
    """Print a single tqdm-style progress line (overwrites previous on TTY)."""
    pct = i / total * 100 if total > 0 else 0
    bar = _progress_bar(i, total)
    eta = _fmt_duration(eta_s) if eta_s > 0 else "--"
    flag = "W" if exp_flag == "WEEK" else "M"

    if status == "skipped":
        detail = "SKIP"
    elif status == "failed":
        detail = "FAIL"
    else:
        detail = f"{rows:,}r"

    line = f"  {pct:5.1f}% {bar} {i}/{total} | {exp_date} {flag} | {detail} | {_fmt_duration(dur)} | ETA {eta}"

    if _CI:
        # GitHub Actions: plain lines (no \r)
        print(line, flush=True)
    else:
        # TTY: overwrite line
        print(f"\r{line}", end="", flush=True)


def scrape_single_expiry(dhan, expiry: dict):
    """Fetch all strikes x option types for one expiry.

    Returns (rows, api_calls, empty_count).
    """
    expiry_date = expiry["expiry_date"]
    expiry_flag = expiry["expiry_flag"]
    from_date = expiry["from_date"]
    to_date = expiry["to_date"]

    strike_offsets = {s: i - len(LOOP_STRIKES) // 2 for i, s in enumerate(LOOP_STRIKES)}

    all_rows = []
    api_calls = 0
    empty_count = 0

    for strike in LOOP_STRIKES:
        for option_type in LOOP_OPTION_TYPES:
            api_calls += 1
            response = fetch_with_retry(
                dhan,
                security_id=NIFTY_SECURITY_ID,
                exchange_segment=EXCHANGE_SEGMENT,
                instrument_type=INSTRUMENT_TYPE,
                expiry_flag=expiry_flag,
                expiry_code=1,
                strike=strike,
                drv_option_type=option_type,
                required_data=REQUIRED_DATA,
                from_date=from_date,
                to_date=to_date,
            )
            parsed = parse_api_response(response)
            if not parsed:
                empty_count += 1
            else:
                rows = build_raw_rows(parsed, option_type, strike_offsets[strike], expiry_date)
                all_rows.extend(rows)

            time.sleep(API_DELAY_SECONDS)

    return all_rows, api_calls, empty_count


def run_loop(year: int, reset: bool = False) -> dict:
    """Main loop: scrape all expiries for a year with resume support."""
    logging.basicConfig(
        level=logging.WARNING if _CI else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # ── Step 1: Authenticate ─────────────────────────────────────
    token = get_access_token()
    token_time = time.time()
    dhan = create_dhan_client(token)

    # ── Step 2: Fetch actual expiry dates from NSE Bhavcopy ─────
    expiries = fetch_all_expiry_dates(year)

    if not expiries:
        print(f"ERROR: No expiry dates found for {year}. Cannot proceed.", flush=True)
        return {"completed": 0, "failed": 0, "skipped": 0, "total_rows": 0}

    total = len(expiries)
    weeks = sum(1 for e in expiries if e["expiry_flag"] == "WEEK")
    months = sum(1 for e in expiries if e["expiry_flag"] == "MONTH")

    # ── Step 3: Setup DB and seed progress ───────────────────────
    db = LoopExpiriesDB(year)
    db.setup_tables()

    if reset:
        db.reset_progress()

    db.seed_progress(expiries)

    # ── Step 4: Scrape pending expiries ──────────────────────────
    pending = db.get_pending_expiries()
    already_done = total - len(pending)

    print(f"  Loop Expiries — {year} | {total} expiries ({weeks}W + {months}M) | {len(pending)} pending", flush=True)

    start_time = time.time()
    stats = {"completed": 0, "failed": 0, "skipped": 0, "total_rows": 0}

    for idx, progress_row in enumerate(pending, already_done + 1):
        exp_date = progress_row["expiry_date"]
        exp_flag = progress_row["expiry_flag"]
        exp_dt = dt_date.fromisoformat(exp_date)
        expiry = {
            "expiry_date": exp_date,
            "expiry_flag": exp_flag,
            "from_date": (exp_dt - timedelta(days=14)).isoformat(),
            "to_date": exp_date,
        }

        if time.time() - token_time > _TOKEN_MAX_AGE_S:
            token = get_access_token()
            token_time = time.time()
            dhan = create_dhan_client(token)

        db.update_progress(
            exp_date, exp_flag,
            status="in_progress",
            started_at=datetime.now(IST).isoformat(),
        )

        expiry_start = time.time()
        try:
            rows, api_calls, empty_count = scrape_single_expiry(dhan, expiry)
            now_str = datetime.now(IST).isoformat()
            expiry_dur = time.time() - expiry_start

            elapsed = time.time() - start_time
            done_in_session = idx - already_done
            avg = elapsed / done_in_session if done_in_session > 0 else 0
            remaining = total - idx
            eta_s = avg * remaining

            if empty_count == api_calls:
                db.update_progress(
                    exp_date, exp_flag,
                    status="skipped",
                    api_calls_made=api_calls,
                    completed_at=now_str,
                )
                stats["skipped"] += 1
                _print_progress(idx, total, exp_date, exp_flag, "skipped", 0, expiry_dur, eta_s)
            else:
                db.insert_candles(rows, exp_flag)
                db.update_progress(
                    exp_date, exp_flag,
                    status="completed",
                    rows_inserted=len(rows),
                    api_calls_made=api_calls,
                    completed_at=now_str,
                )
                stats["completed"] += 1
                stats["total_rows"] += len(rows)
                _print_progress(idx, total, exp_date, exp_flag, "ok", len(rows), expiry_dur, eta_s)

        except Exception as e:
            expiry_dur = time.time() - expiry_start
            db.update_progress(
                exp_date, exp_flag,
                status="failed",
                error_message=str(e)[:500],
                completed_at=datetime.now(IST).isoformat(),
            )
            stats["failed"] += 1
            elapsed = time.time() - start_time
            done_in_session = idx - already_done
            avg = elapsed / done_in_session if done_in_session > 0 else 0
            eta_s = avg * (total - idx)
            _print_progress(idx, total, exp_date, exp_flag, "failed", 0, expiry_dur, eta_s)
            logger.error("FAILED %s %s: %s", exp_date, exp_flag, e)

    if not _CI:
        print()  # Newline after last \r progress line

    elapsed_total = time.time() - start_time
    print(
        f"  DONE: {stats['completed']}ok {stats['failed']}fail {stats['skipped']}skip | "
        f"{stats['total_rows']:,} rows | {_fmt_duration(elapsed_total)}",
        flush=True,
    )

    _write_job_summary(year, db, elapsed_total)

    return stats


def _write_job_summary(year: int, db: LoopExpiriesDB, elapsed: float):
    """Write markdown summary to $GITHUB_STEP_SUMMARY."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    s = db.get_progress_summary()
    lines = [
        f"## Loop Expiries Scraper — {year}",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| Completed | {s['completed']}/{s['total']} |",
        f"| Failed | {s['failed']} |",
        f"| Skipped | {s['skipped']} |",
        f"| Pending | {s['pending']} |",
        f"| Total Rows | {s['total_rows']:,} |",
        f"| Duration | {elapsed / 60:.1f}m |",
        "",
    ]

    if s["failed_details"]:
        lines.extend(["### Failed Expiries", ""])
        for f in s["failed_details"]:
            lines.append(f"- **{f['expiry_date']} {f['expiry_flag']}**: {f['error_message']}")
        lines.append("")

    try:
        with open(summary_path, "a") as fh:
            fh.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.debug("Could not write job summary: %s", e)
