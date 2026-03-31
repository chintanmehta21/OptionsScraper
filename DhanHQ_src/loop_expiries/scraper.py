"""Core loop: iterate expiries, fetch raw candles, track progress."""

import os
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
    generate_expiry_dates,
)
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
_CI = os.environ.get("GITHUB_ACTIONS") == "true"
_TOKEN_MAX_AGE_S = 20 * 3600


def _fmt_duration(seconds: float) -> str:
    """Format seconds into human-readable duration like '1m32s' or '45s'."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    if m < 60:
        return f"{m}m{s:02d}s"
    h, m = divmod(m, 60)
    return f"{h}h{m:02d}m"


def _progress_bar(current: int, total: int, width: int = 20) -> str:
    """Render a text progress bar like '████████░░░░░░░░░░░░'."""
    filled = int(width * current / total) if total > 0 else 0
    return "\u2588" * filled + "\u2591" * (width - filled)


def _group(title):
    if _CI:
        print(f"::group::{title}", flush=True)
    logger.info(title)


def _endgroup():
    if _CI:
        print("::endgroup::", flush=True)


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
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    db = LoopExpiriesDB(year)
    db.setup_tables()

    expiries = generate_expiry_dates(year)
    total = len(expiries)
    logger.info("=" * 60)
    logger.info("Loop Expiries Scraper — year %d (%d expiries)", year, total)
    logger.info("=" * 60)

    if reset:
        db.reset_progress()

    db.seed_progress(expiries)

    token = get_access_token()
    token_time = time.time()
    dhan = create_dhan_client(token)

    pending = db.get_pending_expiries()
    already_done = total - len(pending)
    logger.info("Resuming: %d done, %d to process", already_done, len(pending))

    start_time = time.time()
    stats = {"completed": 0, "failed": 0, "skipped": 0, "total_rows": 0}

    for i, progress_row in enumerate(pending, already_done + 1):
        exp_date = progress_row["expiry_date"]
        exp_flag = progress_row["expiry_flag"]
        # Reconstruct from_date/to_date (progress table only stores PK fields)
        exp_dt = dt_date.fromisoformat(exp_date)
        expiry = {
            "expiry_date": exp_date,
            "expiry_flag": exp_flag,
            "from_date": (exp_dt - timedelta(days=14)).isoformat(),
            "to_date": exp_date,
        }
        label = f"[{i}/{total}] {i / total * 100:.1f}% | {exp_date} {exp_flag}"

        if time.time() - token_time > _TOKEN_MAX_AGE_S:
            logger.info("Refreshing access token (>20h old)")
            token = get_access_token()
            token_time = time.time()
            dhan = create_dhan_client(token)

        _group(label)
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

            if empty_count == api_calls:
                db.update_progress(
                    exp_date, exp_flag,
                    status="skipped",
                    api_calls_made=api_calls,
                    completed_at=now_str,
                )
                stats["skipped"] += 1
                logger.info("  SKIPPED (holiday) | took %s", _fmt_duration(expiry_dur))
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

                elapsed = time.time() - start_time
                done_in_session = i - already_done
                avg = elapsed / done_in_session if done_in_session > 0 else 0
                remaining = total - i
                eta = avg * remaining / 60

                bar = _progress_bar(i, total, width=20)
                logger.info(
                    "  %s %s | %d rows | took %s | ETA: %s (%d left)",
                    label, bar, len(rows),
                    _fmt_duration(expiry_dur), _fmt_duration(eta * 60),
                    remaining,
                )

        except Exception as e:
            expiry_dur = time.time() - expiry_start
            db.update_progress(
                exp_date, exp_flag,
                status="failed",
                error_message=str(e)[:500],
                completed_at=datetime.now(IST).isoformat(),
            )
            stats["failed"] += 1
            logger.error("  FAILED after %s: %s", _fmt_duration(expiry_dur), e)

        _endgroup()

    elapsed_total = time.time() - start_time
    logger.info("=" * 60)
    logger.info(
        "DONE: %d completed, %d failed, %d skipped | %d rows | %.1fm",
        stats["completed"],
        stats["failed"],
        stats["skipped"],
        stats["total_rows"],
        elapsed_total / 60,
    )
    logger.info("=" * 60)

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
