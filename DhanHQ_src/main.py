# DhanHQ_src/main.py
import os
import sys
import logging
import time
from collections import defaultdict

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DhanHQ_src.config import (
    DB_PATH, EXPIRY_DATE, EXPIRY_FLAG, LOT_SIZE,
    SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY,
)
from DhanHQ_src.fetcher import create_dhan_client, fetch_all_options_data, fetch_iv_baseline
from DhanHQ_src.calculator import compute_derived_metrics, compute_aggregate_metrics
from DhanHQ_src.verifier import verify_against_bhavcopy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# GitHub Actions progress helpers
_CI = os.environ.get("GITHUB_ACTIONS") == "true"


def _group(title):
    if _CI:
        print(f"::group::{title}", flush=True)
    logger.info(title)


def _endgroup():
    if _CI:
        print("::endgroup::", flush=True)


def _use_supabase():
    return bool(SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY)


def _init_db():
    """Return db instance depending on backend."""
    if _use_supabase():
        from DhanHQ_src.supabase_db import SupabaseDB
        db = SupabaseDB()
        logger.info("Using Supabase backend")
        return db
    else:
        from DhanHQ_src.db import Database
        db = Database(DB_PATH)
        db.create_tables()
        logger.info("Using SQLite backend at %s", DB_PATH)
        return db


def _build_output_rows(raw_rows, derived_rows, aggregate_rows, expiry_date):
    """Build denormalized output rows joining raw CE/PE + derived + aggregate.

    One row per (timestamp, strike) with CE/PE candles side-by-side,
    derived metrics, and aggregate metrics.
    """
    # Index raw rows by (timestamp, strike, option_type)
    raw_idx = {}
    for r in raw_rows:
        raw_idx[(r["timestamp"], r["strike"], r["option_type"])] = r

    # Index derived by (timestamp, strike)
    derived_idx = {}
    for r in derived_rows:
        derived_idx[(r["timestamp"], r["strike"])] = r

    # Index aggregate by timestamp
    agg_idx = {}
    for r in aggregate_rows:
        agg_idx[r["timestamp"]] = r

    # Collect unique (timestamp, strike) pairs from derived rows
    output = []
    for (ts, strike), drv in derived_idx.items():
        ce = raw_idx.get((ts, strike, "CE"), {})
        pe = raw_idx.get((ts, strike, "PE"), {})
        agg = agg_idx.get(ts, {})

        # Convert timestamp for Supabase TIMESTAMPTZ
        ts_pg = ts
        if "+" not in ts and "Z" not in ts:
            ts_pg = ts.replace(" ", "T") + "+05:30"

        date_str = ts[:10]
        time_str = ts[11:19] if len(ts) > 19 else ts[11:]

        output.append({
            "timestamp": ts_pg,
            "date": date_str,
            "time": time_str,
            "expiry_date": expiry_date,
            "strike": strike,
            "atm_offset": ce.get("atm_offset") or pe.get("atm_offset", 0),
            "spot": ce.get("spot") or pe.get("spot"),
            # CE candle
            "ce_open": ce.get("open"),
            "ce_high": ce.get("high"),
            "ce_low": ce.get("low"),
            "ce_close": ce.get("close"),
            "ce_volume": ce.get("volume"),
            "ce_oi": ce.get("oi"),
            "ce_iv": ce.get("iv"),
            # PE candle
            "pe_open": pe.get("open"),
            "pe_high": pe.get("high"),
            "pe_low": pe.get("low"),
            "pe_close": pe.get("close"),
            "pe_volume": pe.get("volume"),
            "pe_oi": pe.get("oi"),
            "pe_iv": pe.get("iv"),
            # Derived per-strike
            "ce_ltp_chg": drv.get("ce_ltp_chg"),
            "pe_ltp_chg": drv.get("pe_ltp_chg"),
            "pe_ltp_chg_pct": drv.get("pe_ltp_chg_pct"),
            "ce_oi_chg": drv.get("ce_oi_chg"),
            "pe_oi_chg": drv.get("pe_oi_chg"),
            "ce_iv_chg": drv.get("ce_iv_chg"),
            "pe_iv_chg": drv.get("pe_iv_chg"),
            "pe_ce_oi": drv.get("pe_ce_oi"),
            "pe_ce_oi_chg": drv.get("pe_ce_oi_chg"),
            "pcr_oi": drv.get("pcr_oi"),
            "pcr_oi_chg": drv.get("pcr_oi_chg"),
            "pcr_vol": drv.get("pcr_vol"),
            # Aggregate (index-level)
            "spot_chg": agg.get("spot_chg"),
            "spot_chg_pct": agg.get("spot_chg_pct"),
            "fair_price": agg.get("fair_price"),
            "fair_price_chg": agg.get("fair_price_chg"),
            "atm_iv": agg.get("atm_iv"),
            "ivr": agg.get("ivr"),
            "ivp": agg.get("ivp"),
            "max_pain": agg.get("max_pain"),
            "overall_pcr": agg.get("overall_pcr"),
            "lot_size": agg.get("lot_size"),
            "total_ce_oi": agg.get("total_ce_oi"),
            "total_pe_oi": agg.get("total_pe_oi"),
            "total_oi_net": agg.get("total_oi_net"),
            "total_ce_oi_chg": agg.get("total_ce_oi_chg"),
            "total_pe_oi_chg": agg.get("total_pe_oi_chg"),
            "total_oi_chg_net": agg.get("total_oi_chg_net"),
            "total_bullish_oi": agg.get("total_bullish_oi"),
            "total_bearish_oi": agg.get("total_bearish_oi"),
        })

    return output


def run_pipeline():
    """Run the full scrape -> store -> calculate -> verify -> output pipeline."""
    logger.info("=" * 60)
    logger.info("NIFTY Options Scraper Pipeline")
    logger.info("=" * 60)
    t_start = time.time()
    step_times = {}

    # Step 1: Initialize DB
    _group("Step 1/9: Initializing database")
    t0 = time.time()
    db = _init_db()
    if _use_supabase():
        expiry_id = db.ensure_expiry("NIFTY", EXPIRY_DATE, EXPIRY_FLAG, LOT_SIZE)
        logger.info("  Expiry ID: %d", expiry_id)
    step_times["1. Init DB"] = time.time() - t0
    _endgroup()

    # Step 2: Create DhanHQ client
    _group("Step 2/9: Connecting to DhanHQ API")
    t0 = time.time()
    dhan = create_dhan_client()
    step_times["2. Connect API"] = time.time() - t0
    _endgroup()

    # Step 3: Fetch IV baseline (52-week history for IVR/IVP)
    _group("Step 3/9: Fetching 52-week IV baseline")
    t0 = time.time()
    iv_baseline_rows = fetch_iv_baseline(dhan)
    if iv_baseline_rows:
        db.insert_iv_history(iv_baseline_rows)
        logger.info("  Stored %d IV history entries", len(iv_baseline_rows))
    step_times["3. IV baseline"] = time.time() - t0
    _endgroup()

    # Step 4: Fetch options data
    _group("Step 4/9: Fetching options data (9 strikes x CE/PE)")
    t0 = time.time()
    raw_rows = fetch_all_options_data(dhan)
    if not raw_rows:
        logger.error("No data fetched! Check API credentials and parameters.")
        db.close()
        return

    if _use_supabase():
        strikes_seen = {}
        for r in raw_rows:
            if r["strike"] not in strikes_seen:
                strikes_seen[r["strike"]] = r.get("atm_offset", 0)
        strike_tuples = [(s, o) for s, o in strikes_seen.items()]
        db.ensure_strikes(expiry_id, strike_tuples)
        logger.info("  Registered %d strikes", len(strike_tuples))

    db.insert_raw_option_data(raw_rows)
    logger.info("  Stored %d raw option data rows", len(raw_rows))
    step_times["4. Fetch options"] = time.time() - t0
    _endgroup()

    # Step 5: Compute derived metrics
    _group("Step 5/9: Computing derived metrics")
    t0 = time.time()
    derived_rows = compute_derived_metrics(raw_rows)
    db.insert_derived_metrics(derived_rows)
    logger.info("  Stored %d derived metric rows", len(derived_rows))
    step_times["5. Derived metrics"] = time.time() - t0
    _endgroup()

    # Step 6: Compute aggregate metrics
    _group("Step 6/9: Computing aggregate metrics")
    t0 = time.time()
    iv_history = db.get_iv_history()
    iv_values = [row["atm_iv"] for row in iv_history if row["atm_iv"]]

    by_timestamp = defaultdict(list)
    for row in derived_rows:
        by_timestamp[row["timestamp"]].append(row)

    prev_spot_close = None
    aggregate_rows = []
    prev_fair_price = None

    for ts in sorted(by_timestamp.keys()):
        ts_rows = by_timestamp[ts]
        spot = raw_rows[0]["spot"]
        for r in raw_rows:
            if r["timestamp"] == ts:
                spot = r["spot"]
                break

        agg = compute_aggregate_metrics(ts_rows, spot, prev_spot_close, iv_values, EXPIRY_DATE)

        if prev_fair_price is not None:
            agg["fair_price_chg"] = round(agg["fair_price"] - prev_fair_price, 2)
        prev_fair_price = agg["fair_price"]

        aggregate_rows.append(agg)

        current_date = ts[:10]
        if aggregate_rows and aggregate_rows[-1]["timestamp"][:10] != current_date:
            prev_spot_close = spot

    db.insert_aggregate_metrics(aggregate_rows)
    logger.info("  Stored %d aggregate metric rows", len(aggregate_rows))
    step_times["6. Aggregate metrics"] = time.time() - t0
    _endgroup()

    # Step 7: Verify against NSE Bhavcopy
    _group("Step 7/9: Verifying against NSE Bhavcopy")
    t0 = time.time()
    summary = verify_against_bhavcopy(db)
    logger.info("  Verification: %d/%d matched (%.1f%%)",
                summary["total_matches"], summary["total_checks"], summary["match_rate"])
    if summary["mismatches"] > 0:
        logger.warning("  %d mismatches found! Check verification_log table.", summary["mismatches"])
    step_times["7. Verification"] = time.time() - t0
    _endgroup()

    # Step 8: Build and store denormalized output (Supabase only)
    output_row_count = 0
    if _use_supabase():
        _group("Step 8/9: Building denormalized output table")
        t0 = time.time()
        output_rows = _build_output_rows(raw_rows, derived_rows, aggregate_rows, EXPIRY_DATE)
        db.insert_output(output_rows)
        output_row_count = len(output_rows)
        logger.info("  Stored %d output rows", output_row_count)
        step_times["8. Output table"] = time.time() - t0
        _endgroup()

    # Step 9: Run EDA checks (Supabase only)
    eda_status = "skipped"
    if _use_supabase():
        _group("Step 9/9: Running EDA checks")
        t0 = time.time()
        try:
            from tests.supabase.eda import run_eda
            eda_dir = run_eda()
            if eda_dir:
                logger.info("  EDA reports: %s", eda_dir)
                eda_status = "pass"
        except Exception as e:
            logger.warning("  EDA checks failed (non-fatal): %s", e)
            eda_status = "fail"
        step_times["9. EDA checks"] = time.time() - t0
        _endgroup()

    # Done
    db.close()
    total_time = time.time() - t_start

    logger.info("=" * 60)
    logger.info("Pipeline complete! (%.1fs)", total_time)
    if not _use_supabase():
        logger.info("  Database: %s", os.path.abspath(DB_PATH))
    logger.info("  Raw rows: %d", len(raw_rows))
    logger.info("  Derived rows: %d", len(derived_rows))
    logger.info("  Aggregate rows: %d", len(aggregate_rows))
    logger.info("=" * 60)

    # Write GitHub Actions job summary
    _write_job_summary(
        raw_rows, derived_rows, aggregate_rows, output_row_count,
        iv_baseline_rows, summary, eda_status, step_times, total_time,
    )


def _write_job_summary(raw_rows, derived_rows, aggregate_rows, output_row_count,
                       iv_baseline_rows, verification, eda_status, step_times, total_time):
    """Write a markdown summary to $GITHUB_STEP_SUMMARY if running in CI."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    match_rate = verification.get("match_rate", 0)
    verify_icon = "white_check_mark" if match_rate >= 95 else "warning"
    eda_icon = {"pass": "white_check_mark", "fail": "x", "skipped": "heavy_minus_sign"}[eda_status]

    lines = [
        f"## NIFTY Options Scraper — {EXPIRY_DATE}",
        "",
        "### Row Counts",
        "| Table | Rows |",
        "|-------|------|",
        f"| IV History | {len(iv_baseline_rows) if iv_baseline_rows else 0:,} |",
        f"| Raw Candles | {len(raw_rows):,} |",
        f"| Derived Metrics | {len(derived_rows):,} |",
        f"| Aggregate Metrics | {len(aggregate_rows):,} |",
        f"| Output (denormalized) | {output_row_count:,} |",
        "",
        "### Verification",
        f"| Metric | Value |",
        "|--------|-------|",
        f"| Checks | {verification['total_checks']} |",
        f"| Matches | {verification['total_matches']} |",
        f"| Match rate | {match_rate:.1f}% :{verify_icon}: |",
        f"| Mismatches | {verification['mismatches']} |",
        "",
        f"### EDA: :{eda_icon}:",
        "",
        "### Step Timings",
        "| Step | Duration |",
        "|------|----------|",
    ]
    for step_name, duration in step_times.items():
        lines.append(f"| {step_name} | {duration:.1f}s |")
    lines.append(f"| **Total** | **{total_time:.1f}s** |")

    try:
        with open(summary_path, "a") as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.debug("Could not write job summary: %s", e)


if __name__ == "__main__":
    run_pipeline()
