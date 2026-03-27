# DhanHQ_src/main.py
import os
import sys
import time
import logging
from collections import defaultdict
from datetime import date as dt_date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DhanHQ_src.config import (
    DB_PATH, EXPIRY_CONFIGS, SYMBOL,
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
    """Build denormalized output rows joining raw CE/PE + derived + aggregate."""
    raw_idx = {}
    for r in raw_rows:
        raw_idx[(r["timestamp"], r["strike"], r["option_type"])] = r

    derived_idx = {}
    for r in derived_rows:
        derived_idx[(r["timestamp"], r["strike"])] = r

    agg_idx = {}
    for r in aggregate_rows:
        agg_idx[r["timestamp"]] = r

    output = []
    for (ts, strike), drv in derived_idx.items():
        ce = raw_idx.get((ts, strike, "CE"), {})
        pe = raw_idx.get((ts, strike, "PE"), {})
        agg = agg_idx.get(ts, {})

        ts_pg = ts
        if "+" not in ts and "Z" not in ts:
            ts_pg = ts.replace(" ", "T") + "+05:30"

        date_str = ts[:10]
        time_str = ts[11:19] if len(ts) > 19 else ts[11:]

        output.append({
            "timestamp": ts_pg, "date": date_str, "time": time_str,
            "expiry_date": expiry_date, "strike": strike,
            "atm_offset": ce.get("atm_offset") or pe.get("atm_offset", 0),
            "spot": ce.get("spot") or pe.get("spot"),
            "ce_open": ce.get("open"), "ce_high": ce.get("high"),
            "ce_low": ce.get("low"), "ce_close": ce.get("close"),
            "ce_volume": ce.get("volume"), "ce_oi": ce.get("oi"), "ce_iv": ce.get("iv"),
            "pe_open": pe.get("open"), "pe_high": pe.get("high"),
            "pe_low": pe.get("low"), "pe_close": pe.get("close"),
            "pe_volume": pe.get("volume"), "pe_oi": pe.get("oi"), "pe_iv": pe.get("iv"),
            "ce_ltp_chg": drv.get("ce_ltp_chg"), "pe_ltp_chg": drv.get("pe_ltp_chg"),
            "pe_ltp_chg_pct": drv.get("pe_ltp_chg_pct"),
            "ce_oi_chg": drv.get("ce_oi_chg"), "pe_oi_chg": drv.get("pe_oi_chg"),
            "ce_iv_chg": drv.get("ce_iv_chg"), "pe_iv_chg": drv.get("pe_iv_chg"),
            "pe_ce_oi": drv.get("pe_ce_oi"), "pe_ce_oi_chg": drv.get("pe_ce_oi_chg"),
            "pcr_oi": drv.get("pcr_oi"), "pcr_oi_chg": drv.get("pcr_oi_chg"),
            "pcr_vol": drv.get("pcr_vol"),
            "spot_chg": agg.get("spot_chg"), "spot_chg_pct": agg.get("spot_chg_pct"),
            "fair_price": agg.get("fair_price"), "fair_price_chg": agg.get("fair_price_chg"),
            "atm_iv": agg.get("atm_iv"), "ivr": agg.get("ivr"), "ivp": agg.get("ivp"),
            "max_pain": agg.get("max_pain"), "overall_pcr": agg.get("overall_pcr"),
            "lot_size": agg.get("lot_size"),
            "total_ce_oi": agg.get("total_ce_oi"), "total_pe_oi": agg.get("total_pe_oi"),
            "total_oi_net": agg.get("total_oi_net"),
            "total_ce_oi_chg": agg.get("total_ce_oi_chg"),
            "total_pe_oi_chg": agg.get("total_pe_oi_chg"),
            "total_oi_chg_net": agg.get("total_oi_chg_net"),
            "total_bullish_oi": agg.get("total_bullish_oi"),
            "total_bearish_oi": agg.get("total_bearish_oi"),
        })
    return output


def _run_expiry(db, dhan, config):
    """Process a single expiry config. Returns stats dict or None."""
    expiry_date = config["expiry_date"]
    from_date = config["from_date"]
    to_date = config["to_date"]
    expiry_flag = config["expiry_flag"]
    expiry_code = config.get("expiry_code", 1)
    lot_size = config["lot_size"]
    is_supa = _use_supabase()

    # IV baseline: 52 weeks before from_date
    from_dt = dt_date.fromisoformat(from_date)
    iv_from = (from_dt - timedelta(weeks=52)).isoformat()
    iv_to = from_date

    # Ensure expiry (Supabase FK setup)
    if is_supa:
        expiry_id = db.ensure_expiry(SYMBOL, expiry_date, expiry_flag, lot_size)
        logger.info("  Expiry ID: %d", expiry_id)

    # IV baseline
    logger.info("  Fetching IV baseline (%s to %s)", iv_from, iv_to)
    iv_rows = fetch_iv_baseline(dhan, baseline_from=iv_from, baseline_to=iv_to,
                                 expiry_flag=expiry_flag)
    if iv_rows:
        db.insert_iv_history(iv_rows)
        logger.info("  Stored %d IV history entries", len(iv_rows))

    # Fetch options
    logger.info("  Fetching options (9 strikes x CE/PE)")
    raw_rows = fetch_all_options_data(
        dhan, from_date=from_date, to_date=to_date,
        expiry_date=expiry_date, expiry_flag=expiry_flag,
        expiry_code=expiry_code,
    )
    if not raw_rows:
        logger.error("  No data fetched for %s!", expiry_date)
        return None

    # Register strikes
    if is_supa:
        strikes_seen = {}
        for r in raw_rows:
            if r["strike"] not in strikes_seen:
                strikes_seen[r["strike"]] = r.get("atm_offset", 0)
        db.ensure_strikes(expiry_id, list(strikes_seen.items()))
        logger.info("  Registered %d strikes", len(strikes_seen))

    db.insert_raw_option_data(raw_rows)
    logger.info("  Stored %d raw rows", len(raw_rows))

    # Derived metrics
    derived_rows = compute_derived_metrics(raw_rows)
    db.insert_derived_metrics(derived_rows)
    logger.info("  Stored %d derived rows", len(derived_rows))

    # Aggregate metrics
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

        agg = compute_aggregate_metrics(ts_rows, spot, prev_spot_close, iv_values, expiry_date)

        if prev_fair_price is not None:
            agg["fair_price_chg"] = round(agg["fair_price"] - prev_fair_price, 2)
        prev_fair_price = agg["fair_price"]
        aggregate_rows.append(agg)

        current_date = ts[:10]
        if aggregate_rows and aggregate_rows[-1]["timestamp"][:10] != current_date:
            prev_spot_close = spot

    db.insert_aggregate_metrics(aggregate_rows)
    logger.info("  Stored %d aggregate rows", len(aggregate_rows))

    # Verify against NSE Bhavcopy
    logger.info("  Verifying against NSE Bhavcopy")
    verification = verify_against_bhavcopy(db, expiry_date=expiry_date)
    logger.info("  Verification: %d/%d matched (%.1f%%)",
                verification["total_matches"], verification["total_checks"],
                verification["match_rate"])
    if verification["mismatches"] > 0:
        logger.warning("  %d mismatches found!", verification["mismatches"])

    # Output table (Supabase)
    output_count = 0
    if is_supa:
        output_rows = _build_output_rows(raw_rows, derived_rows, aggregate_rows, expiry_date)
        db.insert_output(output_rows)
        output_count = len(output_rows)
        logger.info("  Stored %d output rows", output_count)

    return {
        "expiry_date": expiry_date,
        "iv_baseline": len(iv_rows) if iv_rows else 0,
        "raw": len(raw_rows),
        "derived": len(derived_rows),
        "aggregate": len(aggregate_rows),
        "output": output_count,
        "verification": verification,
    }


def run_pipeline():
    """Run the full pipeline for all configured expiries."""
    logger.info("=" * 60)
    logger.info("NIFTY Options Scraper Pipeline")
    logger.info("  Expiries: %s", ", ".join(c["expiry_date"] for c in EXPIRY_CONFIGS))
    logger.info("=" * 60)
    t_start = time.time()
    step_times = {}

    # Setup
    _group("Setup: Database + API connection")
    t0 = time.time()
    db = _init_db()
    dhan = create_dhan_client()
    step_times["Setup"] = time.time() - t0
    _endgroup()

    # Process each expiry
    all_stats = []
    total = len(EXPIRY_CONFIGS)

    for i, config in enumerate(EXPIRY_CONFIGS, 1):
        label = f"Expiry {i}/{total}: {config['expiry_date']} ({config['expiry_flag']})"
        _group(label)
        t0 = time.time()
        stats = _run_expiry(db, dhan, config)
        step_times[config["expiry_date"]] = time.time() - t0
        if stats:
            all_stats.append(stats)
        else:
            logger.warning("  Expiry %s produced no data", config["expiry_date"])
        _endgroup()

    # EDA
    eda_status = "skipped"
    if _use_supabase() and all_stats:
        _group("EDA: Data quality checks")
        t0 = time.time()
        try:
            from tests.supabase.eda import run_eda
            for stats in all_stats:
                eda_dir = run_eda(expiry_date=stats["expiry_date"])
                if eda_dir:
                    logger.info("  EDA report for %s: %s", stats["expiry_date"], eda_dir)
            eda_status = "pass"
        except Exception as e:
            logger.warning("  EDA failed (non-fatal): %s", e)
            eda_status = "fail"
        step_times["EDA"] = time.time() - t0
        _endgroup()

    # Done
    db.close()
    total_time = time.time() - t_start

    logger.info("=" * 60)
    logger.info("Pipeline complete! (%.1fs)", total_time)
    for stats in all_stats:
        logger.info("  [%s] raw=%d derived=%d agg=%d output=%d",
                     stats["expiry_date"], stats["raw"], stats["derived"],
                     stats["aggregate"], stats["output"])
    if not all_stats:
        logger.error("  No data produced for any expiry!")
    logger.info("=" * 60)

    _write_job_summary(all_stats, eda_status, step_times, total_time)


def _write_job_summary(all_stats, eda_status, step_times, total_time):
    """Write markdown summary to $GITHUB_STEP_SUMMARY."""
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return

    lines = ["## NIFTY Options Scraper", ""]

    if not all_stats:
        lines.append("> :x: **No data produced.** Check API credentials and logs.")
        lines.append("")

    for stats in all_stats:
        v = stats["verification"]
        vi = ":white_check_mark:" if v["match_rate"] >= 95 else ":warning:"
        lines.extend([
            f"### Expiry: {stats['expiry_date']}",
            "| Table | Rows |", "|-------|------|",
            f"| IV History | {stats['iv_baseline']:,} |",
            f"| Raw Candles | {stats['raw']:,} |",
            f"| Derived Metrics | {stats['derived']:,} |",
            f"| Aggregate Metrics | {stats['aggregate']:,} |",
            f"| Output | {stats['output']:,} |",
            "",
            f"Verification: {v['total_matches']}/{v['total_checks']} "
            f"({v['match_rate']:.1f}%) {vi}", "",
        ])

    ei = {"pass": ":white_check_mark:", "fail": ":x:", "skipped": ":heavy_minus_sign:"}[eda_status]
    lines.extend([f"### EDA: {ei}", "", "### Timings",
                  "| Step | Duration |", "|------|----------|"])
    for name, dur in step_times.items():
        lines.append(f"| {name} | {dur:.1f}s |")
    lines.append(f"| **Total** | **{total_time:.1f}s** |")

    try:
        with open(summary_path, "a") as f:
            f.write("\n".join(lines) + "\n")
    except Exception as e:
        logger.debug("Could not write job summary: %s", e)


if __name__ == "__main__":
    run_pipeline()
