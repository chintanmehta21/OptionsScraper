# DhanHQ_src/main.py
import os
import sys
import logging
from collections import defaultdict

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from DhanHQ_src.config import DB_PATH, EXPIRY_DATE
from DhanHQ_src.db import Database
from DhanHQ_src.fetcher import create_dhan_client, fetch_all_options_data, fetch_iv_baseline
from DhanHQ_src.calculator import compute_derived_metrics, compute_aggregate_metrics
from DhanHQ_src.verifier import verify_against_bhavcopy

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_pipeline():
    """Run the full scrape -> store -> calculate -> verify pipeline."""
    logger.info("=" * 60)
    logger.info("NIFTY Options Scraper Pipeline")
    logger.info("=" * 60)

    # Step 1: Initialize DB
    logger.info("Step 1: Initializing database at %s", DB_PATH)
    db = Database(DB_PATH)
    db.create_tables()

    # Step 2: Create DhanHQ client
    logger.info("Step 2: Connecting to DhanHQ API")
    dhan = create_dhan_client()

    # Step 3: Fetch IV baseline (52-week history for IVR/IVP)
    logger.info("Step 3: Fetching 52-week IV baseline")
    iv_baseline_rows = fetch_iv_baseline(dhan)
    if iv_baseline_rows:
        db.insert_iv_history(iv_baseline_rows)
        logger.info("  Stored %d IV history entries", len(iv_baseline_rows))

    # Step 4: Fetch options data
    logger.info("Step 4: Fetching options data (8 strikes x CE/PE)")
    raw_rows = fetch_all_options_data(dhan)
    if not raw_rows:
        logger.error("No data fetched! Check API credentials and parameters.")
        db.close()
        return
    db.insert_raw_option_data(raw_rows)
    logger.info("  Stored %d raw option data rows", len(raw_rows))

    # Step 5: Compute derived metrics
    logger.info("Step 5: Computing derived metrics")
    derived_rows = compute_derived_metrics(raw_rows)
    db.insert_derived_metrics(derived_rows)
    logger.info("  Stored %d derived metric rows", len(derived_rows))

    # Step 6: Compute aggregate metrics
    logger.info("Step 6: Computing aggregate metrics")
    iv_history = db.get_iv_history()
    iv_values = [row["atm_iv"] for row in iv_history if row["atm_iv"]]

    # Group derived rows by timestamp
    by_timestamp = defaultdict(list)
    for row in derived_rows:
        by_timestamp[row["timestamp"]].append(row)

    # Get previous day's spot close for each date
    prev_spot_close = None
    aggregate_rows = []
    prev_fair_price = None

    for ts in sorted(by_timestamp.keys()):
        ts_rows = by_timestamp[ts]
        spot = raw_rows[0]["spot"]  # get spot from any raw row at this timestamp
        for r in raw_rows:
            if r["timestamp"] == ts:
                spot = r["spot"]
                break

        agg = compute_aggregate_metrics(ts_rows, spot, prev_spot_close, iv_values, EXPIRY_DATE)

        if prev_fair_price is not None:
            agg["fair_price_chg"] = round(agg["fair_price"] - prev_fair_price, 2)
        prev_fair_price = agg["fair_price"]

        aggregate_rows.append(agg)

        # Update prev_spot_close at end of each day
        current_date = ts[:10]
        if aggregate_rows and aggregate_rows[-1]["timestamp"][:10] != current_date:
            prev_spot_close = spot

    db.insert_aggregate_metrics(aggregate_rows)
    logger.info("  Stored %d aggregate metric rows", len(aggregate_rows))

    # Step 7: Verify against NSE Bhavcopy
    logger.info("Step 7: Verifying against NSE Bhavcopy")
    summary = verify_against_bhavcopy(db)
    logger.info("  Verification: %d/%d matched (%.1f%%)",
                summary["total_matches"], summary["total_checks"], summary["match_rate"])

    if summary["mismatches"] > 0:
        logger.warning("  %d mismatches found! Check verification_log table.", summary["mismatches"])

    # Done
    db.close()
    logger.info("=" * 60)
    logger.info("Pipeline complete!")
    logger.info("  Database: %s", os.path.abspath(DB_PATH))
    logger.info("  Raw rows: %d", len(raw_rows))
    logger.info("  Derived rows: %d", len(derived_rows))
    logger.info("  Aggregate rows: %d", len(aggregate_rows))
    logger.info("=" * 60)


if __name__ == "__main__":
    run_pipeline()
