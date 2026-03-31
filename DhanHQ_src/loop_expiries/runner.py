"""CLI entry point: python -m DhanHQ_src.loop_expiries.runner --year 2026"""

import argparse
import os
import sys
import logging

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from DhanHQ_src.loop_expiries.scraper import run_loop
from DhanHQ_src.loop_expiries.db import LoopExpiriesDB


def print_status(year: int):
    """Query Supabase and print progress summary."""
    db = LoopExpiriesDB(year)
    s = db.get_progress_summary()

    print(f"Loop Expiries Progress — {year}")
    print(f"  Total:       {s['total']}")
    print(f"  Completed:   {s['completed']}")
    print(f"  Failed:      {s['failed']}")
    print(f"  Skipped:     {s['skipped']}")
    print(f"  Pending:     {s['pending']}")
    print(f"  In Progress: {s['in_progress']}")
    print(f"  Total Rows:  {s['total_rows']:,}")

    if s["failed_details"]:
        print("  Failed expiries:")
        for f in s["failed_details"]:
            print(f"    {f['expiry_date']} {f['expiry_flag']}: {f['error_message']}")

    if s["failed"] > 0:
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(description="Loop Expiries Scraper")
    parser.add_argument("--year", type=int, required=True, help="Year to scrape (e.g. 2026)")
    parser.add_argument("--reset", action="store_true", help="Reset progress and start fresh")
    parser.add_argument("--status", action="store_true", help="Print progress summary and exit")
    args = parser.parse_args()

    if args.status:
        print_status(args.year)
        return

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stats = run_loop(args.year, reset=args.reset)

    if stats["failed"] > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
