"""Supabase client for loop_expiries dynamic year tables."""

import logging
from supabase import create_client

from DhanHQ_src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


class LoopExpiriesDB:
    def __init__(self, year: int, url=None, key=None):
        self.year = year
        self.data_table = f"full_expiries_{year}"
        self.progress_table = f"scrape_progress_{year}"
        self.client = create_client(
            url or SUPABASE_URL,
            key or SUPABASE_SERVICE_ROLE_KEY,
        )

    def setup_tables(self):
        """Verify year-specific tables exist (must be pre-created via migration).

        Tables are created externally: Supabase SQL Editor, MCP, or
        ``SELECT create_loop_tables(year);`` — PostgREST cannot run DDL.
        """
        try:
            self.client.table(self.progress_table).select("*").limit(1).execute()
            self.client.table(self.data_table).select("*").limit(1).execute()
            logger.info("Tables verified: %s, %s", self.data_table, self.progress_table)
        except Exception as e:
            raise RuntimeError(
                f"Tables {self.data_table} / {self.progress_table} not found. "
                f"Create them first: SELECT create_loop_tables({self.year}); — "
                f"Error: {e}"
            )

    def seed_progress(self, expiries: list[dict]) -> int:
        """Seed progress table with pending entries, skip already completed/skipped."""
        existing = self._get_all_progress()
        done = {
            (e["expiry_date"], e["expiry_flag"])
            for e in existing
            if e["status"] in ("completed", "skipped")
        }

        rows = [
            {"expiry_date": e["expiry_date"], "expiry_flag": e["expiry_flag"], "status": "pending"}
            for e in expiries
            if (e["expiry_date"], e["expiry_flag"]) not in done
        ]

        if rows:
            for i in range(0, len(rows), BATCH_SIZE):
                chunk = rows[i : i + BATCH_SIZE]
                self.client.table(self.progress_table).upsert(
                    chunk, on_conflict="expiry_date,expiry_flag"
                ).execute()

        logger.info("Seeded %d pending expiries (%d already done)", len(rows), len(done))
        return len(rows)

    def get_pending_expiries(self) -> list[dict]:
        """Get expiries that need processing, ordered by date."""
        resp = (
            self.client.table(self.progress_table)
            .select("*")
            .in_("status", ["pending", "failed", "in_progress"])
            .order("expiry_date")
            .order("expiry_flag")
            .execute()
        )
        return resp.data

    def update_progress(self, expiry_date: str, expiry_flag: str, **fields):
        """Update progress for a single expiry."""
        (
            self.client.table(self.progress_table)
            .update(fields)
            .eq("expiry_date", expiry_date)
            .eq("expiry_flag", expiry_flag)
            .execute()
        )

    def reset_progress(self):
        """Reset all entries to pending (for --reset flag)."""
        all_rows = self._get_all_progress()
        for row in all_rows:
            self.update_progress(
                row["expiry_date"],
                row["expiry_flag"],
                status="pending",
                rows_inserted=0,
                api_calls_made=0,
                started_at=None,
                completed_at=None,
                error_message=None,
            )
        logger.info("Reset %d progress entries to pending", len(all_rows))

    def insert_candles(self, rows: list[dict], expiry_flag: str) -> int:
        """Batch upsert raw candle rows into full_expiries_{year}."""
        mapped = []
        for r in rows:
            ts = r["timestamp"]
            if "+" not in ts and "Z" not in ts:
                ts = ts.replace(" ", "T") + "+05:30"
            row = {
                "expiry_date": r["expiry_date"],
                "expiry_flag": expiry_flag,
                "timestamp": ts,
                "date": r["date"],
                "time": r["time"],
                "strike": int(r["strike"]) if r.get("strike") is not None else 0,
                "atm_offset": int(r["atm_offset"]),
                "option_type": r["option_type"],
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "volume": int(r["volume"]) if r.get("volume") is not None else None,
                "oi": int(r["oi"]) if r.get("oi") is not None else None,
                "iv": r.get("iv"),
                "spot": r.get("spot"),
            }
            mapped.append(row)

        for i in range(0, len(mapped), BATCH_SIZE):
            chunk = mapped[i : i + BATCH_SIZE]
            self.client.table(self.data_table).upsert(
                chunk,
                on_conflict="expiry_date,expiry_flag,timestamp,strike,option_type",
                returning="minimal",
            ).execute()

        logger.info("Upserted %d candle rows into %s", len(mapped), self.data_table)
        return len(mapped)

    def get_progress_summary(self) -> dict:
        """Get summary of scraping progress for reporting."""
        all_rows = self._get_all_progress()
        summary = {
            "total": len(all_rows),
            "completed": 0,
            "failed": 0,
            "skipped": 0,
            "pending": 0,
            "in_progress": 0,
            "total_rows": 0,
            "failed_details": [],
        }
        for row in all_rows:
            status = row["status"]
            summary[status] = summary.get(status, 0) + 1
            summary["total_rows"] += row.get("rows_inserted", 0) or 0
            if status == "failed":
                summary["failed_details"].append({
                    "expiry_date": row["expiry_date"],
                    "expiry_flag": row["expiry_flag"],
                    "error_message": row.get("error_message"),
                })
        return summary

    def close(self):
        """No-op — HTTP client is stateless."""
        pass

    def _get_all_progress(self) -> list[dict]:
        resp = (
            self.client.table(self.progress_table)
            .select("*")
            .order("expiry_date")
            .order("expiry_flag")
            .execute()
        )
        return resp.data
