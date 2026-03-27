# DhanHQ_src/supabase_db.py
"""Supabase Postgres backend — same interface as db.Database (SQLite)."""

import logging
from supabase import create_client

from DhanHQ_src.config import SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY

logger = logging.getLogger(__name__)

BATCH_SIZE = 500


class SupabaseDB:
    def __init__(self, url=None, key=None):
        self.client = create_client(
            url or SUPABASE_URL,
            key or SUPABASE_SERVICE_ROLE_KEY,
        )
        self._expiry_id = None
        self._strike_map = {}  # {strike_price: strike_id}

    def ensure_expiry(self, symbol, expiry_date, expiry_flag, lot_size):
        """Upsert expiry row, return its id."""
        resp = (
            self.client.table("expiries")
            .upsert(
                {
                    "symbol": symbol,
                    "expiry_date": expiry_date,
                    "expiry_flag": expiry_flag,
                    "lot_size": lot_size,
                },
                on_conflict="symbol,expiry_date",
            )
            .execute()
        )
        self._expiry_id = resp.data[0]["id"]
        return self._expiry_id

    def ensure_strikes(self, expiry_id, strikes_with_offsets):
        """Upsert strikes, return {strike_price: strike_id}.

        strikes_with_offsets: list of (strike_price, atm_offset) tuples.
        """
        rows = [
            {"expiry_id": expiry_id, "strike": strike, "atm_offset": offset}
            for strike, offset in strikes_with_offsets
        ]
        resp = (
            self.client.table("strikes")
            .upsert(rows, on_conflict="expiry_id,strike")
            .execute()
        )
        self._strike_map = {row["strike"]: row["id"] for row in resp.data}
        return self._strike_map

    # ── Inserts ──────────────────────────────────────────────────────

    def insert_raw_candles(self, rows):
        """Batch upsert raw candle rows.

        Each row must have: strike_id, option_type, timestamp, OHLCIV, spot.
        """
        self._batch_upsert(
            "raw_candles", rows, "strike_id,option_type,timestamp"
        )

    def insert_raw_option_data(self, rows):
        """Accept rows in the flat SQLite format and map to normalized schema.

        Flat row keys: timestamp, date, time, expiry_date, strike, option_type,
                       open, high, low, close, volume, oi, iv, spot, atm_offset.
        """
        candle_rows = []
        for r in rows:
            strike_id = self._strike_map.get(r["strike"])
            if strike_id is None:
                logger.warning("No strike_id for strike=%s, skipping", r["strike"])
                continue
            # Fetcher produces "YYYY-MM-DD HH:MM:SS" (IST, no offset)
            ts = r["timestamp"]
            if "+" not in ts and "Z" not in ts:
                ts = ts.replace(" ", "T") + "+05:30"
            candle_rows.append({
                "strike_id": strike_id,
                "option_type": r["option_type"],
                "timestamp": ts,
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "volume": r.get("volume"),
                "oi": r.get("oi"),
                "iv": r.get("iv"),
                "spot": r.get("spot"),
            })
        if candle_rows:
            self.insert_raw_candles(candle_rows)
        logger.info("Mapped %d/%d raw rows to candles", len(candle_rows), len(rows))

    def insert_derived_metrics(self, rows):
        """Accept flat derived metric rows and map strike -> strike_id."""
        mapped = []
        for r in rows:
            strike_id = self._strike_map.get(r.get("strike"))
            if strike_id is None:
                continue
            row = {k: v for k, v in r.items() if k != "strike"}
            row["strike_id"] = strike_id
            mapped.append(row)
        if mapped:
            self._batch_upsert("derived_metrics", mapped, "strike_id,timestamp")

    def insert_aggregate_metrics(self, rows):
        """Accept flat aggregate rows, inject expiry_id."""
        mapped = []
        for r in rows:
            row = dict(r)
            row["expiry_id"] = self._expiry_id
            # Remove fields not in the Supabase schema
            row.pop("timestamp_key", None)
            mapped.append(row)
        if mapped:
            self._batch_upsert("aggregate_metrics", mapped, "expiry_id,timestamp")

    def insert_iv_history(self, rows):
        """Accept flat IV history rows, inject expiry_id."""
        mapped = []
        for r in rows:
            row = dict(r)
            row["expiry_id"] = self._expiry_id
            mapped.append(row)
        if mapped:
            self._batch_upsert("iv_history", mapped, "expiry_id,date")

    def insert_verification_log(self, rows):
        """Accept flat verification rows and map strike -> strike_id."""
        mapped = []
        for r in rows:
            strike_id = self._strike_map.get(r.get("strike"))
            if strike_id is None:
                continue
            row = {k: v for k, v in r.items() if k != "strike" and k != "option_type"}
            row["strike_id"] = strike_id
            mapped.append(row)
        if mapped:
            self._batch_upsert("verification_log", mapped, "strike_id,date")

    def insert_output(self, rows):
        """Batch upsert into the denormalized output table."""
        if rows:
            self._batch_upsert("output", rows, "timestamp,strike")

    # ── Queries ──────────────────────────────────────────────────────

    def get_raw_data_by_date(self, date):
        """Get all raw candles for a given date (YYYY-MM-DD)."""
        day = date[:10]  # handle both "2026-03-16" and "2026-03-16T..."
        resp = (
            self.client.table("raw_candles")
            .select("*, strikes(strike, atm_offset)")
            .gte("timestamp", f"{day}T00:00:00+05:30")
            .lt("timestamp", f"{day}T23:59:59+05:30")
            .order("timestamp")
            .order("strike_id")
            .execute()
        )
        return self._flatten_candle_rows(resp.data)

    def get_raw_data_ordered(self):
        """Get all raw candles ordered by timestamp, strike, option_type."""
        all_rows = []
        page_size = 1000
        offset = 0
        while True:
            resp = (
                self.client.table("raw_candles")
                .select("*, strikes(strike, atm_offset)")
                .order("timestamp")
                .order("strike_id")
                .order("option_type")
                .range(offset, offset + page_size - 1)
                .execute()
            )
            all_rows.extend(resp.data)
            if len(resp.data) < page_size:
                break
            offset += page_size
        return self._flatten_candle_rows(all_rows)

    def get_iv_history(self):
        """Get all IV history rows ordered by date."""
        resp = (
            self.client.table("iv_history")
            .select("*")
            .eq("expiry_id", self._expiry_id)
            .order("date")
            .execute()
        )
        return resp.data

    def get_eod_data(self, date):
        """Get end-of-day snapshot via the v_eod_snapshot view."""
        resp = (
            self.client.table("v_eod_snapshot")
            .select("*")
            .eq("date", date)
            .order("strike")
            .order("option_type")
            .execute()
        )
        return resp.data

    def get_distinct_dates(self):
        """Get distinct dates from raw_candles."""
        resp = (
            self.client.table("raw_candles")
            .select("timestamp")
            .order("timestamp")
            .execute()
        )
        dates = sorted(set(row["timestamp"][:10] for row in resp.data))
        return dates

    def close(self):
        """No-op — HTTP client is stateless."""
        pass

    # ── Helpers ───────────────────────────────────────────────────────

    def _batch_upsert(self, table, rows, on_conflict):
        """Upsert rows in BATCH_SIZE chunks."""
        for i in range(0, len(rows), BATCH_SIZE):
            chunk = rows[i : i + BATCH_SIZE]
            self.client.table(table).upsert(
                chunk,
                on_conflict=on_conflict,
                returning="minimal",
            ).execute()

    def _flatten_candle_rows(self, rows):
        """Convert joined candle+strike rows to flat dicts matching SQLite format."""
        flat = []
        for r in rows:
            strike_info = r.pop("strikes", {}) or {}
            row = dict(r)
            row["strike"] = strike_info.get("strike", 0)
            row["atm_offset"] = strike_info.get("atm_offset", 0)
            # Add date/time columns expected by calculator
            ts = row.get("timestamp", "")
            if ts:
                row["date"] = ts[:10]
                row["time"] = ts[11:19] if len(ts) > 19 else ""
            flat.append(row)
        return flat
