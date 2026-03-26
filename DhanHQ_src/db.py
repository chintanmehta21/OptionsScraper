# DhanHQ_src/db.py
import sqlite3
import os
import logging

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, db_path):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.conn = sqlite3.connect(db_path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

    def create_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS raw_option_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp DATETIME NOT NULL,
                date DATE NOT NULL,
                time TIME NOT NULL,
                expiry_date DATE NOT NULL,
                strike INTEGER NOT NULL,
                option_type TEXT NOT NULL CHECK(option_type IN ('CE', 'PE')),
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume INTEGER,
                oi INTEGER,
                iv REAL,
                spot REAL,
                atm_offset INTEGER,
                UNIQUE(timestamp, strike, option_type)
            );

            CREATE INDEX IF NOT EXISTS idx_raw_date ON raw_option_data(date);
            CREATE INDEX IF NOT EXISTS idx_raw_strike_type ON raw_option_data(strike, option_type);
            CREATE INDEX IF NOT EXISTS idx_raw_timestamp ON raw_option_data(timestamp);

            CREATE TABLE IF NOT EXISTS derived_metrics (
                timestamp DATETIME NOT NULL,
                strike INTEGER NOT NULL,
                ce_ltp REAL,
                pe_ltp REAL,
                ce_ltp_chg REAL,
                pe_ltp_chg REAL,
                pe_ltp_chg_pct REAL,
                ce_volume INTEGER,
                pe_volume INTEGER,
                ce_oi INTEGER,
                pe_oi INTEGER,
                ce_oi_chg INTEGER,
                pe_oi_chg INTEGER,
                ce_iv REAL,
                pe_iv REAL,
                ce_iv_chg REAL,
                pe_iv_chg REAL,
                pe_ce_oi INTEGER,
                pe_ce_oi_chg INTEGER,
                pcr_oi REAL,
                pcr_oi_chg REAL,
                pcr_vol REAL,
                PRIMARY KEY (timestamp, strike)
            );

            CREATE TABLE IF NOT EXISTS aggregate_metrics (
                timestamp DATETIME PRIMARY KEY,
                spot REAL,
                spot_chg REAL,
                spot_chg_pct REAL,
                fair_price REAL,
                fair_price_chg REAL,
                atm_iv REAL,
                ivr REAL,
                ivp REAL,
                max_pain INTEGER,
                overall_pcr REAL,
                lot_size INTEGER,
                total_ce_oi INTEGER,
                total_pe_oi INTEGER,
                total_oi_net INTEGER,
                total_ce_oi_chg INTEGER,
                total_pe_oi_chg INTEGER,
                total_oi_chg_net INTEGER,
                total_bullish_oi INTEGER,
                total_bearish_oi INTEGER,
                otm_ce_oi INTEGER,
                otm_pe_oi INTEGER,
                otm_oi_net INTEGER,
                otm_ce_oi_chg INTEGER,
                otm_pe_oi_chg INTEGER,
                otm_oi_chg_net INTEGER,
                itm_ce_oi INTEGER,
                itm_pe_oi INTEGER,
                itm_oi_net INTEGER,
                itm_ce_oi_chg INTEGER,
                itm_pe_oi_chg INTEGER,
                itm_oi_chg_net INTEGER
            );

            CREATE TABLE IF NOT EXISTS verification_log (
                date DATE NOT NULL,
                strike INTEGER NOT NULL,
                option_type TEXT NOT NULL,
                dhan_close REAL,
                nse_close REAL,
                dhan_oi INTEGER,
                nse_oi INTEGER,
                dhan_volume INTEGER,
                nse_volume INTEGER,
                close_match BOOLEAN,
                oi_match BOOLEAN,
                volume_match BOOLEAN,
                notes TEXT,
                PRIMARY KEY (date, strike, option_type)
            );

            CREATE TABLE IF NOT EXISTS iv_history (
                date DATE PRIMARY KEY,
                atm_iv REAL,
                spot REAL,
                atm_strike INTEGER
            );
        """)
        self.conn.commit()

    def insert_raw_option_data(self, rows):
        self.conn.executemany(
            """INSERT OR IGNORE INTO raw_option_data
            (timestamp, date, time, expiry_date, strike, option_type,
             open, high, low, close, volume, oi, iv, spot, atm_offset)
            VALUES (:timestamp, :date, :time, :expiry_date, :strike, :option_type,
                    :open, :high, :low, :close, :volume, :oi, :iv, :spot, :atm_offset)""",
            rows,
        )
        self.conn.commit()

    def insert_derived_metrics(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO derived_metrics
            (timestamp, strike, ce_ltp, pe_ltp, ce_ltp_chg, pe_ltp_chg, pe_ltp_chg_pct,
             ce_volume, pe_volume, ce_oi, pe_oi, ce_oi_chg, pe_oi_chg,
             ce_iv, pe_iv, ce_iv_chg, pe_iv_chg,
             pe_ce_oi, pe_ce_oi_chg, pcr_oi, pcr_oi_chg, pcr_vol)
            VALUES (:timestamp, :strike, :ce_ltp, :pe_ltp, :ce_ltp_chg, :pe_ltp_chg, :pe_ltp_chg_pct,
                    :ce_volume, :pe_volume, :ce_oi, :pe_oi, :ce_oi_chg, :pe_oi_chg,
                    :ce_iv, :pe_iv, :ce_iv_chg, :pe_iv_chg,
                    :pe_ce_oi, :pe_ce_oi_chg, :pcr_oi, :pcr_oi_chg, :pcr_vol)""",
            rows,
        )
        self.conn.commit()

    def insert_aggregate_metrics(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO aggregate_metrics
            (timestamp, spot, spot_chg, spot_chg_pct, fair_price, fair_price_chg,
             atm_iv, ivr, ivp, max_pain, overall_pcr, lot_size,
             total_ce_oi, total_pe_oi, total_oi_net,
             total_ce_oi_chg, total_pe_oi_chg, total_oi_chg_net,
             total_bullish_oi, total_bearish_oi,
             otm_ce_oi, otm_pe_oi, otm_oi_net,
             otm_ce_oi_chg, otm_pe_oi_chg, otm_oi_chg_net,
             itm_ce_oi, itm_pe_oi, itm_oi_net,
             itm_ce_oi_chg, itm_pe_oi_chg, itm_oi_chg_net)
            VALUES (:timestamp, :spot, :spot_chg, :spot_chg_pct, :fair_price, :fair_price_chg,
                    :atm_iv, :ivr, :ivp, :max_pain, :overall_pcr, :lot_size,
                    :total_ce_oi, :total_pe_oi, :total_oi_net,
                    :total_ce_oi_chg, :total_pe_oi_chg, :total_oi_chg_net,
                    :total_bullish_oi, :total_bearish_oi,
                    :otm_ce_oi, :otm_pe_oi, :otm_oi_net,
                    :otm_ce_oi_chg, :otm_pe_oi_chg, :otm_oi_chg_net,
                    :itm_ce_oi, :itm_pe_oi, :itm_oi_net,
                    :itm_ce_oi_chg, :itm_pe_oi_chg, :itm_oi_chg_net)""",
            rows,
        )
        self.conn.commit()

    def insert_verification_log(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO verification_log
            (date, strike, option_type, dhan_close, nse_close,
             dhan_oi, nse_oi, dhan_volume, nse_volume,
             close_match, oi_match, volume_match, notes)
            VALUES (:date, :strike, :option_type, :dhan_close, :nse_close,
                    :dhan_oi, :nse_oi, :dhan_volume, :nse_volume,
                    :close_match, :oi_match, :volume_match, :notes)""",
            rows,
        )
        self.conn.commit()

    def insert_iv_history(self, rows):
        self.conn.executemany(
            """INSERT OR REPLACE INTO iv_history (date, atm_iv, spot, atm_strike)
            VALUES (:date, :atm_iv, :spot, :atm_strike)""",
            rows,
        )
        self.conn.commit()

    def get_raw_data_by_date(self, date):
        cursor = self.conn.execute(
            "SELECT * FROM raw_option_data WHERE date = ? ORDER BY timestamp, strike, option_type",
            (date,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_raw_data_ordered(self):
        cursor = self.conn.execute(
            "SELECT * FROM raw_option_data ORDER BY timestamp, strike, option_type"
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_iv_history(self):
        cursor = self.conn.execute(
            "SELECT * FROM iv_history ORDER BY date"
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_eod_data(self, date):
        """Get the last candle of the day for each strike/option_type."""
        cursor = self.conn.execute(
            """SELECT strike, option_type, close, oi, volume
            FROM raw_option_data
            WHERE date = ?
            AND timestamp = (
                SELECT MAX(timestamp) FROM raw_option_data r2
                WHERE r2.date = raw_option_data.date
                AND r2.strike = raw_option_data.strike
                AND r2.option_type = raw_option_data.option_type
            )
            ORDER BY strike, option_type""",
            (date,),
        )
        return [dict(row) for row in cursor.fetchall()]

    def get_distinct_dates(self):
        cursor = self.conn.execute(
            "SELECT DISTINCT date FROM raw_option_data ORDER BY date"
        )
        return [row[0] for row in cursor.fetchall()]

    def close(self):
        self.conn.close()
