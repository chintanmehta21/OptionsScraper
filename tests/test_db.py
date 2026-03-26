# tests/test_db.py
import os
import sqlite3
import tempfile
import pytest
from DhanHQ_src.db import Database


@pytest.fixture
def db():
    """Create a temporary database for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    database.create_tables()
    yield database
    database.close()
    os.unlink(path)


def test_tables_exist(db):
    cursor = db.conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    )
    tables = [row[0] for row in cursor.fetchall()]
    assert "raw_option_data" in tables
    assert "derived_metrics" in tables
    assert "aggregate_metrics" in tables
    assert "verification_log" in tables
    assert "iv_history" in tables


def test_insert_raw_option_data(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "date": "2026-03-16",
        "time": "09:15:00",
        "expiry_date": "2026-03-30",
        "strike": 23200,
        "option_type": "CE",
        "open": 500.0,
        "high": 510.0,
        "low": 495.0,
        "close": 505.0,
        "volume": 1000,
        "oi": 50000,
        "iv": 25.5,
        "spot": 23250.0,
        "atm_offset": 0,
    }
    db.insert_raw_option_data([row])
    result = db.conn.execute("SELECT * FROM raw_option_data").fetchall()
    assert len(result) == 1
    assert result[0][5] == 23200  # strike
    assert result[0][6] == "CE"  # option_type


def test_unique_constraint_raw_option_data(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "date": "2026-03-16",
        "time": "09:15:00",
        "expiry_date": "2026-03-30",
        "strike": 23200,
        "option_type": "CE",
        "open": 500.0,
        "high": 510.0,
        "low": 495.0,
        "close": 505.0,
        "volume": 1000,
        "oi": 50000,
        "iv": 25.5,
        "spot": 23250.0,
        "atm_offset": 0,
    }
    db.insert_raw_option_data([row])
    # Inserting duplicate should not raise, should be ignored
    db.insert_raw_option_data([row])
    result = db.conn.execute("SELECT COUNT(*) FROM raw_option_data").fetchone()
    assert result[0] == 1


def test_insert_derived_metrics(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "strike": 23200,
        "ce_ltp": 505.0,
        "pe_ltp": 400.0,
        "ce_ltp_chg": -10.0,
        "pe_ltp_chg": -30.0,
        "pe_ltp_chg_pct": -6.98,
        "ce_volume": 1000,
        "pe_volume": 800,
        "ce_oi": 50000,
        "pe_oi": 60000,
        "ce_oi_chg": 500,
        "pe_oi_chg": -200,
        "ce_iv": 25.5,
        "pe_iv": 24.8,
        "ce_iv_chg": 0.3,
        "pe_iv_chg": -0.1,
        "pe_ce_oi": 10000,
        "pe_ce_oi_chg": -700,
        "pcr_oi": 1.2,
        "pcr_oi_chg": -0.05,
        "pcr_vol": 0.8,
    }
    db.insert_derived_metrics([row])
    result = db.conn.execute("SELECT * FROM derived_metrics").fetchall()
    assert len(result) == 1


def test_insert_aggregate_metrics(db):
    row = {
        "timestamp": "2026-03-16 09:15:00",
        "spot": 23250.0,
        "spot_chg": 42.5,
        "spot_chg_pct": 0.18,
        "fair_price": 23260.0,
        "fair_price_chg": 10.0,
        "atm_iv": 25.0,
        "ivr": 89.39,
        "ivp": 97.98,
        "max_pain": 24500,
        "overall_pcr": 1.03,
        "lot_size": 65,
        "total_ce_oi": 36786,
        "total_pe_oi": 66534,
        "total_oi_net": 29748,
        "total_ce_oi_chg": 0,
        "total_pe_oi_chg": 0,
        "total_oi_chg_net": 0,
        "total_bullish_oi": 0,
        "total_bearish_oi": 0,
        "otm_ce_oi": 22773,
        "otm_pe_oi": 41452,
        "otm_oi_net": 18679,
        "otm_ce_oi_chg": 0,
        "otm_pe_oi_chg": 0,
        "otm_oi_chg_net": 0,
        "itm_ce_oi": 14013,
        "itm_pe_oi": 25082,
        "itm_oi_net": 11069,
        "itm_ce_oi_chg": 0,
        "itm_pe_oi_chg": 0,
        "itm_oi_chg_net": 0,
    }
    db.insert_aggregate_metrics([row])
    result = db.conn.execute("SELECT * FROM aggregate_metrics").fetchall()
    assert len(result) == 1


def test_insert_iv_history(db):
    row = {
        "date": "2025-06-15",
        "atm_iv": 18.5,
        "spot": 22100.0,
        "atm_strike": 22100,
    }
    db.insert_iv_history([row])
    result = db.conn.execute("SELECT * FROM iv_history").fetchall()
    assert len(result) == 1


def test_get_raw_data_by_date(db):
    rows = [
        {
            "timestamp": "2026-03-16 09:15:00",
            "date": "2026-03-16",
            "time": "09:15:00",
            "expiry_date": "2026-03-30",
            "strike": 23200,
            "option_type": "CE",
            "open": 500.0,
            "high": 510.0,
            "low": 495.0,
            "close": 505.0,
            "volume": 1000,
            "oi": 50000,
            "iv": 25.5,
            "spot": 23250.0,
            "atm_offset": 0,
        },
        {
            "timestamp": "2026-03-17 09:15:00",
            "date": "2026-03-17",
            "time": "09:15:00",
            "expiry_date": "2026-03-30",
            "strike": 23200,
            "option_type": "CE",
            "open": 510.0,
            "high": 520.0,
            "low": 505.0,
            "close": 515.0,
            "volume": 1200,
            "oi": 52000,
            "iv": 26.0,
            "spot": 23300.0,
            "atm_offset": 0,
        },
    ]
    db.insert_raw_option_data(rows)
    result = db.get_raw_data_by_date("2026-03-16")
    assert len(result) == 1
    assert result[0]["close"] == 505.0
