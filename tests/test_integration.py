# tests/test_integration.py
import os
import tempfile
import pytest
from unittest.mock import patch, MagicMock
from DhanHQ_src.db import Database
from DhanHQ_src.fetcher import parse_api_response, build_raw_rows
from DhanHQ_src.calculator import compute_derived_metrics, compute_aggregate_metrics


@pytest.fixture
def db():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    database = Database(path)
    database.create_tables()
    yield database
    database.close()
    os.unlink(path)


def test_full_pipeline_mock(db):
    """Test the full pipeline with mock data: fetch -> parse -> store -> calculate."""
    # Simulate API response for one strike CE
    ce_response = {
        "open": [500.0, 495.0],
        "high": [510.0, 505.0],
        "low": [495.0, 490.0],
        "close": [505.0, 498.0],
        "volume": [1000, 1200],
        "oi": [50000, 50500],
        "iv": [25.5, 25.8],
        "spot": [23250.0, 23260.0],
        "strike": [23200, 23200],
        "timestamp": [1742108100, 1742108160],
    }
    # Simulate API response for one strike PE
    pe_response = {
        "open": [400.0, 410.0],
        "high": [415.0, 420.0],
        "low": [395.0, 405.0],
        "close": [410.0, 415.0],
        "volume": [800, 900],
        "oi": [60000, 60200],
        "iv": [24.8, 25.0],
        "spot": [23250.0, 23260.0],
        "strike": [23200, 23200],
        "timestamp": [1742108100, 1742108160],
    }

    # Parse and build rows
    ce_parsed = parse_api_response(ce_response)
    ce_rows = build_raw_rows(ce_parsed, "CALL", 0, "2026-03-30")

    pe_parsed = parse_api_response(pe_response)
    pe_rows = build_raw_rows(pe_parsed, "PUT", 0, "2026-03-30")

    all_rows = ce_rows + pe_rows

    # Store raw data
    db.insert_raw_option_data(all_rows)
    stored = db.get_raw_data_ordered()
    assert len(stored) == 4  # 2 timestamps x 2 option types

    # Compute derived metrics
    derived = compute_derived_metrics(all_rows)
    assert len(derived) == 2  # 2 timestamps x 1 strike
    db.insert_derived_metrics(derived)

    # Compute aggregate metrics
    agg = compute_aggregate_metrics(derived[:1], 23250.0, None, [20.0, 22.0, 25.0], "2026-03-30")
    assert agg["total_ce_oi"] == 50000
    assert agg["total_pe_oi"] == 60000
    assert agg["overall_pcr"] > 0
    db.insert_aggregate_metrics([agg])

    # Verify data was stored
    result = db.conn.execute("SELECT COUNT(*) FROM aggregate_metrics").fetchone()
    assert result[0] == 1
