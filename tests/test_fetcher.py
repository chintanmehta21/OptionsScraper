# tests/test_fetcher.py
import pytest
from unittest.mock import MagicMock, patch
from DhanHQ_src.fetcher import parse_api_response, build_raw_rows


def make_mock_response():
    """Simulates DhanHQ API response format (parallel arrays)."""
    return {
        "open": [500.0, 505.0, 502.0],
        "high": [510.0, 512.0, 508.0],
        "low": [495.0, 500.0, 498.0],
        "close": [505.0, 508.0, 504.0],
        "volume": [1000, 1200, 800],
        "oi": [50000, 50500, 50200],
        "iv": [25.5, 25.8, 25.3],
        "spot": [23250.0, 23260.0, 23255.0],
        "strike": [23200, 23200, 23200],
        "timestamp": [1742108100, 1742108160, 1742108220],
    }


def test_parse_api_response_row_count():
    response = make_mock_response()
    rows = parse_api_response(response)
    assert len(rows) == 3


def test_parse_api_response_row_fields():
    response = make_mock_response()
    rows = parse_api_response(response)
    row = rows[0]
    assert row["open"] == 500.0
    assert row["close"] == 505.0
    assert row["volume"] == 1000
    assert row["oi"] == 50000
    assert row["iv"] == 25.5
    assert row["spot"] == 23250.0
    assert row["strike"] == 23200


def test_parse_api_response_timestamp_conversion():
    response = make_mock_response()
    rows = parse_api_response(response)
    assert "timestamp" in rows[0]
    assert "date" in rows[0]
    assert "time" in rows[0]


def test_build_raw_rows_adds_metadata():
    response = make_mock_response()
    parsed = parse_api_response(response)
    rows = build_raw_rows(parsed, option_type="CE", atm_offset=0, expiry_date="2026-03-30")
    assert rows[0]["option_type"] == "CE"
    assert rows[0]["atm_offset"] == 0
    assert rows[0]["expiry_date"] == "2026-03-30"


def test_parse_empty_response():
    response = {
        "open": [], "high": [], "low": [], "close": [],
        "volume": [], "oi": [], "iv": [], "spot": [],
        "strike": [], "timestamp": [],
    }
    rows = parse_api_response(response)
    assert len(rows) == 0
