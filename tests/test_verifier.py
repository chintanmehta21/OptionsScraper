# tests/test_verifier.py
import os
import tempfile
import pytest
from DhanHQ_src.verifier import parse_bhavcopy_csv, compare_values


SAMPLE_BHAVCOPY_CSV = """INSTRUMENT,SYMBOL,EXPIRY_DT,STRIKE_PR,OPTION_TYP,OPEN,HIGH,LOW,CLOSE,SETTLE_PR,CONTRACTS,VAL_INLAKH,OPEN_INT,CHG_IN_OI,TIMESTAMP
OPTIDX,NIFTY,30-Mar-2026,23100.00,CE,560.00,570.00,540.00,550.60,550.60,5000,75000.00,3594,500,16-Mar-2026
OPTIDX,NIFTY,30-Mar-2026,23100.00,PE,390.00,405.00,385.00,399.80,399.80,4000,60000.00,15920,200,16-Mar-2026
OPTIDX,NIFTY,30-Mar-2026,23200.00,CE,500.00,515.00,490.00,491.90,491.90,6000,90000.00,11552,800,16-Mar-2026
OPTIDX,NIFTY,30-Mar-2026,23200.00,PE,430.00,450.00,425.00,441.00,441.00,3500,52500.00,20428,300,16-Mar-2026
"""


def test_parse_bhavcopy_csv():
    fd, path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    with open(path, "w") as f:
        f.write(SAMPLE_BHAVCOPY_CSV)

    result = parse_bhavcopy_csv(path, symbol="NIFTY", expiry="30-Mar-2026")
    os.unlink(path)

    assert len(result) == 4
    assert result[0]["strike"] == 23100
    assert result[0]["option_type"] == "CE"
    assert result[0]["close"] == 550.60
    assert result[0]["oi"] == 3594


def test_compare_values_match():
    dhan = {"close": 550.60, "oi": 3594, "volume": 5000}
    nse = {"close": 550.60, "oi": 3594, "volume": 5000}
    result = compare_values(dhan, nse, close_tolerance=0.05)
    assert result["close_match"] is True
    assert result["oi_match"] is True
    assert result["volume_match"] is True


def test_compare_values_close_within_tolerance():
    dhan = {"close": 550.60, "oi": 3594, "volume": 5000}
    nse = {"close": 550.63, "oi": 3594, "volume": 5000}
    result = compare_values(dhan, nse, close_tolerance=0.05)
    assert result["close_match"] is True


def test_compare_values_mismatch():
    dhan = {"close": 550.60, "oi": 3594, "volume": 5000}
    nse = {"close": 555.00, "oi": 3600, "volume": 5100}
    result = compare_values(dhan, nse, close_tolerance=0.05)
    assert result["close_match"] is False
    assert result["oi_match"] is False
    assert result["volume_match"] is False
