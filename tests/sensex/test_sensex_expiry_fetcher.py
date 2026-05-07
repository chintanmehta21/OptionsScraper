import os
from pathlib import Path

import pytest
from unittest.mock import patch, MagicMock

from DhanHQ_src.Sensex_Scraping.expiry_fetcher import (
    _extract_sensex_expiry_dates,
    _download_bse_bhavcopy,
    fetch_all_expiry_dates,
)


FIXTURE_PATH = Path(__file__).parent / "fixtures" / "bse_fo_sample.csv"


class TestExtractSensexExpiryDates:
    def test_extracts_only_sensex_index_options(self):
        result = _extract_sensex_expiry_dates(str(FIXTURE_PATH))
        # Three unique SENSEX IDO XpryDt values; futures, BANKEX, stock options excluded.
        assert result == ["2026-04-30", "2026-05-07", "2026-06-25"]

    def test_returns_sorted_unique(self):
        result = _extract_sensex_expiry_dates(str(FIXTURE_PATH))
        assert result == sorted(set(result))

    def test_missing_file_returns_empty(self, tmp_path):
        missing = tmp_path / "does_not_exist.csv"
        result = _extract_sensex_expiry_dates(str(missing))
        assert result == []


class TestDownloadBseBhavcopy:
    def test_returns_cached_path_when_exists(self, tmp_path, monkeypatch):
        # Pre-create a cached file in a temp BHAVCOPY_DIR.
        monkeypatch.setattr(
            "DhanHQ_src.Sensex_Scraping.expiry_fetcher.BHAVCOPY_DIR",
            str(tmp_path),
        )
        cached = tmp_path / "bse_fo_2026-04-30.csv"
        cached.write_text("TradDt,...,TckrSymb,...\nrow", encoding="utf-8")

        result = _download_bse_bhavcopy("2026-04-30")
        assert result == str(cached)

    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher.requests.get")
    def test_html_404_response_returns_none(self, mock_get, tmp_path, monkeypatch):
        """BSE returns HTTP 200 with HTML body for missing dates — must be detected."""
        monkeypatch.setattr(
            "DhanHQ_src.Sensex_Scraping.expiry_fetcher.BHAVCOPY_DIR",
            str(tmp_path),
        )
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = b"<html><body>page not found</body></html>"
        mock_get.return_value = mock_resp

        result = _download_bse_bhavcopy("2020-01-01")
        assert result is None
        # Must NOT cache the HTML body.
        assert not (tmp_path / "bse_fo_2020-01-01.csv").exists()

    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher.requests.get")
    def test_valid_csv_response_caches_and_returns_path(self, mock_get, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "DhanHQ_src.Sensex_Scraping.expiry_fetcher.BHAVCOPY_DIR",
            str(tmp_path),
        )
        # Real CSV starts with the bhavcopy header.
        csv_body = FIXTURE_PATH.read_bytes()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.content = csv_body
        mock_get.return_value = mock_resp

        result = _download_bse_bhavcopy("2026-04-30")
        assert result == str(tmp_path / "bse_fo_2026-04-30.csv")
        assert (tmp_path / "bse_fo_2026-04-30.csv").read_bytes() == csv_body


class TestFetchAllExpiryDates:
    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher._download_bse_bhavcopy")
    def test_returns_classified_expiries(self, mock_download):
        # Make every probed date hit the same fixture, only succeed for 2026 dates
        # and only on the first attempt per month so the inner break fires.
        def _side(date_str):
            if date_str.startswith("2026-"):
                return str(FIXTURE_PATH)
            return None
        mock_download.side_effect = _side

        result = fetch_all_expiry_dates(2026)

        # Must contain WEEK entries for each 2026 SENSEX expiry from the fixture
        # and MONTH entries for the last date in each month present (4-30, 5-07, 6-25
        # — each is the last in its month within this fixture, so all become monthly).
        dates = sorted({(e["expiry_date"], e["expiry_flag"]) for e in result})
        assert ("2026-04-30", "WEEK") in dates
        assert ("2026-04-30", "MONTH") in dates
        assert ("2026-05-07", "WEEK") in dates
        assert ("2026-06-25", "WEEK") in dates
        # Schema check on one entry
        first = result[0]
        assert set(first.keys()) == {"expiry_date", "expiry_flag", "from_date", "to_date"}

    @patch("DhanHQ_src.Sensex_Scraping.expiry_fetcher._download_bse_bhavcopy", return_value=None)
    def test_no_bhavcopy_returns_empty(self, mock_download):
        result = fetch_all_expiry_dates(2022)
        assert result == []
