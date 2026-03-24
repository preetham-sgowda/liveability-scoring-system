"""
test_cpcb_client.py — Unit tests for the CPCB AQI API client.

Tests response parsing, safe conversions, and row preparation
using mocked API responses.
"""

import pytest
from unittest.mock import patch, MagicMock
from datetime import date

from scripts.api_clients.cpcb_aqi_client import (
    CpcbAqiClient,
    get_prepare_rows,
    BENGALURU_STATIONS,
)


class TestSafeConversions:
    """Test static helper methods."""

    def test_safe_float_valid(self):
        assert CpcbAqiClient._safe_float("42.5") == 42.5

    def test_safe_float_none(self):
        assert CpcbAqiClient._safe_float(None) is None

    def test_safe_float_na(self):
        assert CpcbAqiClient._safe_float("NA") is None

    def test_safe_float_empty(self):
        assert CpcbAqiClient._safe_float("") is None

    def test_safe_int_valid(self):
        assert CpcbAqiClient._safe_int("150") == 150

    def test_safe_int_float_string(self):
        assert CpcbAqiClient._safe_int("150.7") == 150

    def test_safe_int_none(self):
        assert CpcbAqiClient._safe_int(None) is None


class TestParseStationResponse:
    """Test API response parsing."""

    def setup_method(self):
        self.client = CpcbAqiClient(api_key="test_key")

    def test_parses_valid_response(self):
        data = {
            "records": [{
                "pm25": "45.2",
                "pm10": "89.1",
                "no2": "32.5",
                "so2": "12.3",
                "co": "1.2",
                "o3": "28.4",
                "aqi": "120",
                "prominent_pollutant": "PM2.5",
            }]
        }
        result = self.client._parse_station_response(
            data, "site_5029", "BTM Layout",
            date(2024, 1, 15), 12.9165, 77.6101
        )

        assert result is not None
        assert result["station_id"] == "site_5029"
        assert result["pm25"] == 45.2
        assert result["aqi"] == 120
        assert result["city"] == "Bengaluru"

    def test_handles_empty_records(self):
        data = {"records": []}
        result = self.client._parse_station_response(
            data, "site_5029", "BTM Layout",
            date(2024, 1, 15), 12.9165, 77.6101
        )
        assert result is None

    def test_handles_alternative_keys(self):
        data = {
            "data": [{
                "PM2.5": "50.0",
                "PM10": "100.0",
                "NO2": "25.0",
                "SO2": "10.0",
                "CO": "1.0",
                "Ozone": "30.0",
                "AQI": "95",
                "predominant": "PM10",
            }]
        }
        result = self.client._parse_station_response(
            data, "site_5030", "Peenya",
            date(2024, 1, 15), 13.0285, 77.5192
        )

        assert result is not None
        assert result["pm25"] == 50.0
        assert result["o3"] == 30.0


class TestFetchDailyReadings:
    """Test daily readings with mocked HTTP."""

    @patch.object(CpcbAqiClient, "fetch_station_data")
    def test_fetches_all_stations(self, mock_fetch):
        mock_fetch.return_value = {
            "station_id": "test",
            "pm25": 45.0,
            "aqi": 120,
        }

        client = CpcbAqiClient(api_key="test")
        readings = client.fetch_daily_readings(date(2024, 1, 15))

        assert len(readings) == len(BENGALURU_STATIONS)
        assert mock_fetch.call_count == len(BENGALURU_STATIONS)


class TestGetPrepareRows:
    def test_prepares_valid_rows(self):
        readings = [{
            "station_id": "site_5029",
            "station_name": "BTM Layout",
            "city": "Bengaluru",
            "latitude": 12.9165,
            "longitude": 77.6101,
            "date": "2024-01-15",
            "pm25": 45.2,
            "pm10": 89.1,
            "no2": 32.5,
            "so2": 12.3,
            "co": 1.2,
            "o3": 28.4,
            "aqi": 120,
            "prominent_pollutant": "PM2.5",
        }]
        columns, rows = get_prepare_rows(readings)
        assert len(columns) == 14
        assert len(rows) == 1
        assert rows[0][0] == "site_5029"

    def test_empty_readings(self):
        columns, rows = get_prepare_rows([])
        assert len(rows) == 0
