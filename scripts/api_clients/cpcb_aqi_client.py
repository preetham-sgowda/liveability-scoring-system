"""
cpcb_aqi_client.py — CPCB AQI REST API client.

Fetches daily air quality data (PM2.5, PM10, NO2, SO2, CO, O3, AQI)
from CPCB monitoring stations in Bengaluru.

Handles:
  - Station listing
  - Daily readings with retry + exponential backoff
  - Rate limiting
"""

import os
import time
import logging
from datetime import datetime, date, timedelta
from typing import Optional

import requests

logger = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://app.cpcbccr.com/ccr_docs/ccr_doc/api"
BENGALURU_STATIONS = [
    {"id": "site_5029", "name": "BTM Layout", "lat": 12.9165, "lon": 77.6101},
    {"id": "site_5030", "name": "Peenya", "lat": 13.0285, "lon": 77.5192},
    {"id": "site_5031", "name": "Silk Board", "lat": 12.9173, "lon": 77.6228},
    {"id": "site_5032", "name": "Jayanagar", "lat": 12.9299, "lon": 77.5838},
    {"id": "site_5033", "name": "Hebbal", "lat": 13.0358, "lon": 77.5970},
    {"id": "site_5034", "name": "Bapuji Nagar", "lat": 12.9519, "lon": 77.5381},
    {"id": "site_5035", "name": "Hombegowda Nagar", "lat": 12.9400, "lon": 77.5900},
    {"id": "site_5036", "name": "City Railway Station", "lat": 12.9784, "lon": 77.5710},
]

MAX_RETRIES = 3
BACKOFF_FACTOR = 2
REQUEST_TIMEOUT = 30


class CpcbAqiClient:
    """Client for the CPCB Central Control Room AQI API."""

    def __init__(self, api_key: Optional[str] = None,
                 base_url: Optional[str] = None):
        self.api_key = api_key or os.getenv("CPCB_API_KEY", "")
        self.base_url = base_url or os.getenv(
            "CPCB_API_BASE_URL", DEFAULT_BASE_URL
        )
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Authorization": f"Bearer {self.api_key}" if self.api_key else "",
        })

    def fetch_station_data(self, station_id: str, station_name: str,
                           target_date: date, lat: float,
                           lon: float) -> Optional[dict]:
        """
        Fetch daily AQI data for a single station.

        Args:
            station_id: CPCB station identifier
            station_name: Human-readable station name
            target_date: Date to fetch data for
            lat: Station latitude
            lon: Station longitude

        Returns:
            Dict with AQI readings or None on failure
        """
        params = {
            "station_id": station_id,
            "date": target_date.isoformat(),
            "format": "json",
        }

        for attempt in range(MAX_RETRIES):
            try:
                response = self._make_request(
                    f"{self.base_url}/station_data", params
                )
                if response is None:
                    continue

                return self._parse_station_response(
                    response, station_id, station_name, target_date, lat, lon
                )

            except requests.exceptions.RequestException as e:
                wait_time = BACKOFF_FACTOR ** attempt
                logger.warning(
                    f"Request failed for {station_name} "
                    f"(attempt {attempt + 1}/{MAX_RETRIES}): {e}. "
                    f"Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)

        logger.error(
            f"Failed to fetch data for station {station_name} "
            f"after {MAX_RETRIES} attempts"
        )
        return None

    def fetch_daily_readings(self, target_date: date) -> list[dict]:
        """
        Fetch AQI data for all Bengaluru stations for a given date.

        Args:
            target_date: Date to fetch readings for

        Returns:
            List of station reading dicts
        """
        logger.info(
            f"Fetching AQI data for {len(BENGALURU_STATIONS)} stations "
            f"on {target_date.isoformat()}"
        )
        readings = []

        for station in BENGALURU_STATIONS:
            reading = self.fetch_station_data(
                station_id=station["id"],
                station_name=station["name"],
                target_date=target_date,
                lat=station["lat"],
                lon=station["lon"],
            )
            if reading:
                readings.append(reading)

            # Rate limiting: 500ms between requests
            time.sleep(0.5)

        logger.info(
            f"Fetched {len(readings)}/{len(BENGALURU_STATIONS)} "
            f"station readings for {target_date}"
        )
        return readings

    def fetch_date_range(self, start_date: date,
                         end_date: date) -> list[dict]:
        """
        Fetch AQI data for a date range.

        Args:
            start_date: Start date (inclusive)
            end_date: End date (inclusive)

        Returns:
            List of all readings across all dates and stations
        """
        all_readings = []
        current = start_date

        while current <= end_date:
            readings = self.fetch_daily_readings(current)
            all_readings.extend(readings)
            current += timedelta(days=1)

        logger.info(
            f"Fetched {len(all_readings)} total readings "
            f"from {start_date} to {end_date}"
        )
        return all_readings

    def _make_request(self, url: str,
                      params: dict) -> Optional[dict]:
        """Make an HTTP request with timeout and error handling."""
        try:
            response = self.session.get(
                url, params=params, timeout=REQUEST_TIMEOUT
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429:
                logger.warning("Rate limited by CPCB API, backing off...")
                time.sleep(5)
            raise
        except ValueError:
            logger.error(f"Invalid JSON response from {url}")
            return None

    def _parse_station_response(self, data: dict, station_id: str,
                                station_name: str, target_date: date,
                                lat: float, lon: float) -> Optional[dict]:
        """
        Parse the API response into a standardized record.

        Handles various CPCB API response formats.
        """
        try:
            # CPCB API returns different structures depending on endpoint
            readings = data.get("records", data.get("data", []))

            if isinstance(readings, list) and readings:
                record = readings[0] if isinstance(readings[0], dict) else {}
            elif isinstance(readings, dict):
                record = readings
            else:
                return None

            return {
                "station_id": station_id,
                "station_name": station_name,
                "city": "Bengaluru",
                "latitude": lat,
                "longitude": lon,
                "date": target_date.isoformat(),
                "pm25": self._safe_float(record.get("pm25", record.get("PM2.5"))),
                "pm10": self._safe_float(record.get("pm10", record.get("PM10"))),
                "no2": self._safe_float(record.get("no2", record.get("NO2"))),
                "so2": self._safe_float(record.get("so2", record.get("SO2"))),
                "co": self._safe_float(record.get("co", record.get("CO"))),
                "o3": self._safe_float(record.get("o3", record.get("Ozone"))),
                "aqi": self._safe_int(record.get("aqi", record.get("AQI"))),
                "prominent_pollutant": record.get(
                    "prominent_pollutant",
                    record.get("predominant", "")
                ),
            }
        except (KeyError, TypeError, IndexError) as e:
            logger.warning(
                f"Failed to parse response for {station_name}: {e}"
            )
            return None

    @staticmethod
    def _safe_float(value) -> Optional[float]:
        """Safely convert a value to float."""
        if value is None or value == "" or value == "NA":
            return None
        try:
            return round(float(value), 2)
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _safe_int(value) -> Optional[int]:
        """Safely convert a value to int."""
        if value is None or value == "" or value == "NA":
            return None
        try:
            return int(float(value))
        except (ValueError, TypeError):
            return None


def get_prepare_rows(readings: list[dict]) -> tuple[list[str], list[tuple]]:
    """
    Prepare AQI readings for bulk DB insertion.

    Returns:
        Tuple of (column_names, row_tuples)
    """
    columns = [
        "station_id", "station_name", "city", "latitude", "longitude",
        "date", "pm25", "pm10", "no2", "so2", "co", "o3",
        "aqi", "prominent_pollutant"
    ]
    rows = [
        (
            r["station_id"], r["station_name"], r["city"],
            r["latitude"], r["longitude"], r["date"],
            r["pm25"], r["pm10"], r["no2"], r["so2"],
            r["co"], r["o3"], r["aqi"], r["prominent_pollutant"]
        )
        for r in readings
    ]
    return columns, rows
