"""
extract.py — Fetch raw weather data from Open-Meteo API.

City: Singapore (lat 1.29, lon 103.85)
Fetches the last 7 days of daily weather data; no API key required.
"""

import logging
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

API_URL = "https://api.open-meteo.com/v1/forecast"
CITY = "Singapore"
LATITUDE = 1.29
LONGITUDE = 103.85


def _build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def extract() -> dict:
    """Call Open-Meteo and return the raw JSON response."""
    params = {
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "windspeed_10m_max",
        ],
        "past_days": 7,
        "timezone": "Asia/Singapore",
    }

    log.info("Fetching weather data for %s (lat=%s, lon=%s)", CITY, LATITUDE, LONGITUDE)
    response = _build_session().get(API_URL, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    days_returned = len(data.get("daily", {}).get("time", []))
    log.info("Received %d days of data", days_returned)
    return data


if __name__ == "__main__":
    raw = extract()
    print(raw)
