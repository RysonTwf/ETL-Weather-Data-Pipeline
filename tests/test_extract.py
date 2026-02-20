"""
Unit tests for pipeline/extract.py.

Patches _build_session so no real HTTP calls are made.
"""

from unittest.mock import patch, MagicMock

import pytest

from pipeline.extract import extract

MOCK_RESPONSE_DATA = {
    "daily": {
        "time":               ["2024-01-01", "2024-01-02"],
        "temperature_2m_max": [32.0,          33.0],
        "temperature_2m_min": [25.0,          26.0],
        "precipitation_sum":  [0.0,           5.0],
        "windspeed_10m_max":  [15.0,          20.0],
    }
}


def _mock_session(data=MOCK_RESPONSE_DATA):
    """Return a mock session whose .get() returns a successful response."""
    mock_resp = MagicMock()
    mock_resp.json.return_value = data
    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp
    return mock_session


# ── return type ──────────────────────────────────────────────────────────────

def test_extract_returns_dict():
    with patch("pipeline.extract._build_session", return_value=_mock_session()):
        result = extract()
    assert isinstance(result, dict)


def test_extract_contains_daily_key():
    with patch("pipeline.extract._build_session", return_value=_mock_session()):
        result = extract()
    assert "daily" in result


# ── response shape ───────────────────────────────────────────────────────────

def test_extract_daily_has_expected_fields():
    with patch("pipeline.extract._build_session", return_value=_mock_session()):
        result = extract()
    daily = result["daily"]
    for field in ("time", "temperature_2m_max", "temperature_2m_min",
                  "precipitation_sum", "windspeed_10m_max"):
        assert field in daily


def test_extract_returns_correct_day_count():
    with patch("pipeline.extract._build_session", return_value=_mock_session()):
        result = extract()
    assert len(result["daily"]["time"]) == 2


# ── HTTP behaviour ───────────────────────────────────────────────────────────

def test_extract_calls_open_meteo_url():
    mock_session = _mock_session()
    with patch("pipeline.extract._build_session", return_value=mock_session):
        extract()
    url = mock_session.get.call_args[0][0]
    assert "open-meteo.com" in url


def test_extract_passes_singapore_coordinates():
    mock_session = _mock_session()
    with patch("pipeline.extract._build_session", return_value=mock_session):
        extract()
    kwargs = mock_session.get.call_args[1]
    params = kwargs.get("params", {})
    assert params["latitude"] == 1.29
    assert params["longitude"] == 103.85


def test_extract_raises_on_http_error():
    mock_resp = MagicMock()
    mock_resp.raise_for_status.side_effect = Exception("HTTP 500")
    mock_session = MagicMock()
    mock_session.get.return_value = mock_resp
    with patch("pipeline.extract._build_session", return_value=mock_session):
        with pytest.raises(Exception):
            extract()
