"""
Unit tests for pipeline/transform.py.

Uses a mock API response so no network or database connection is needed.
"""

import pandas as pd
import pytest

from pipeline.transform import transform

# ── Fixtures ───────────────────────────────────────────────────────────────────

MOCK_API_RESPONSE = {
    "daily": {
        "time":                ["2024-01-01", "2024-01-02", "2024-01-03"],
        "temperature_2m_max":  [32.0,         33.5,         31.0],
        "temperature_2m_min":  [25.0,         26.0,         24.5],
        "precipitation_sum":   [0.0,          5.2,          None],   # None → should become 0
        "windspeed_10m_max":   [15.0,         20.0,         12.5],
    }
}

MOCK_EMPTY_RESPONSE = {
    "daily": {
        "time":                [],
        "temperature_2m_max":  [],
        "temperature_2m_min":  [],
        "precipitation_sum":   [],
        "windspeed_10m_max":   [],
    }
}


# ── Tests: return types ────────────────────────────────────────────────────────

def test_transform_returns_tuple():
    result = transform(MOCK_API_RESPONSE)
    assert isinstance(result, tuple) and len(result) == 2


def test_transform_returns_dataframes():
    raw_df, summary_df = transform(MOCK_API_RESPONSE)
    assert isinstance(raw_df, pd.DataFrame)
    assert isinstance(summary_df, pd.DataFrame)


# ── Tests: raw_df schema ──────────────────────────────────────────────────────

def test_raw_df_columns():
    raw_df, _ = transform(MOCK_API_RESPONSE)
    expected = {"city", "date", "temperature_max", "temperature_min", "precipitation", "windspeed_max"}
    assert expected.issubset(set(raw_df.columns))


def test_raw_df_row_count():
    raw_df, _ = transform(MOCK_API_RESPONSE)
    assert len(raw_df) == 3


def test_raw_df_city_value():
    raw_df, _ = transform(MOCK_API_RESPONSE)
    assert (raw_df["city"] == "Singapore").all()


def test_raw_df_date_dtype():
    raw_df, _ = transform(MOCK_API_RESPONSE)
    assert pd.api.types.is_datetime64_any_dtype(raw_df["date"])


# ── Tests: null handling ──────────────────────────────────────────────────────

def test_null_precipitation_filled_with_zero():
    raw_df, _ = transform(MOCK_API_RESPONSE)
    assert raw_df["precipitation"].isna().sum() == 0
    assert raw_df.loc[2, "precipitation"] == 0.0


# ── Tests: summary_df schema ──────────────────────────────────────────────────

def test_summary_df_columns():
    _, summary_df = transform(MOCK_API_RESPONSE)
    expected = {"city", "date", "avg_temperature", "total_precipitation", "max_windspeed"}
    assert expected.issubset(set(summary_df.columns))


def test_summary_df_row_count():
    _, summary_df = transform(MOCK_API_RESPONSE)
    assert len(summary_df) == 3


# ── Tests: calculations ────────────────────────────────────────────────────────

def test_avg_temperature_calculation():
    _, summary_df = transform(MOCK_API_RESPONSE)
    expected = (32.0 + 25.0) / 2   # first row
    assert summary_df.iloc[0]["avg_temperature"] == pytest.approx(expected)


def test_total_precipitation_matches_raw():
    raw_df, summary_df = transform(MOCK_API_RESPONSE)
    assert list(summary_df["total_precipitation"]) == list(raw_df["precipitation"])


def test_max_windspeed_matches_raw():
    raw_df, summary_df = transform(MOCK_API_RESPONSE)
    assert list(summary_df["max_windspeed"]) == list(raw_df["windspeed_max"])


# ── Tests: edge cases ────────────────────────────────────────────────────────

def test_empty_api_response_returns_empty_dataframes():
    raw_df, summary_df = transform(MOCK_EMPTY_RESPONSE)
    assert len(raw_df) == 0
    assert len(summary_df) == 0
