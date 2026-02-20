"""
transform.py — Parse raw Open-Meteo JSON into clean DataFrames.

Produces:
  - raw_df   : one row per day, matching the raw_weather table schema
  - summary_df: one row per day, matching the daily_summary table schema
"""

import logging
import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

CITY = "Singapore"


def transform(raw_data: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Transform raw API response into (raw_df, summary_df).

    Parameters
    ----------
    raw_data : dict
        JSON response from Open-Meteo extract step.

    Returns
    -------
    tuple[pd.DataFrame, pd.DataFrame]
        (raw_df, summary_df)
    """
    daily = raw_data.get("daily", {})

    raw_df = pd.DataFrame({
        "city":            CITY,
        "date":            pd.to_datetime(daily["time"]),
        "temperature_max": daily["temperature_2m_max"],
        "temperature_min": daily["temperature_2m_min"],
        "precipitation":   daily["precipitation_sum"],
        "windspeed_max":   daily["windspeed_10m_max"],
    })

    # Only fill precipitation with 0 — null on dry days is expected.
    # Other columns (temperature, windspeed) should not be silently zeroed.
    raw_df["precipitation"] = raw_df["precipitation"].fillna(0)

    log.info("Transformed %d rows into raw_df", len(raw_df))

    summary_df = pd.DataFrame({
        "city":                raw_df["city"],
        "date":                raw_df["date"],
        "avg_temperature":     (raw_df["temperature_max"] + raw_df["temperature_min"]) / 2,
        "total_precipitation": raw_df["precipitation"],
        "max_windspeed":       raw_df["windspeed_max"],
    })

    log.info("Built summary_df with %d rows", len(summary_df))
    return raw_df, summary_df


if __name__ == "__main__":
    from extract import extract
    raw, summary = transform(extract())
    print("=== raw_df ===")
    print(raw)
    print("\n=== summary_df ===")
    print(summary)
