"""
load.py — Upsert transformed DataFrame into PostgreSQL raw_weather table.

Uses ON CONFLICT (city, date) DO NOTHING so re-running the DAG never
produces duplicate rows 

daily_summary is now built by dbt (see dbt/models/daily_summary.sql).
"""

import logging
import os

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.dialects.postgresql import insert as pg_insert

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

DEFAULT_DB_URL = "postgresql+psycopg2://admin:password@localhost:5432/weather"


def get_engine():
    db_url = os.getenv("WEATHER_DB_CONN", DEFAULT_DB_URL)
    return create_engine(db_url)


def upsert_raw(engine, raw_df: pd.DataFrame) -> int:
    """
    Insert rows into raw_weather; skip any (city, date) pair that already exists.

    Returns the number of rows inserted.
    """
    meta = MetaData()
    meta.reflect(bind=engine, only=["raw_weather"])
    table: Table = meta.tables["raw_weather"]

    records = raw_df.to_dict(orient="records")

    with engine.begin() as conn:
        stmt = pg_insert(table).values(records)
        stmt = stmt.on_conflict_do_nothing(index_elements=["city", "date"])
        result = conn.execute(stmt)

    inserted = result.rowcount
    log.info("Upserted %d new rows into raw_weather (skipped duplicates)", inserted)
    return inserted


def load(raw_df: pd.DataFrame) -> None:
    """
    Load raw_df into raw_weather with duplicate protection.

    Raises
    ------
    RuntimeError
        If the table is empty after the upsert (data quality gate).
    """
    engine = get_engine()
    upsert_raw(engine, raw_df)

    with engine.connect() as conn:
        from sqlalchemy import text
        count = conn.execute(text("SELECT COUNT(*) FROM raw_weather")).scalar()

    log.info("raw_weather total rows: %d", count)
    if count == 0:
        raise RuntimeError("Data quality gate FAILED: raw_weather is empty after load")

    log.info("Load complete")


if __name__ == "__main__":
    from extract import extract
    from transform import transform

    raw_df, _ = transform(extract())
    load(raw_df)
