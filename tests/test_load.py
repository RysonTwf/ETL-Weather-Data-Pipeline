"""
Unit tests for pipeline/load.py.

Uses mock SQLAlchemy engine — no real database connection needed.
"""

from unittest.mock import patch, MagicMock, call

import pandas as pd
import pytest

from pipeline.load import upsert_raw, load

SAMPLE_DF = pd.DataFrame({
    "city":            ["Singapore", "Singapore"],
    "date":            ["2024-01-01", "2024-01-02"],
    "temperature_max": [32.0,         33.0],
    "temperature_min": [25.0,         26.0],
    "precipitation":   [0.0,          5.0],
    "windspeed_max":   [15.0,         20.0],
})


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_begin_conn(rowcount=2):
    """Mock connection for engine.begin() context manager."""
    mock_result = MagicMock()
    mock_result.rowcount = rowcount
    mock_conn = MagicMock()
    mock_conn.execute.return_value = mock_result
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    return mock_conn


def _make_connect_conn(scalar_value=5):
    """Mock connection for engine.connect() context manager."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.scalar.return_value = scalar_value
    return mock_conn


# ── upsert_raw ───────────────────────────────────────────────────────────────

def test_upsert_raw_returns_rowcount():
    mock_conn = _make_begin_conn(rowcount=2)
    mock_engine = MagicMock()
    mock_engine.begin.return_value = mock_conn

    with patch("pipeline.load.MetaData") as mock_meta_cls, \
         patch("pipeline.load.pg_insert"):
        mock_meta = MagicMock()
        mock_meta_cls.return_value = mock_meta
        mock_meta.tables = {"raw_weather": MagicMock()}
        result = upsert_raw(mock_engine, SAMPLE_DF)

    assert result == 2


def test_upsert_raw_uses_on_conflict_do_nothing():
    mock_conn = _make_begin_conn(rowcount=1)
    mock_engine = MagicMock()
    mock_engine.begin.return_value = mock_conn

    with patch("pipeline.load.MetaData") as mock_meta_cls, \
         patch("pipeline.load.pg_insert") as mock_insert:
        mock_meta = MagicMock()
        mock_meta_cls.return_value = mock_meta
        mock_meta.tables = {"raw_weather": MagicMock()}

        mock_stmt = MagicMock()
        mock_insert.return_value.values.return_value = mock_stmt
        mock_stmt.on_conflict_do_nothing.return_value = mock_stmt

        upsert_raw(mock_engine, SAMPLE_DF)

    mock_stmt.on_conflict_do_nothing.assert_called_once_with(
        index_elements=["city", "date"]
    )


def test_upsert_raw_returns_zero_for_all_duplicates():
    mock_conn = _make_begin_conn(rowcount=0)
    mock_engine = MagicMock()
    mock_engine.begin.return_value = mock_conn

    with patch("pipeline.load.MetaData") as mock_meta_cls, \
         patch("pipeline.load.pg_insert"):
        mock_meta = MagicMock()
        mock_meta_cls.return_value = mock_meta
        mock_meta.tables = {"raw_weather": MagicMock()}
        result = upsert_raw(mock_engine, SAMPLE_DF)

    assert result == 0


# ── load ─────────────────────────────────────────────────────────────────────

def test_load_raises_if_table_empty():
    mock_engine = MagicMock()
    mock_engine.connect.return_value = _make_connect_conn(scalar_value=0)

    with patch("pipeline.load.get_engine", return_value=mock_engine), \
         patch("pipeline.load.upsert_raw", return_value=0):
        with pytest.raises(RuntimeError, match="Data quality gate FAILED"):
            load(SAMPLE_DF)


def test_load_succeeds_when_rows_exist():
    mock_engine = MagicMock()
    mock_engine.connect.return_value = _make_connect_conn(scalar_value=5)

    with patch("pipeline.load.get_engine", return_value=mock_engine), \
         patch("pipeline.load.upsert_raw", return_value=2):
        load(SAMPLE_DF)  # should not raise


def test_load_calls_upsert_raw_with_correct_dataframe():
    mock_engine = MagicMock()
    mock_engine.connect.return_value = _make_connect_conn(scalar_value=3)

    with patch("pipeline.load.get_engine", return_value=mock_engine), \
         patch("pipeline.load.upsert_raw", return_value=3) as mock_upsert:
        load(SAMPLE_DF)

    mock_upsert.assert_called_once()
    _, called_df = mock_upsert.call_args[0]
    assert list(called_df.columns) == list(SAMPLE_DF.columns)
    assert len(called_df) == len(SAMPLE_DF)
