"""
RetailNova - pytest conftest.py
================================
Shared fixtures and mocks for the test suite.

KEY DESIGN: Tests run WITHOUT Docker/PostgreSQL.
Logger DB calls are mocked so unit tests stay fast and self-contained.
Only Spark (local[*] mode) is required.

Run:  pytest tests/ -v
Or:   make test
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# ─── Force local Spark mode for tests ───────────────────────────────────────
# These MUST be set before PySpark is imported
os.environ.setdefault("SPARK_MASTER",      "local[2]")
os.environ.setdefault("RETAILNOVA_ENV",    "dev")
os.environ.setdefault("POSTGRES_HOST",     "localhost")   # won't be used (mocked)
os.environ.setdefault("SQLSERVER_HOST",    "localhost")   # won't be used (mocked)
os.environ.setdefault("MINIO_ENDPOINT",    "http://localhost:9000")

sys.path.insert(0, os.path.dirname(__file__))


# ─── Auto-mock PostgreSQL for ALL tests ─────────────────────────────────────
# This means tests never try to open a real DB connection.
# Individual tests that DO want real DB can override with: @pytest.mark.usefixtures("real_db")

@pytest.fixture(autouse=True)
def mock_postgres(monkeypatch):
    """
    Auto-applied to every test.
    Mocks psycopg2.connect so no real PostgreSQL connection is attempted.
    Logger functions (log_execution_start, log_error etc.) silently no-op.
    """
    mock_conn   = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.__enter__ = MagicMock(return_value=mock_cursor)
    mock_cursor.__exit__  = MagicMock(return_value=False)
    mock_conn.cursor.return_value = mock_cursor
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__  = MagicMock(return_value=False)

    # Watermark always returns epoch (first run)
    from datetime import datetime, timezone
    mock_cursor.fetchone.return_value = (datetime(1900, 1, 1, tzinfo=timezone.utc),)
    mock_cursor.fetchall.return_value = []

    with patch("psycopg2.connect", return_value=mock_conn):
        yield mock_conn


# ─── Shared SparkSession ─────────────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    """
    Session-scoped SparkSession in local mode.
    Created once and shared across all tests — fast startup.
    Delta Lake extension included for Delta-specific tests.
    """
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailNova-Tests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")       # no UI in tests
        .config("spark.sql.warehouse.dir", "/tmp/retailnova-test-warehouse")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ─── Temp directory for Delta writes in tests ────────────────────────────────
@pytest.fixture
def tmp_delta_path(tmp_path):
    """Returns a fresh temp directory for Delta Lake writes in each test."""
    return str(tmp_path / "delta")


# ─── Print test header ───────────────────────────────────────────────────────
def pytest_runtest_setup(item):
    """Print a clean separator before each test class boundary."""
    pass


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "slow: marks tests as slow (skipped with -m 'not slow')"
    )
    config.addinivalue_line(
        "markers", "requires_docker: marks tests that need Docker services running"
    )
