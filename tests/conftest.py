"""
Shared pytest fixtures for recon test suite.

Environment-aware:
- **Linux / CI with PySpark**: full test suite (spark + polars tests).
- **Windows (no PySpark)**: ``pytest -m "not spark"`` runs polars-only tests.
- **Databricks**: reuses the cluster SparkSession; no local[*] session.

Markers:
- ``@pytest.mark.spark``  — requires PySpark + Delta.
- ``@pytest.mark.polars`` — requires Polars + deltalake.

Run subsets::

    pytest -m "not spark"   # skip Spark tests (Windows)
    pytest -m "not polars"  # skip Polars tests
    pytest                  # run everything available
"""

import os
import sys
import shutil
import tempfile

import pytest

# Ensure the project root is on sys.path for non-pip-installed usage.
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def _is_databricks() -> bool:
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _has_pyspark() -> bool:
    try:
        import pyspark  # noqa: F401
        return True
    except ImportError:
        return False


def _has_polars() -> bool:
    try:
        import polars  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Auto-skip markers when dependencies are missing
# ---------------------------------------------------------------------------

def pytest_collection_modifyitems(config, items):
    """Auto-skip tests whose marker dependencies are unavailable."""
    has_spark = _has_pyspark()
    has_polars = _has_polars()

    skip_spark = pytest.mark.skip(reason="PySpark not installed")
    skip_polars = pytest.mark.skip(reason="Polars not installed")

    for item in items:
        if "spark" in item.keywords and not has_spark:
            item.add_marker(skip_spark)
        if "polars" in item.keywords and not has_polars:
            item.add_marker(skip_polars)


# ---------------------------------------------------------------------------
# Spark fixture (session-scoped)
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def local_spark():
    """Provide a SparkSession with Delta Lake support.

    - On Databricks: reuses the cluster SparkSession.
    - Locally: creates a local[*] session with delta-spark.
    - Without PySpark: skips.
    """
    pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
    from pyspark.sql import SparkSession

    if _is_databricks():
        spark = SparkSession.builder.getOrCreate()
        from recon.helpers import set_spark
        set_spark(spark)
        yield spark
        return  # don't stop the cluster session

    warehouse_dir = tempfile.mkdtemp(prefix="recon_test_warehouse_")

    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .appName("recon_test")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.ui.enabled", "false")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        spark = (
            SparkSession.builder
            .appName("recon_test")
            .master("local[*]")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.driver.memory", "4g")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.default.parallelism", "4")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    # Inject into recon module
    from recon.helpers import set_spark
    set_spark(spark)

    yield spark

    spark.stop()
    shutil.rmtree(warehouse_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Utility fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_delta_dir(tmp_path):
    """Provide a temporary directory for Delta table storage per test."""
    delta_dir = tmp_path / "delta_tables"
    delta_dir.mkdir()
    return str(delta_dir)
