"""Spark multi-group reconciliation baseline (PR-1 test safety net).

Establishes the *current Spark behavior* as the reference baseline for wide
tables that span multiple hash groups. Exercises:

- Phase 3 group triage across 3+ column groups.
- Phase 5 zero-fill for an entirely-unchanged group.
- Phase 5 zero-fill for an identical quarter.

All assertions here describe behavior that Spark already produces, so this
file is a passing baseline (ADR-0001: Spark is the reference implementation).
"""

from __future__ import annotations

import os

import pytest

pytestmark = pytest.mark.spark

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")

from pyspark.sql import functions as F

from recon.config import ReconcileConfig
from recon.helpers import build_column_groups, batch_key_value
from recon.runner import run_reconciliation

N_NUMERIC = 250
HASH_GROUP_SIZE = 100
CRITICAL = [f"num_col_{i:03d}" for i in range(5)]
ALL_FEATURES = [f"num_col_{i:03d}" for i in range(N_NUMERIC)]

# Column ordering inside groups is: critical first, then remaining sorted.
# With 5 critical + 245 non-critical and group size 100:
#   group 0 -> num_col_000..num_col_099
#   group 1 -> num_col_100..num_col_199   (left entirely unchanged)
#   group 2 -> num_col_200..num_col_249
CHANGED_G0 = "num_col_010"
CHANGED_G2 = "num_col_240"
UNCHANGED_G0 = "num_col_050"
UNCHANGED_G1 = "num_col_150"


@pytest.fixture(scope="module")
def multigroup_env(local_spark, tmp_path_factory):
    from tests.data_generator import (
        generate_test_data,
        inject_differences,
        _quarter_dates,
    )

    spark = local_spark
    base_dir = str(tmp_path_factory.mktemp("multigroup"))

    left_path, right_path, _critical = generate_test_data(
        spark=spark,
        output_path=base_dir,
        n_quarters=3,
        base_rows_per_quarter=150,
        rows_per_quarter_increment=0,
        n_numeric_cols=N_NUMERIC,
        n_string_cols=0,
        n_date_cols=0,
        n_bool_cols=0,
        seed=42,
    )

    quarters = _quarter_dates(3, 2020)
    # Inject changes only into quarters 1 and 2 (quarter 0 stays identical),
    # and only into columns that live in group 0 and group 2.
    modified_path = inject_differences(
        spark=spark,
        source_path=left_path,
        output_path=os.path.join(base_dir, "right_multigroup"),
        change_rate=0.20,
        change_cols=[CHANGED_G0, CHANGED_G2],
        quarters_affected=[quarters[1], quarters[2]],
        seed=99,
    )

    spark.read.format("delta").load(left_path).createOrReplaceTempView("mg_left")
    spark.read.format("delta").load(modified_path).createOrReplaceTempView("mg_right")
    spark.sql("CREATE DATABASE IF NOT EXISTS recon_mg_db")

    cfg = ReconcileConfig(
        left_table_name="mg_left",
        right_table_name="mg_right",
        output_catalog="spark_catalog",
        output_schema="recon_mg_db",
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=CRITICAL,
        all_feature_cols=ALL_FEATURES,
        hash_group_size=HASH_GROUP_SIZE,
        run_id="mg_baseline_001",
        source_label="TEST",
        detail_mode="sample",
        cleanup_tmp_tables=True,
    )
    outputs = run_reconciliation(cfg)

    return {"spark": spark, "cfg": cfg, "outputs": outputs, "quarters": quarters}


def test_column_groups_span_three_groups():
    """Baseline: 250 columns at group size 100 form 3 groups."""
    groups = build_column_groups(ALL_FEATURES, CRITICAL, HASH_GROUP_SIZE)
    assert len(groups) == 3
    assert CHANGED_G0 in groups[0]
    assert UNCHANGED_G1 in groups[1]
    assert CHANGED_G2 in groups[2]


def test_quarter_status_identical_and_changed(multigroup_env):
    """Baseline: quarter 0 is identical; quarters 1 and 2 are changed."""
    spark = multigroup_env["spark"]
    outputs = multigroup_env["outputs"]
    quarters = multigroup_env["quarters"]

    rows = spark.table(outputs["quarter_checksums"]).filter(
        F.col("run_id") == "mg_baseline_001"
    ).collect()
    status = {r["batch_key"]: r["quarter_status"] for r in rows}

    assert status[batch_key_value(quarters[0])] == "identical"
    assert status[batch_key_value(quarters[1])] == "changed"
    assert status[batch_key_value(quarters[2])] == "changed"


def test_changed_columns_have_mismatches(multigroup_env):
    """Baseline: only the injected columns (group 0 and group 2) show mismatches."""
    spark = multigroup_env["spark"]
    outputs = multigroup_env["outputs"]

    rows = spark.table(outputs["column_summary_all_quarters"]).filter(
        F.col("run_id") == "mg_baseline_001"
    ).collect()
    mismatch = {r["column"]: r["mismatch_count"] for r in rows}

    assert mismatch[CHANGED_G0] > 0
    assert mismatch[CHANGED_G2] > 0
    # Unchanged column in a partially-changed group.
    assert mismatch[UNCHANGED_G0] == 0
    # Column in the fully-unchanged group 1 (Phase 5 zero-fill).
    assert mismatch[UNCHANGED_G1] == 0


def test_all_columns_present_via_zero_fill(multigroup_env):
    """Baseline: every compared column appears in the rollup (zero-filled if unchanged)."""
    spark = multigroup_env["spark"]
    outputs = multigroup_env["outputs"]

    rows = spark.table(outputs["column_summary_all_quarters"]).filter(
        F.col("run_id") == "mg_baseline_001"
    ).collect()
    cols = {r["column"] for r in rows}

    assert set(ALL_FEATURES).issubset(cols)


def test_identical_quarter_zero_filled(multigroup_env):
    """Baseline: the identical quarter has zero mismatches for all columns."""
    spark = multigroup_env["spark"]
    outputs = multigroup_env["outputs"]
    quarters = multigroup_env["quarters"]

    rows = spark.table(outputs["column_summary_by_quarter"]).filter(
        (F.col("run_id") == "mg_baseline_001")
        & (F.col("batch_key") == batch_key_value(quarters[0]))
    ).collect()

    assert len(rows) > 0
    for r in rows:
        assert r["mismatch_count"] == 0
        assert r["null_mismatch_count"] == 0
