"""Cross-engine observable-output parity (PR-1 test safety net).

Both engines are driven to their observable reconciliation outputs on the
*same* synthetic Delta inputs, and compared at the level of reconciliation
meaning — mismatch counts, null mismatches, tolerance suppression, quarter
status, and zero-filled unchanged columns — not raw hash integers
(architecture_contract.md sections 1, 5, 8).

Passing tests establish parity that currently holds. Tests marked
``xfail(strict=True)`` expose specific, known divergences that PR-2 must
close (ADR-0001).
"""

from __future__ import annotations

import os
from datetime import date

import pytest

pytestmark = [pytest.mark.spark, pytest.mark.polars]

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
polars = pytest.importorskip("polars", reason="Polars not installed")
deltalake = pytest.importorskip("deltalake", reason="deltalake not installed")

import polars as pl

from recon.config import ReconcileConfig
from recon.runner import run_reconciliation

from recon.helpers import batch_key_value

from tests.test_recon._parity_helpers import (
    write_delta,
    run_polars_reconciliation,
    polars_mismatch_by_column,
    spark_mismatch_by_column,
    spark_quarter_status,
)

Q0 = date(2020, 1, 1)  # identical quarter
Q1 = date(2020, 4, 1)  # changed quarter
N = 20
TOL = 0.5

FEATURES = ["num_exact", "num_null", "num_unchanged", "num_within_tol"]

# Expected observable meaning on the crafted dataset (the reference baseline).
EXPECTED = {
    "num_exact": (5, 0),        # 5 value mismatches beyond tolerance
    "num_within_tol": (0, 0),   # 5 changes of 0.1 < 0.5 tolerance -> suppressed
    "num_null": (5, 5),         # 5 rows: left value vs right null -> null mismatch
    "num_unchanged": (0, 0),    # never changed -> zero-filled
}


def _build_numeric_parity_data(base_dir: str) -> tuple[str, str]:
    """Left/right Delta tables differing only in Q1 in controlled ways."""
    pids: list[int] = []
    qs: list[date] = []
    left: dict[str, list] = {c: [] for c in FEATURES}
    right: dict[str, list] = {c: [] for c in FEATURES}

    for q in (Q0, Q1):
        for i in range(N):
            pid = i + 1
            pids.append(pid)
            qs.append(q)
            ne, nt, nn, nu = float(pid), float(pid) * 2, float(pid) * 3, float(pid) * 4

            left["num_exact"].append(ne)
            left["num_within_tol"].append(nt)
            left["num_null"].append(nn)
            left["num_unchanged"].append(nu)

            if q == Q1 and i < 5:
                right["num_exact"].append(ne + 10.0)     # beyond tolerance
                right["num_within_tol"].append(nt + 0.1)  # within tolerance
                right["num_null"].append(nn)
                right["num_unchanged"].append(nu)
            elif q == Q1 and 5 <= i < 10:
                right["num_exact"].append(ne)
                right["num_within_tol"].append(nt)
                right["num_null"].append(None)            # null mismatch
                right["num_unchanged"].append(nu)
            else:
                right["num_exact"].append(ne)
                right["num_within_tol"].append(nt)
                right["num_null"].append(nn)
                right["num_unchanged"].append(nu)

    left_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, **left})
    right_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, **right})

    left_path = write_delta(left_df, os.path.join(base_dir, "left"))
    right_path = write_delta(right_df, os.path.join(base_dir, "right"))
    return left_path, right_path


@pytest.fixture(scope="module")
def parity_env(local_spark, tmp_path_factory):
    spark = local_spark
    base_dir = str(tmp_path_factory.mktemp("parity"))
    left_path, right_path = _build_numeric_parity_data(base_dir)

    # --- Spark run (through the public runner) ---
    spark.read.format("delta").load(left_path).createOrReplaceTempView("parity_left")
    spark.read.format("delta").load(right_path).createOrReplaceTempView("parity_right")
    spark.sql("CREATE DATABASE IF NOT EXISTS recon_parity_db")

    spark_cfg = ReconcileConfig(
        left_table_name="parity_left",
        right_table_name="parity_right",
        output_catalog="spark_catalog",
        output_schema="recon_parity_db",
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=["num_exact"],
        all_feature_cols=FEATURES,
        default_numeric_tolerance=TOL,
        run_id="parity_spark_001",
        source_label="TEST",
        detail_mode="sample",
        cleanup_tmp_tables=True,
    )
    spark_outputs = run_reconciliation(spark_cfg)

    # --- Polars run (full engine orchestration) ---
    polars_out = os.path.join(base_dir, "polars_output")
    polars_cfg = ReconcileConfig(
        left_table_name=left_path,
        right_table_name=right_path,
        output_catalog="local",
        output_schema=polars_out,
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=["num_exact"],
        all_feature_cols=FEATURES,
        default_numeric_tolerance=TOL,
        engine="polars",
        run_id="parity_polars_001",
        source_label="TEST",
        detail_mode="sample",
        cleanup_tmp_tables=True,
    )
    run_polars_reconciliation(polars_cfg)

    spark_cols = spark_mismatch_by_column(
        spark, spark_outputs["column_summary_all_quarters"], "parity_spark_001"
    )
    polars_cols = polars_mismatch_by_column(polars_cfg)
    spark_qstatus = spark_quarter_status(
        spark, spark_outputs["quarter_checksums"], "parity_spark_001"
    )
    polars_qstatus = {
        k: v
        for k, v in _read_polars_quarter_status(polars_out, "parity_polars_001").items()
    }

    return {
        "spark_cols": spark_cols,
        "polars_cols": polars_cols,
        "spark_qstatus": spark_qstatus,
        "polars_qstatus": polars_qstatus,
    }


def _read_polars_quarter_status(out_dir: str, run_id: str) -> dict:
    path = os.path.join(out_dir, "recon_quarter_checksums")
    df = pl.read_delta(path).filter(pl.col("run_id") == run_id)
    return {r["batch_key"]: r["quarter_status"] for r in df.iter_rows(named=True)}


# ---------------------------------------------------------------------------
# Passing parity — the reconciliation meaning currently agrees for these cases
# ---------------------------------------------------------------------------

def test_spark_baseline_matches_expected(parity_env):
    """Reference baseline: Spark reproduces the crafted observable meaning."""
    for col, expected in EXPECTED.items():
        assert parity_env["spark_cols"][col] == expected, col


def test_observable_mismatch_parity(parity_env):
    """Both engines agree on (mismatch_count, null_mismatch_count) per column."""
    assert parity_env["spark_cols"] == parity_env["polars_cols"]


def test_tolerance_parity(parity_env):
    """A sub-tolerance change is suppressed identically by both engines."""
    assert parity_env["spark_cols"]["num_within_tol"] == (0, 0)
    assert parity_env["polars_cols"]["num_within_tol"] == (0, 0)


def test_null_mismatch_parity(parity_env):
    """left-value vs right-null is counted as a null mismatch by both engines."""
    assert parity_env["spark_cols"]["num_null"] == (5, 5)
    assert parity_env["polars_cols"]["num_null"] == (5, 5)


def test_zero_fill_unchanged_column_parity(parity_env):
    """An entirely unchanged column is zero-filled by both engines."""
    assert parity_env["spark_cols"]["num_unchanged"] == (0, 0)
    assert parity_env["polars_cols"]["num_unchanged"] == (0, 0)


def test_quarter_status_parity(parity_env):
    """Quarter status agrees: an identical quarter cannot be 'changed' elsewhere."""
    sp = parity_env["spark_qstatus"]
    po = parity_env["polars_qstatus"]
    for q in (Q0, Q1):
        assert sp[batch_key_value(q)] == po[batch_key_value(q)], q
    assert sp[batch_key_value(Q0)] == "identical"
    assert sp[batch_key_value(Q1)] == "changed"


# ---------------------------------------------------------------------------
# Public dispatch gap (CR-001)
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=True,
    reason="CR-001: run_reconciliation ignores cfg.engine and always runs the "
    "Spark path; it does not dispatch to the Polars engine.",
)
def test_public_runner_dispatches_to_polars(tmp_path):
    """run_reconciliation(engine='polars') should run Polars against Delta paths.

    Currently it always executes the Spark runner, which cannot read a
    filesystem path as a Spark table, so no Polars output is produced.
    """
    base = str(tmp_path)
    left_path, right_path = _build_numeric_parity_data(base)
    out_dir = os.path.join(base, "dispatch_out")

    cfg = ReconcileConfig(
        left_table_name=left_path,
        right_table_name=right_path,
        output_catalog="local",
        output_schema=out_dir,
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=["num_exact"],
        all_feature_cols=FEATURES,
        engine="polars",
        run_id="dispatch_001",
        source_label="TEST",
    )
    run_reconciliation(cfg)

    # Only true if the Polars engine actually executed.
    assert os.path.exists(os.path.join(out_dir, "recon_column_summary_all_quarters"))
