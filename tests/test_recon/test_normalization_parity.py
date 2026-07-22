"""Normalization-semantics characterization (PR-1 test safety net).

Covers the three hash-normalization flags — ``trim_strings_for_hash``,
``lower_strings_for_hash`` and ``float_hash_round_scale`` — and verifies
their effect in *both* triage (Phase 0/1 hashing) and the *final* Phase 4
value comparison (architecture_contract.md sections 4 and 6).

Current, characterized behavior:

- **Spark triage honors the flags** — a difference that normalizes away
  makes the quarter identical (passing baseline).
- **Spark Phase 4 does NOT re-apply the flags** — once a row reaches Phase 4
  (because some other column genuinely changed), a normalize-away difference
  is still reported as a mismatch. This violates contract section 6 and is
  captured as a strict xfail.
- **Polars ignores the flags entirely** — even in triage — so it diverges
  from the Spark reference (ADR-0001). Captured as a strict xfail.
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

from tests.test_recon._parity_helpers import (
    write_delta,
    run_polars_reconciliation,
    polars_mismatch_by_column,
    spark_mismatch_by_column,
    spark_quarter_status,
)

Q0 = date(2020, 1, 1)  # identical quarter
Q1 = date(2020, 4, 1)  # quarter carrying normalize-away differences
N = 10

NORM_COLS = ["f_round", "s_lower", "s_trim"]


def _build_norm_data(base_dir: str, *, with_co_change: bool) -> tuple[str, str]:
    """Build tables whose Q1 differences all vanish under normalization.

    - ``s_trim``  : "ABC" vs " ABC "  (removed by trim)
    - ``s_lower`` : "abc" vs "ABC"    (removed by lower)
    - ``f_round`` : 1.001 vs 1.002    (removed by round-to-2)
    - ``co_change``: 5.0 vs 105.0     (a genuine change, only when requested,
      used to force the row into Phase 4).
    """
    pids: list[int] = []
    qs: list[date] = []
    left: dict[str, list] = {}
    right: dict[str, list] = {}
    cols = ["s_trim", "s_lower", "f_round"] + (["co_change"] if with_co_change else [])
    for c in cols:
        left[c] = []
        right[c] = []

    for q in (Q0, Q1):
        for i in range(N):
            pids.append(i + 1)
            qs.append(q)
            changed = q == Q1

            left["s_trim"].append("ABC")
            right["s_trim"].append(" ABC " if changed else "ABC")

            left["s_lower"].append("abc")
            right["s_lower"].append("ABC" if changed else "abc")

            left["f_round"].append(1.001)
            right["f_round"].append(1.002 if changed else 1.001)

            if with_co_change:
                left["co_change"].append(5.0)
                right["co_change"].append(105.0 if changed else 5.0)

    left_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, **left})
    right_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, **right})
    return (
        write_delta(left_df, os.path.join(base_dir, "left")),
        write_delta(right_df, os.path.join(base_dir, "right")),
    )


def _read_polars_qstatus(out_dir: str, run_id: str) -> dict:
    df = pl.read_delta(os.path.join(out_dir, "recon_quarter_checksums")).filter(
        pl.col("run_id") == run_id
    )
    return {r["quarter_date"]: r["quarter_status"] for r in df.iter_rows(named=True)}


@pytest.fixture(scope="module")
def triage_env(local_spark, tmp_path_factory):
    """Normalize-away differences only — nothing forces Phase 4."""
    spark = local_spark
    base = str(tmp_path_factory.mktemp("norm_triage"))
    left_path, right_path = _build_norm_data(base, with_co_change=False)

    spark.read.format("delta").load(left_path).createOrReplaceTempView("norm_t_left")
    spark.read.format("delta").load(right_path).createOrReplaceTempView("norm_t_right")
    spark.sql("CREATE DATABASE IF NOT EXISTS recon_norm_db")

    common = dict(
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=["f_round"],
        all_feature_cols=["s_trim", "s_lower", "f_round"],
        trim_strings_for_hash=True,
        lower_strings_for_hash=True,
        float_hash_round_scale=2,
        default_numeric_tolerance=0.0,
        source_label="TEST",
        detail_mode="sample",
    )

    spark_cfg = ReconcileConfig(
        left_table_name="norm_t_left",
        right_table_name="norm_t_right",
        output_catalog="spark_catalog",
        output_schema="recon_norm_db",
        run_id="norm_triage_spark",
        **common,
    )
    spark_outputs = run_reconciliation(spark_cfg)

    polars_out = os.path.join(base, "polars_output")
    polars_cfg = ReconcileConfig(
        left_table_name=left_path,
        right_table_name=right_path,
        output_catalog="local",
        output_schema=polars_out,
        engine="polars",
        run_id="norm_triage_polars",
        **common,
    )
    run_polars_reconciliation(polars_cfg)

    return {
        "spark_cols": spark_mismatch_by_column(
            spark, spark_outputs["column_summary_all_quarters"], "norm_triage_spark"
        ),
        "spark_qstatus": spark_quarter_status(
            spark, spark_outputs["quarter_checksums"], "norm_triage_spark", "quarter_date"
        ),
        "polars_cols": polars_mismatch_by_column(polars_cfg),
        "polars_qstatus": _read_polars_qstatus(polars_out, "norm_triage_polars"),
    }


@pytest.fixture(scope="module")
def phase4_env(local_spark, tmp_path_factory):
    """A genuine co-change forces the normalize-away rows into Phase 4 (Spark)."""
    spark = local_spark
    base = str(tmp_path_factory.mktemp("norm_phase4"))
    left_path, right_path = _build_norm_data(base, with_co_change=True)

    spark.read.format("delta").load(left_path).createOrReplaceTempView("norm_p4_left")
    spark.read.format("delta").load(right_path).createOrReplaceTempView("norm_p4_right")
    spark.sql("CREATE DATABASE IF NOT EXISTS recon_norm_db")

    spark_cfg = ReconcileConfig(
        left_table_name="norm_p4_left",
        right_table_name="norm_p4_right",
        output_catalog="spark_catalog",
        output_schema="recon_norm_db",
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=["f_round"],
        all_feature_cols=["s_trim", "s_lower", "f_round", "co_change"],
        trim_strings_for_hash=True,
        lower_strings_for_hash=True,
        float_hash_round_scale=2,
        default_numeric_tolerance=0.0,
        run_id="norm_p4_spark",
        source_label="TEST",
        detail_mode="sample",
    )
    spark_outputs = run_reconciliation(spark_cfg)

    return {
        "spark_cols": spark_mismatch_by_column(
            spark, spark_outputs["column_summary_all_quarters"], "norm_p4_spark"
        ),
    }


# ---------------------------------------------------------------------------
# Spark triage honors the normalization flags — passing baseline
# ---------------------------------------------------------------------------

def test_spark_triage_suppresses_normalized_differences(triage_env):
    """Baseline: normalize-away Q1 diffs make the quarter identical in Spark."""
    assert triage_env["spark_qstatus"][Q1] == "identical"
    for col in NORM_COLS:
        assert triage_env["spark_cols"][col] == (0, 0), col


# ---------------------------------------------------------------------------
# Polars ignores the normalization flags (ADR-0001) — strict xfail
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=True,
    reason="ADR-0001: Polars _hash_row_cols ignores trim/lower/float rounding, "
    "so normalize-away differences are treated as changes (diverges from Spark).",
)
def test_polars_triage_matches_spark_normalization(triage_env):
    """Polars should suppress normalize-away diffs in triage like Spark does."""
    assert triage_env["polars_qstatus"][Q1] == "identical"
    for col in NORM_COLS:
        assert triage_env["polars_cols"][col] == (0, 0), col


# ---------------------------------------------------------------------------
# Spark Phase 4 does not re-apply normalization (contract section 6) — xfail
# ---------------------------------------------------------------------------

@pytest.mark.xfail(
    strict=True,
    reason="architecture_contract.md section 6: normalization is applied in "
    "triage but Phase 4 (compare_columns_for_keys) compares raw values, so a "
    "normalize-away difference is reported once the row reaches Phase 4.",
)
def test_spark_phase4_applies_normalization(phase4_env):
    """Once in Phase 4, normalize-away diffs should still be suppressed."""
    for col in NORM_COLS:
        mismatch, _null = phase4_env["spark_cols"][col]
        assert mismatch == 0, col
