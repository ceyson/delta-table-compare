"""Null-sentinel collision characterization (PR-1 test safety net).

``normalize_for_hash`` (Spark) and ``_hash_row_cols`` (Polars) both replace
NULL with the literal string ``"__NULL__"`` before hashing. A genuine string
value equal to that sentinel therefore hashes identically to a real NULL, so
a ``value == "__NULL__"`` vs ``NULL`` difference is silently absorbed during
triage and never reaches Phase 4.

This is a correctness gap (architecture_contract.md sections 5 and 8: a real
value must not be conflated with a missing one). The test asserts the
*desired* behavior — the difference is observed — and is a strict xfail until
the sentinel collision is fixed.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

pytestmark = pytest.mark.spark

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")
polars = pytest.importorskip("polars", reason="Polars not installed")
deltalake = pytest.importorskip("deltalake", reason="deltalake not installed")

import polars as pl

from recon.config import ReconcileConfig
from recon.runner import run_reconciliation

from tests.test_recon._parity_helpers import write_delta, spark_mismatch_by_column

Q = date(2020, 1, 1)
N = 10
SENTINEL = "__NULL__"


@pytest.fixture(scope="module")
def sentinel_env(local_spark, tmp_path_factory):
    spark = local_spark
    base = str(tmp_path_factory.mktemp("sentinel"))

    pids, qs, left_s, right_s = [], [], [], []
    for i in range(N):
        pids.append(i + 1)
        qs.append(Q)
        if i < 5:
            left_s.append("X")
            right_s.append("X")          # identical
        else:
            left_s.append(SENTINEL)      # a REAL string equal to the sentinel
            right_s.append(None)         # an actual NULL

    left_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, "s": left_s})
    right_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, "s": right_s})
    left_path = write_delta(left_df, os.path.join(base, "left"))
    right_path = write_delta(right_df, os.path.join(base, "right"))

    spark.read.format("delta").load(left_path).createOrReplaceTempView("sent_left")
    spark.read.format("delta").load(right_path).createOrReplaceTempView("sent_right")
    spark.sql("CREATE DATABASE IF NOT EXISTS recon_sentinel_db")

    cfg = ReconcileConfig(
        left_table_name="sent_left",
        right_table_name="sent_right",
        output_catalog="spark_catalog",
        output_schema="recon_sentinel_db",
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=["s"],
        all_feature_cols=["s"],
        run_id="sentinel_spark",
        source_label="TEST",
        detail_mode="sample",
    )
    outputs = run_reconciliation(cfg)
    return spark_mismatch_by_column(spark, outputs["column_summary_all_quarters"], "sentinel_spark")


@pytest.mark.xfail(
    strict=True,
    reason="Sentinel collision: NULL and the literal string '__NULL__' both "
    "normalize to '__NULL__', so a real-value-vs-NULL difference is absorbed "
    "in triage and never reported.",
)
def test_real_sentinel_string_not_treated_as_null(sentinel_env):
    """A real '__NULL__' value opposite an actual NULL must be a difference."""
    mismatch, null_mismatch = sentinel_env["s"]
    assert (mismatch + null_mismatch) >= 1
