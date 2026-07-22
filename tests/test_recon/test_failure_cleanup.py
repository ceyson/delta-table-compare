"""Failure and cleanup characterization (PR-1 test safety net).

Records the *current* Spark failure/cleanup behavior and pins the gaps that
ADR-0003 / CR-010 will close, without inventing the future cleanup API:

- On failure the run is marked ``FAILED`` in ``run_metadata`` and the
  exception propagates (passing baseline).
- Temporary artifacts created before the failure are **retained** — cleanup
  runs only on success (passing baseline; matches ADR-0003 default).
- ``ReconcileConfig`` does not yet expose the independent
  ``cleanup_on_success`` / ``cleanup_on_failure`` controls defined by
  ADR-0003 (strict xfail).

Failures are induced by monkeypatching phase functions to raise; no
production code is modified.
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
from pyspark.sql import functions as F

import recon.runner as runner
from recon.config import ReconcileConfig
from recon.helpers import safe_suffix, schema_fqn

from tests.test_recon._parity_helpers import write_delta

Q0 = date(2020, 1, 1)
Q1 = date(2020, 4, 1)
N = 10


def _build_changed_data(base_dir: str) -> tuple[str, str]:
    """Two quarters; Q1 has a genuine value change so phases 1-4 execute."""
    pids, qs, left_v, right_v = [], [], [], []
    for q in (Q0, Q1):
        for i in range(N):
            pids.append(i + 1)
            qs.append(q)
            left_v.append(float(i))
            right_v.append(float(i) + (100.0 if (q == Q1 and i < 5) else 0.0))
    left_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, "v": left_v})
    right_df = pl.DataFrame({"policy_id": pids, "quarter_date": qs, "v": right_v})
    return (
        write_delta(left_df, os.path.join(base_dir, "left")),
        write_delta(right_df, os.path.join(base_dir, "right")),
    )


def _make_cfg(run_id: str) -> ReconcileConfig:
    return ReconcileConfig(
        left_table_name="fail_left",
        right_table_name="fail_right",
        output_catalog="spark_catalog",
        output_schema="recon_fail_db",
        key_cols=["policy_id", "quarter_date"],
        qtr_col="quarter_date",
        critical_cols=["v"],
        all_feature_cols=["v"],
        run_id=run_id,
        source_label="TEST",
        detail_mode="sample",
        cleanup_tmp_tables=True,
    )


@pytest.fixture(scope="module")
def fail_tables(local_spark, tmp_path_factory):
    spark = local_spark
    base = str(tmp_path_factory.mktemp("failcleanup"))
    left_path, right_path = _build_changed_data(base)
    spark.read.format("delta").load(left_path).createOrReplaceTempView("fail_left")
    spark.read.format("delta").load(right_path).createOrReplaceTempView("fail_right")
    spark.sql("CREATE DATABASE IF NOT EXISTS recon_fail_db")
    return spark


def _temp_tables_for_run(spark, cfg: ReconcileConfig) -> list[str]:
    db = schema_fqn(cfg.output_catalog, cfg.output_schema)
    safe_run = safe_suffix(cfg.run_id)
    prefix = f"{cfg.temp_prefix}_"
    rows = spark.sql(f"SHOW TABLES IN {db}").collect()
    return [
        r["tableName"]
        for r in rows
        if r["tableName"].startswith(prefix) and safe_run in r["tableName"]
    ]


def test_failed_run_marked_failed(fail_tables, monkeypatch):
    """Baseline: a mid-run failure marks run_metadata FAILED and re-raises."""
    spark = fail_tables
    cfg = _make_cfg("fail_marked_001")

    def boom(*args, **kwargs):
        raise RuntimeError("induced phase0 failure")

    monkeypatch.setattr(runner, "phase0_quarter_screening", boom)

    with pytest.raises(RuntimeError, match="induced phase0 failure"):
        runner.run_reconciliation(cfg)

    rows = (
        spark.table(runner.final_table(cfg, "run_metadata"))
        .filter(F.col("run_id") == "fail_marked_001")
        .collect()
    )
    assert len(rows) >= 1
    assert all(r["status"] == "FAILED" for r in rows)


def test_failed_run_retains_temp_tables(fail_tables, monkeypatch):
    """Baseline: temp artifacts created before a failure are retained (ADR-0003)."""
    spark = fail_tables
    cfg = _make_cfg("fail_retain_002")

    def boom(*args, **kwargs):
        raise RuntimeError("induced phase4 failure")

    # Fail in Phase 4, after Phase 1-3 have created temporary tables.
    monkeypatch.setattr(runner, "phase4_targeted_comparison", boom)

    with pytest.raises(RuntimeError, match="induced phase4 failure"):
        runner.run_reconciliation(cfg)

    retained = _temp_tables_for_run(spark, cfg)
    assert len(retained) > 0, "expected temp tables to be retained after failure"


@pytest.mark.xfail(
    strict=True,
    reason="ADR-0003 / CR-010: ReconcileConfig does not yet expose independent "
    "cleanup_on_success / cleanup_on_failure controls.",
)
def test_config_exposes_failure_cleanup_policy():
    """The explicit success/failure cleanup policy fields should exist."""
    cfg = _make_cfg("fail_policy_003")
    assert hasattr(cfg, "cleanup_on_success")
    assert hasattr(cfg, "cleanup_on_failure")
