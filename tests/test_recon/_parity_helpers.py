"""Shared helpers for the PR-1 reconciliation test safety net.

These helpers build small, fully-controlled Delta inputs and drive both
engines to their *observable* reconciliation outputs so that cross-engine
parity can be asserted at the level of reconciliation meaning (mismatch
counts, null mismatches, quarter status) rather than raw hash integers.

Notes
-----
- The Polars engine has no public ``run_reconciliation`` dispatch yet
  (CR-001), so :func:`run_polars_reconciliation` reproduces the same phase
  orchestration the benchmark harness uses — the only complete Polars path.
- No production code is imported for mutation; only the public engine API
  and helpers are used.
"""

from __future__ import annotations

import os
from typing import Any

import polars as pl
from deltalake import write_deltalake

from recon.config import ReconcileConfig
from recon.engines import get_engine
from recon.helpers import build_column_groups


def write_delta(df: pl.DataFrame, path: str) -> str:
    """Write *df* as a fresh Delta table at *path* and return the path."""
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, df.to_arrow(), mode="overwrite")
    return path


def run_polars_reconciliation(cfg: ReconcileConfig) -> str:
    """Execute the full Polars reconciliation pipeline via the engine API.

    Returns the output directory (``cfg.output_schema``). Mirrors the phase
    sequence used elsewhere for Polars because ``run_reconciliation`` does
    not dispatch on ``cfg.engine`` (CR-001).
    """
    engine = get_engine("polars")
    engine.setup(cfg)
    engine.validate_tables(cfg)
    all_cols = engine.resolve_compare_cols(cfg)
    noncritical = [c for c in all_cols if c not in set(cfg.critical_cols)]
    groups = build_column_groups(all_cols, list(cfg.critical_cols), cfg.hash_group_size)
    engine.write_run_metadata(cfg, all_cols, noncritical)

    changed_quarters, quarter_status = engine.phase0_quarter_screening(cfg, all_cols)
    nonnull = engine.phase2b_nonnull_counts(cfg, all_cols, quarter_status)

    total_matched: Any = pl.DataFrame(
        schema={cfg.qtr_col: quarter_status.schema[cfg.qtr_col], "total_matched_count": pl.Int64}
    )
    group_changed: dict[int, Any] = {}
    if changed_quarters:
        left_h, right_h = engine.phase1_hash_extraction(
            cfg, changed_quarters, all_cols, groups
        )
        changed_keys, total_matched = engine.phase2_key_recon(
            cfg, left_h, right_h, len(groups)
        )
        group_changed = engine.phase3_group_triage(cfg, changed_keys, len(groups))
        engine.phase4_targeted_comparison(
            cfg, changed_quarters, groups, group_changed,
            all_cols, total_matched, nonnull,
        )

    engine.phase5_rollups(
        cfg, changed_quarters, quarter_status, all_cols,
        groups, group_changed, total_matched, nonnull,
    )
    engine.mark_run_complete(cfg, "COMPLETED")
    return cfg.output_schema


def polars_quarter_status(cfg: ReconcileConfig) -> dict[Any, str]:
    """Return ``{quarter -> quarter_status}`` from the Polars Phase 0 screen."""
    engine = get_engine("polars")
    engine.setup(cfg)
    engine.validate_tables(cfg)
    all_cols = engine.resolve_compare_cols(cfg)
    _changed, quarter_status = engine.phase0_quarter_screening(cfg, all_cols)
    return {
        row[cfg.qtr_col]: row["quarter_status"]
        for row in quarter_status.iter_rows(named=True)
    }


def polars_mismatch_by_column(cfg: ReconcileConfig) -> dict[str, tuple[int, int]]:
    """Read the Polars all-quarter rollup as ``{column -> (mismatch, null_mismatch)}``."""
    path = os.path.join(cfg.output_schema, f"{cfg.final_prefix}_column_summary_all_quarters")
    df = pl.read_delta(path).filter(pl.col("run_id") == cfg.run_id)
    return {
        r["column"]: (int(r["mismatch_count"]), int(r["null_mismatch_count"]))
        for r in df.iter_rows(named=True)
    }


def spark_mismatch_by_column(spark, table_name: str, run_id: str) -> dict[str, tuple[int, int]]:
    """Read the Spark all-quarter rollup as ``{column -> (mismatch, null_mismatch)}``."""
    from pyspark.sql import functions as F

    rows = spark.table(table_name).filter(F.col("run_id") == run_id).collect()
    return {
        r["column"]: (int(r["mismatch_count"]), int(r["null_mismatch_count"]))
        for r in rows
    }


def spark_quarter_status(spark, table_name: str, run_id: str) -> dict[Any, str]:
    """Read Spark quarter_checksums as ``{batch_key -> quarter_status}``."""
    from pyspark.sql import functions as F

    rows = spark.table(table_name).filter(F.col("run_id") == run_id).collect()
    return {r["batch_key"]: r["quarter_status"] for r in rows}
