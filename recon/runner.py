"""
Main reconciliation runner — orchestrates all phases and manages run lifecycle.
"""

from __future__ import annotations

import time as _time
from datetime import datetime
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import ReconcileConfig
from .helpers import (
    get_spark,
    schema_fqn,
    safe_suffix,
    table_fqn,
    final_table,
    ensure_schema_exists,
    validate_columns_exist,
    resolve_all_compare_cols,
    resolve_noncritical_cols,
    write_delta_append,
    build_column_groups,
)

# Convenience accessor — all functions in this module use get_spark() directly.
from .phases import (
    phase0_quarter_screening,
    emit_row_status_for_identical_quarters,
    emit_row_status_for_onesided_quarters,
    phase1_hash_extraction,
    phase2_key_recon_and_row_triage,
    compute_nonnull_counts,
    phase3_group_triage,
    phase4_targeted_comparison,
    emit_zero_fill_for_identical_quarters,
    emit_zero_fill_for_unchanged_groups,
    build_rollups,
)


# ---------------------------------------------------------------------------
# Run metadata
# ---------------------------------------------------------------------------


def create_run_metadata(
    cfg: ReconcileConfig, all_compare_cols: list[str], noncritical_cols: list[str]
) -> None:
    """Insert an initial RUNNING row into the run_metadata table.

    This records the configuration snapshot (tables, key columns, column
    counts, detail mode, etc.) so that each run is fully traceable.

    Args:
        cfg: Active reconciliation configuration.
        all_compare_cols: Resolved list of all columns being compared.
        noncritical_cols: Subset of compare columns that are not critical.
    """
    df = get_spark().createDataFrame(
        [
            (
                cfg.run_id,
                cfg.source_label,
                cfg.left_table_name,
                cfg.right_table_name,
                list(cfg.key_cols),
                cfg.qtr_col,
                len(cfg.critical_cols),
                len(noncritical_cols),
                len(all_compare_cols),
                cfg.hash_group_size,
                cfg.detail_mode,
                datetime.now(),
                None,
                "RUNNING",
            )
        ],
        schema=T.StructType(
            [
                T.StructField("run_id", T.StringType(), False),
                T.StructField("source_label", T.StringType(), True),
                T.StructField("left_table_name", T.StringType(), False),
                T.StructField("right_table_name", T.StringType(), False),
                T.StructField("key_cols", T.ArrayType(T.StringType()), False),
                T.StructField("qtr_col", T.StringType(), False),
                T.StructField("critical_column_count", T.IntegerType(), False),
                T.StructField("noncritical_column_count", T.IntegerType(), False),
                T.StructField("total_compare_column_count", T.IntegerType(), False),
                T.StructField("hash_group_size", T.IntegerType(), False),
                T.StructField("detail_mode", T.StringType(), False),
                T.StructField("started_at", T.TimestampType(), False),
                T.StructField("completed_at", T.TimestampType(), True),
                T.StructField("status", T.StringType(), False),
            ]
        ),
    )
    write_delta_append(df, final_table(cfg, "run_metadata"))


def mark_run_complete(cfg: ReconcileConfig, status: str) -> None:
    """Update the run_metadata row with a final status and timestamp.

    If the UPDATE fails (e.g. table doesn't exist yet), a warning is
    printed but no exception is raised.

    Args:
        cfg: Active reconciliation configuration.
        status: Final status string (e.g. 'COMPLETED' or 'FAILED').
    """
    run_id_sql = str(cfg.run_id).replace("'", "''")
    status_sql = str(status).replace("'", "''")
    try:
        get_spark().sql(
            f"""
            UPDATE {final_table(cfg, "run_metadata")}
            SET completed_at = current_timestamp(),
                status = '{status_sql}'
            WHERE run_id = '{run_id_sql}'
            """
        )
    except Exception as exc:
        print(
            f"WARNING: Could not update run_metadata status for run_id={cfg.run_id}: {exc}"
        )


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def cleanup_temp_tables_for_run(cfg: ReconcileConfig) -> None:
    """Drop all temporary Delta tables created by this run.

    Identifies temp tables by matching the configured temp_prefix and
    the sanitized run_id in the table name.

    Args:
        cfg: Active reconciliation configuration.
    """
    db = schema_fqn(cfg.output_catalog, cfg.output_schema)
    safe_run = safe_suffix(cfg.run_id)
    prefix = f"{cfg.temp_prefix}_"

    tables = get_spark().sql(f"SHOW TABLES IN {db}").collect()
    to_drop = []
    for row in tables:
        tbl_name = row["tableName"]
        if tbl_name.startswith(prefix) and safe_run in tbl_name:
            to_drop.append(tbl_name)

    for tbl_name in to_drop:
        get_spark().sql(
            f"DROP TABLE IF EXISTS {table_fqn(cfg.output_catalog, cfg.output_schema, tbl_name)}"
        )

    print(f"Cleanup: Dropped {len(to_drop)} temp tables.")


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------


def run_reconciliation(cfg: ReconcileConfig, collect_timings: bool = False) -> dict[str, str]:
    """Execute a full reconciliation run.

    Orchestrates all phases (0–5): quarter checksums, row hashing,
    key reconciliation, group triage, targeted comparison, rollups,
    and cleanup.  On failure the run is marked FAILED and the
    exception is re-raised.

    Args:
        cfg: Fully populated :class:`ReconcileConfig` instance.
        collect_timings: If True, the returned dict includes a
            'phase_timings' key with a list of (phase_name, seconds) tuples.

    Returns:
        Dict mapping logical output names (e.g. 'run_metadata',
        'mismatch_sample') to their fully-qualified Delta table names.
        If collect_timings is True, also includes 'phase_timings'.

    Raises:
        Exception: Any unhandled error during reconciliation (re-raised
            after marking the run as FAILED).
    """
    phase_timings: list[tuple[str, float]] = []

    def _timed(name):
        """Context-manager-like helper for phase timing."""
        class _Timer:
            def __enter__(self):
                self.t0 = _time.perf_counter()
                return self
            def __exit__(self, *_):
                phase_timings.append((name, _time.perf_counter() - self.t0))
        return _Timer()

    try:
        with _timed("setup"):
            ensure_schema_exists(cfg)
            validate_columns_exist(cfg)

            all_compare_cols = resolve_all_compare_cols(cfg)
            noncritical_cols = resolve_noncritical_cols(cfg, all_compare_cols)
            critical_cols = list(cfg.critical_cols)

            groups = build_column_groups(
                all_compare_cols, critical_cols, cfg.hash_group_size
            )
            num_groups = len(groups)

            create_run_metadata(cfg, all_compare_cols, noncritical_cols)

        print(f"Starting reconciliation run_id={cfg.run_id}")
        print(f"Total compare columns: {len(all_compare_cols)}")
        print(f"Critical columns: {len(critical_cols)}")
        print(f"Non-critical columns: {len(noncritical_cols)}")
        print(f"Column groups ({cfg.hash_group_size}/group): {num_groups}")
        print(f"Detail mode: {cfg.detail_mode}")

        # --- Phase 0: Quarter-level checksums ---
        with _timed("phase0_quarter_screening"):
            changed_quarters, quarter_status_df = phase0_quarter_screening(
                cfg, all_compare_cols
            )

        # --- Phase 2b: Nonnull counts (runs on all both-sided quarters) ---
        with _timed("phase2b_nonnull_counts"):
            nonnull_counts_table = compute_nonnull_counts(
                cfg, all_compare_cols, quarter_status_df
            )

        if not changed_quarters:
            print(
                "All quarters identical. Emitting zero-fill summaries and completing."
            )
            with _timed("phase5_rollups"):
                emit_row_status_for_identical_quarters(cfg, quarter_status_df)
                emit_zero_fill_for_identical_quarters(
                    cfg, quarter_status_df, all_compare_cols, nonnull_counts_table
                )
                build_rollups(cfg)
            mark_run_complete(cfg, "COMPLETED")
            print(f"Completed reconciliation run_id={cfg.run_id}")
            out = _build_output_dict(cfg)
            if collect_timings:
                out["phase_timings"] = phase_timings
            return out

        # Emit row_status_counts for one-sided quarters (Phase 0 found them, Phase 2 won't see them).
        emit_row_status_for_onesided_quarters(cfg, quarter_status_df)
        # Emit row_status_counts for identical quarters.
        emit_row_status_for_identical_quarters(cfg, quarter_status_df)

        # --- Phase 1: Row-level hash extraction ---
        with _timed("phase1_hash_extraction"):
            left_hash_table, right_hash_table = phase1_hash_extraction(
                cfg, changed_quarters, all_compare_cols, groups
            )

        # --- Phase 2: Key reconciliation + row triage ---
        with _timed("phase2_key_recon"):
            changed_keys_table, total_matched_per_qtr = phase2_key_recon_and_row_triage(
                cfg, left_hash_table, right_hash_table, num_groups
            )

        # --- Phase 3: Group triage ---
        with _timed("phase3_group_triage"):
            group_changed_keys = phase3_group_triage(cfg, changed_keys_table, num_groups)

        # --- Phase 4: Targeted comparison ---
        with _timed("phase4_targeted_comparison"):
            phase4_targeted_comparison(
                cfg,
                changed_quarters,
                groups,
                group_changed_keys,
                all_compare_cols,
                total_matched_per_qtr,
                nonnull_counts_table,
            )

        # --- Phase 5: Zero-fill + rollups ---
        with _timed("phase5_rollups"):
            emit_zero_fill_for_identical_quarters(
                cfg, quarter_status_df, all_compare_cols, nonnull_counts_table
            )
            emit_zero_fill_for_unchanged_groups(
                cfg,
                changed_quarters,
                groups,
                group_changed_keys,
                total_matched_per_qtr,
                nonnull_counts_table,
            )
            build_rollups(cfg)

        # --- Cleanup ---
        with _timed("cleanup"):
            if cfg.cleanup_tmp_tables:
                cleanup_temp_tables_for_run(cfg)

        mark_run_complete(cfg, "COMPLETED")
        print(f"Completed reconciliation run_id={cfg.run_id}")

    except Exception as exc:
        try:
            mark_run_complete(cfg, "FAILED")
        except Exception as complete_exc:
            print(
                f"WARNING: Could not mark run FAILED for run_id={cfg.run_id}: {complete_exc}"
            )
        raise

    out = _build_output_dict(cfg)
    if collect_timings:
        out["phase_timings"] = phase_timings
    return out


def _build_output_dict(cfg: ReconcileConfig) -> dict[str, str]:
    """Assemble the output table name dictionary returned to the caller."""
    return {
        "run_id": cfg.run_id,
        "run_metadata": final_table(cfg, "run_metadata"),
        "quarter_checksums": final_table(cfg, "quarter_checksums"),
        "row_status_detail": final_table(cfg, "row_status_detail"),
        "row_status_counts": final_table(cfg, "row_status_counts"),
        "column_summary_by_quarter": final_table(cfg, "column_summary_by_quarter"),
        "column_summary_all_quarters": final_table(cfg, "column_summary_all_quarters"),
        "mismatch_sample": final_table(cfg, "mismatch_sample"),
        "mismatch_detail": final_table(cfg, "mismatch_detail"),
        "noisy_columns": final_table(cfg, "noisy_columns"),
    }
