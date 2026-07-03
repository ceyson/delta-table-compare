"""
Spark engine — wraps existing PySpark phase implementations.

This engine delegates to the existing phases.py and runner.py functions,
providing a consistent interface with the Polars engine.
"""

from __future__ import annotations

from typing import Any

from ..config import ReconcileConfig
from .base import ReconEngine


class SparkEngine(ReconEngine):
    """PySpark + Delta Lake reconciliation engine."""

    def setup(self, cfg: ReconcileConfig) -> None:
        from ..helpers import get_spark, ensure_schema_exists
        self._spark = get_spark()
        ensure_schema_exists(cfg)

    def validate_tables(self, cfg: ReconcileConfig) -> None:
        from ..helpers import validate_columns_exist
        validate_columns_exist(cfg)

    def resolve_compare_cols(self, cfg: ReconcileConfig) -> list[str]:
        from ..helpers import resolve_all_compare_cols
        return resolve_all_compare_cols(cfg)

    def phase0_quarter_screening(
        self, cfg: ReconcileConfig, all_compare_cols: list[str]
    ) -> tuple[list[Any], Any]:
        from ..phases import phase0_quarter_screening
        return phase0_quarter_screening(cfg, all_compare_cols)

    def phase1_hash_extraction(
        self,
        cfg: ReconcileConfig,
        changed_quarters: list[Any],
        all_compare_cols: list[str],
        groups: list[list[str]],
    ) -> tuple[Any, Any]:
        from ..phases import phase1_hash_extraction
        return phase1_hash_extraction(cfg, changed_quarters, all_compare_cols, groups)

    def phase2_key_recon(
        self,
        cfg: ReconcileConfig,
        left_hashes: Any,
        right_hashes: Any,
        num_groups: int,
    ) -> tuple[Any, Any]:
        from ..phases import phase2_key_recon_and_row_triage
        return phase2_key_recon_and_row_triage(cfg, left_hashes, right_hashes, num_groups)

    def phase2b_nonnull_counts(
        self,
        cfg: ReconcileConfig,
        all_compare_cols: list[str],
        quarter_status: Any,
    ) -> Any:
        from ..phases import compute_nonnull_counts
        return compute_nonnull_counts(cfg, all_compare_cols, quarter_status)

    def phase3_group_triage(
        self,
        cfg: ReconcileConfig,
        changed_keys: Any,
        num_groups: int,
    ) -> dict[int, Any]:
        from ..phases import phase3_group_triage
        return phase3_group_triage(cfg, changed_keys, num_groups)

    def phase4_targeted_comparison(
        self,
        cfg: ReconcileConfig,
        changed_quarters: list[Any],
        groups: list[list[str]],
        group_changed_keys: dict[int, Any],
        all_compare_cols: list[str],
        total_matched_per_qtr: Any,
        nonnull_counts: Any,
    ) -> None:
        from ..phases import phase4_targeted_comparison
        phase4_targeted_comparison(
            cfg, changed_quarters, groups, group_changed_keys,
            all_compare_cols, total_matched_per_qtr, nonnull_counts,
        )

    def phase5_rollups(
        self,
        cfg: ReconcileConfig,
        changed_quarters: list[Any],
        quarter_status: Any,
        all_compare_cols: list[str],
        groups: list[list[str]],
        group_changed_keys: dict[int, Any],
        total_matched_per_qtr: Any,
        nonnull_counts: Any,
    ) -> None:
        from ..phases import (
            emit_zero_fill_for_identical_quarters,
            emit_zero_fill_for_unchanged_groups,
            emit_row_status_for_identical_quarters,
            emit_row_status_for_onesided_quarters,
            build_rollups,
        )
        emit_row_status_for_identical_quarters(cfg, quarter_status)
        emit_row_status_for_onesided_quarters(cfg, quarter_status)
        emit_zero_fill_for_identical_quarters(cfg, quarter_status, all_compare_cols, nonnull_counts)
        if changed_quarters:
            emit_zero_fill_for_unchanged_groups(
                cfg, changed_quarters, groups, group_changed_keys,
                total_matched_per_qtr, nonnull_counts,
            )
        build_rollups(cfg)

    def cleanup(self, cfg: ReconcileConfig) -> None:
        from ..runner import cleanup_temp_tables_for_run
        if cfg.cleanup_tmp_tables:
            cleanup_temp_tables_for_run(cfg)

    def write_run_metadata(
        self, cfg: ReconcileConfig, all_compare_cols: list[str], noncritical_cols: list[str]
    ) -> None:
        from ..runner import create_run_metadata
        create_run_metadata(cfg, all_compare_cols, noncritical_cols)

    def mark_run_complete(self, cfg: ReconcileConfig, status: str) -> None:
        from ..runner import mark_run_complete
        mark_run_complete(cfg, status)
