"""
Functional tests for the Polars reconciliation engine.

Tests run against local Delta tables using Polars + deltalake.
No PySpark dependency — uses the Polars-native data generator.
"""

from __future__ import annotations

import os
from datetime import date

import pytest

pytestmark = pytest.mark.polars

polars = pytest.importorskip("polars", reason="Polars not installed")
deltalake = pytest.importorskip("deltalake", reason="deltalake not installed")

import polars as pl

from recon.config import ReconcileConfig
from recon.engines import get_engine
from recon.helpers import build_column_groups


@pytest.fixture(scope="module")
def polars_test_env(tmp_path_factory):
    """Generate test data using the Polars-native data generator."""
    from tests.polars_data_generator import generate_test_data

    base_dir = str(tmp_path_factory.mktemp("polars_functional"))
    output_dir = os.path.join(base_dir, "polars_output")
    os.makedirs(output_dir, exist_ok=True)

    left_path, right_path, critical_cols = generate_test_data(
        output_path=base_dir,
        n_quarters=3,
        base_rows_per_quarter=1000,
        rows_per_quarter_increment=0,
        n_numeric_cols=10,
        n_string_cols=3,
        n_date_cols=2,
        n_bool_cols=2,
        seed=42,
    )

    return {
        "base_dir": base_dir,
        "output_dir": output_dir,
        "left_path": left_path,
        "right_path": right_path,
        "critical_cols": critical_cols,
    }


class TestPolarsIdentical:
    """Polars engine: identical tables should produce zero mismatches."""

    def test_identical_tables(self, polars_test_env):
        env = polars_test_env
        output_dir = os.path.join(env["output_dir"], "identical")
        os.makedirs(output_dir, exist_ok=True)

        cfg = ReconcileConfig(
            left_table_name=env["left_path"],
            right_table_name=env["right_path"],
            output_catalog="local",
            output_schema=output_dir,
            key_cols=["policy_id", "quarter_date"],
            qtr_col="quarter_date",
            critical_cols=env["critical_cols"],
            all_feature_cols=env["critical_cols"],
            run_id="polars_identical_001",
            source_label="TEST",
            engine="polars",
            detail_mode="sample",
            cleanup_tmp_tables=True,
        )

        engine = get_engine("polars")
        engine.setup(cfg)
        engine.validate_tables(cfg)
        all_compare_cols = engine.resolve_compare_cols(cfg)
        noncritical_cols = [c for c in all_compare_cols if c not in set(cfg.critical_cols)]
        groups = build_column_groups(all_compare_cols, list(cfg.critical_cols), cfg.hash_group_size)

        engine.write_run_metadata(cfg, all_compare_cols, noncritical_cols)
        changed_quarters, quarter_status = engine.phase0_quarter_screening(cfg, all_compare_cols)

        # Identical tables: no changed quarters
        assert len(changed_quarters) == 0

        # All quarters should be identical
        identical_count = quarter_status.filter(pl.col("quarter_status") == "identical").height
        assert identical_count == 3


class TestPolarsWithDifferences:
    """Polars engine: detect injected differences."""

    def test_value_changes(self, polars_test_env):
        from tests.polars_data_generator import inject_differences

        env = polars_test_env
        output_dir = os.path.join(env["output_dir"], "changes")
        os.makedirs(output_dir, exist_ok=True)

        # Inject 10% changes in first 3 columns
        critical_cols = env["critical_cols"]
        target_cols = critical_cols[:3]
        modified_path = inject_differences(
            source_path=env["left_path"],
            output_path=os.path.join(env["base_dir"], "polars_right_modified"),
            change_rate=0.10,
            change_cols=target_cols,
            seed=77,
        )

        cfg = ReconcileConfig(
            left_table_name=env["left_path"],
            right_table_name=modified_path,
            output_catalog="local",
            output_schema=output_dir,
            key_cols=["policy_id", "quarter_date"],
            qtr_col="quarter_date",
            critical_cols=critical_cols,
            all_feature_cols=critical_cols,
            run_id="polars_changes_001",
            source_label="TEST",
            engine="polars",
            detail_mode="sample",
            cleanup_tmp_tables=True,
        )

        engine = get_engine("polars")
        engine.setup(cfg)
        engine.validate_tables(cfg)
        all_compare_cols = engine.resolve_compare_cols(cfg)
        noncritical_cols = [c for c in all_compare_cols if c not in set(cfg.critical_cols)]
        groups = build_column_groups(all_compare_cols, list(cfg.critical_cols), cfg.hash_group_size)

        engine.write_run_metadata(cfg, all_compare_cols, noncritical_cols)
        changed_quarters, quarter_status = engine.phase0_quarter_screening(cfg, all_compare_cols)

        # Should detect changes
        assert len(changed_quarters) > 0

        nonnull_counts = engine.phase2b_nonnull_counts(cfg, all_compare_cols, quarter_status)
        left_hashes, right_hashes = engine.phase1_hash_extraction(
            cfg, changed_quarters, all_compare_cols, groups
        )
        changed_keys, total_matched_per_qtr = engine.phase2_key_recon(
            cfg, left_hashes, right_hashes, len(groups)
        )

        # Should have changed keys
        assert changed_keys.height > 0

        group_changed_keys = engine.phase3_group_triage(cfg, changed_keys, len(groups))

        engine.phase4_targeted_comparison(
            cfg, changed_quarters, groups, group_changed_keys,
            all_compare_cols, total_matched_per_qtr, nonnull_counts,
        )

        engine.phase5_rollups(
            cfg, changed_quarters, quarter_status, all_compare_cols,
            groups, group_changed_keys, total_matched_per_qtr, nonnull_counts,
        )
        engine.mark_run_complete(cfg, "COMPLETED")

        # Verify summary artifact was written
        summary_path = os.path.join(output_dir, "recon_column_summary_by_quarter")
        assert os.path.exists(summary_path)

        summary = pl.scan_delta(summary_path).filter(
            pl.col("run_id") == "polars_changes_001"
        ).collect()
        assert summary.height > 0

        # Target columns should have mismatches
        for col in target_cols:
            col_data = summary.filter(pl.col("column") == col)
            total_mismatches = col_data.select(pl.col("mismatch_count").sum()).item()
            assert total_mismatches > 0, f"{col} should have mismatches"
