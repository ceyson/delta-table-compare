"""
Polars engine — native Polars + delta-rs implementation.

Provides the same reconciliation phases as the Spark engine but uses
Polars LazyFrames and the deltalake Python package for I/O.
"""

from __future__ import annotations

import hashlib
import os
import time as _time
from datetime import datetime
from typing import Any, Optional

import pyarrow as pa

from ..config import ReconcileConfig
from ..helpers import get_write_timings
from .base import ReconEngine

try:
    import polars as pl
except ImportError as e:
    raise ImportError(
        "Polars engine requires the 'polars' package. "
        "Install with: pip install 'recon[polars]'"
    ) from e

try:
    from deltalake import DeltaTable, write_deltalake
    _HAS_DELTALAKE = True
except ImportError:
    _HAS_DELTALAKE = False


# ---------------------------------------------------------------------------
# Environment detection
# ---------------------------------------------------------------------------

def _is_databricks() -> bool:
    """Detect if running inside a Databricks cluster."""
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _is_path(table_name: str) -> bool:
    """Return True if *table_name* looks like a filesystem path."""
    return "/" in table_name or "\\" in table_name


# ---------------------------------------------------------------------------
# Read helpers
# ---------------------------------------------------------------------------

def _read_delta(table_name: str) -> pl.LazyFrame:
    """Read a Delta table as a Polars LazyFrame.

    - **Local**: uses ``pl.scan_delta(path)``.
    - **Databricks**: reads via Spark, converts Arrow → Polars lazy.
    """
    if _is_databricks():
        return _read_delta_via_spark(table_name)
    if not _HAS_DELTALAKE:
        raise ImportError(
            "The 'deltalake' package is required for local Delta reads. "
            "Install with: pip install 'recon[polars]'"
        )
    return pl.scan_delta(table_name)


def _read_delta_via_spark(table_name: str) -> pl.LazyFrame:
    """Read a Delta table through Spark and return a Polars LazyFrame."""
    from ..helpers import get_spark
    spark = get_spark()
    if _is_path(table_name):
        sdf = spark.read.format("delta").load(table_name)
    else:
        sdf = spark.table(table_name)
    arrow_table = sdf.toPandas()  # Spark → Pandas (Arrow-optimised)
    return pl.from_pandas(arrow_table).lazy()


# ---------------------------------------------------------------------------
# Write helpers
# ---------------------------------------------------------------------------

def _write_delta_append(df: pl.DataFrame, table_path: str) -> None:
    """Append a Polars DataFrame to a Delta table."""
    t0 = _time.perf_counter()
    if _is_databricks():
        _write_via_spark(df, table_path, mode="append")
    else:
        _write_local(df, table_path, mode="append")
    elapsed = _time.perf_counter() - t0
    get_write_timings().record(table_path, "append", elapsed, row_count=df.height)


def _overwrite_delta(df: pl.DataFrame, table_path: str) -> None:
    """Overwrite a Delta table with a Polars DataFrame."""
    t0 = _time.perf_counter()
    if _is_databricks():
        _write_via_spark(df, table_path, mode="overwrite")
    else:
        _write_local(df, table_path, mode="overwrite")
    elapsed = _time.perf_counter() - t0
    get_write_timings().record(table_path, "overwrite", elapsed, row_count=df.height)


def _write_via_spark(df: pl.DataFrame, table_name: str, mode: str) -> None:
    """Write a Polars DataFrame to Delta via Spark (Databricks path)."""
    from ..helpers import get_spark
    spark = get_spark()
    # Spark does not support unsigned Arrow types and uses LongType/DoubleType
    # by default. Cast all narrow/unsigned types to their Spark-compatible equivalents.
    _UPCAST_MAP = {
        pl.UInt8: pl.Int16,
        pl.UInt16: pl.Int32,
        pl.UInt32: pl.Int64,
        pl.UInt64: pl.Int64,
        pl.Int8: pl.Int16,
        pl.Int16: pl.Int32,
        pl.Int32: pl.Int64,
        pl.Float32: pl.Float64,
    }
    cast_map = {}
    for col_name, dtype in zip(df.columns, df.dtypes):
        if dtype in _UPCAST_MAP:
            cast_map[col_name] = _UPCAST_MAP[dtype]
    if cast_map:
        df = df.cast(cast_map)
    spark_df = spark.createDataFrame(df.to_pandas())
    writer = spark_df.write.format("delta").mode(mode)
    if mode == "overwrite":
        writer = writer.option("overwriteSchema", "true")
    if _is_path(table_name):
        writer.save(table_name)
    else:
        writer.option("mergeSchema", "true").saveAsTable(table_name)


def _write_local(df: pl.DataFrame, table_path: str, mode: str) -> None:
    """Write a Polars DataFrame to a local Delta table via deltalake."""
    if not _HAS_DELTALAKE:
        raise ImportError(
            "The 'deltalake' package is required for local Delta writes. "
            "Install with: pip install 'recon[polars]'"
        )
    arrow_table = df.to_arrow()
    if mode == "append":
        if os.path.exists(table_path) and os.path.isdir(table_path) and os.path.exists(os.path.join(table_path, "_delta_log")):
            write_deltalake(table_path, arrow_table, mode="append", schema_mode="merge")
        else:
            os.makedirs(table_path, exist_ok=True)
            write_deltalake(table_path, arrow_table, mode="overwrite")
    else:
        os.makedirs(table_path, exist_ok=True)
        write_deltalake(table_path, arrow_table, mode="overwrite", schema_mode="overwrite")


def _hash_row_cols(df: pl.LazyFrame, cols: list[str], hash_name: str) -> pl.Expr:
    """Create a hash expression over multiple columns using Polars native hash."""
    # Polars hash works on a struct of columns
    # We cast all to string, concat, then hash for determinism
    exprs = []
    for c in cols:
        exprs.append(pl.col(c).cast(pl.Utf8).fill_null("__NULL__"))
    concat_expr = pl.concat_str(exprs, separator="|")
    return concat_expr.hash().alias(hash_name)


def _table_ref(cfg: ReconcileConfig, logical_name: str) -> str:
    """Build a table reference for a final artifact table.

    - **Local**: returns a filesystem path.
    - **Databricks**: returns a fully-qualified UC table name.
    """
    table_leaf = f"{cfg.final_prefix}_{logical_name}"
    if _is_databricks():
        from ..helpers import table_fqn
        return table_fqn(cfg.output_catalog, cfg.output_schema, table_leaf)
    return os.path.join(cfg.output_schema, table_leaf)


def _tmp_ref(cfg: ReconcileConfig, logical_name: str) -> str:
    """Build a table reference for a temporary intermediate table."""
    safe_run = cfg.run_id.replace("-", "_").replace(":", "_").replace(" ", "_")
    table_leaf = f"{cfg.temp_prefix}_{logical_name}_{safe_run}"
    if _is_databricks():
        from ..helpers import table_fqn
        return table_fqn(cfg.output_catalog, cfg.output_schema, table_leaf)
    return os.path.join(cfg.output_schema, table_leaf)


class PolarsEngine(ReconEngine):
    """Polars + delta-rs reconciliation engine."""

    def setup(self, cfg: ReconcileConfig) -> None:
        """Prepare the output location."""
        if not _is_databricks():
            os.makedirs(cfg.output_schema, exist_ok=True)

    def validate_tables(self, cfg: ReconcileConfig) -> None:
        """Verify source tables exist and required columns are present."""
        left_df = _read_delta(cfg.left_table_name)
        right_df = _read_delta(cfg.right_table_name)
        left_cols = set(left_df.collect_schema().names())
        right_cols = set(right_df.collect_schema().names())
        required = set(cfg.key_cols) | set(cfg.critical_cols)
        missing_left = sorted(required - left_cols)
        missing_right = sorted(required - right_cols)
        if missing_left:
            raise ValueError(f"Missing columns from left table: {missing_left[:50]}")
        if missing_right:
            raise ValueError(f"Missing columns from right table: {missing_right[:50]}")

    def resolve_compare_cols(self, cfg: ReconcileConfig) -> list[str]:
        """Determine the full set of columns to compare."""
        left_cols = set(_read_delta(cfg.left_table_name).collect_schema().names())
        right_cols = set(_read_delta(cfg.right_table_name).collect_schema().names())
        common = left_cols & right_cols
        excluded = set(cfg.key_cols)
        if cfg.all_feature_cols is not None:
            return sorted(c for c in cfg.all_feature_cols if c in common and c not in excluded)
        return sorted(common - excluded)

    def phase0_quarter_screening(
        self, cfg: ReconcileConfig, all_compare_cols: list[str]
    ) -> tuple[list[Any], Any]:
        """Quarter-level checksum screening using Polars."""
        print("Phase 0: Computing quarter-level checksums (Polars)...")

        hash_cols = list(cfg.key_cols) + all_compare_cols

        left_lf = _read_delta(cfg.left_table_name)
        right_lf = _read_delta(cfg.right_table_name)

        # Compute per-quarter checksums (sum of row hashes, reinterpret u64 -> i64)
        left_ck = (
            left_lf
            .with_columns(_hash_row_cols(left_lf, hash_cols, "_row_hash"))
            .group_by(cfg.qtr_col)
            .agg(
                pl.col("_row_hash").reinterpret(signed=True).sum().alias("quarter_checksum"),
                pl.len().alias("row_count"),
            )
            .collect()
        )

        right_ck = (
            right_lf
            .with_columns(_hash_row_cols(right_lf, hash_cols, "_row_hash"))
            .group_by(cfg.qtr_col)
            .agg(
                pl.col("_row_hash").reinterpret(signed=True).sum().alias("quarter_checksum"),
                pl.len().alias("row_count"),
            )
            .collect()
        )

        # Full outer join on quarter
        joined = left_ck.join(
            right_ck, on=cfg.qtr_col, how="full", suffix="_right"
        )

        # Determine status
        quarter_status = joined.with_columns(
            pl.when(pl.col("quarter_checksum").is_null())
            .then(pl.lit("right_only"))
            .when(pl.col("quarter_checksum_right").is_null())
            .then(pl.lit("left_only"))
            .when(pl.col("quarter_checksum") == pl.col("quarter_checksum_right"))
            .then(pl.lit("identical"))
            .otherwise(pl.lit("changed"))
            .alias("quarter_status"),
            pl.lit(cfg.run_id).alias("run_id"),
            pl.lit(cfg.source_label).alias("source_label"),
        ).rename({
            "quarter_checksum": "left_checksum",
            "quarter_checksum_right": "right_checksum",
            "row_count": "left_row_count",
            "row_count_right": "right_row_count",
        })

        # Write quarter checksums artifact
        artifact = quarter_status.select(
            "run_id", "source_label", cfg.qtr_col,
            "left_checksum", "right_checksum",
            "left_row_count", "right_row_count", "quarter_status",
        )
        _write_delta_append(artifact, _table_ref(cfg, "quarter_checksums"))

        # Extract changed quarters
        changed_quarters = (
            quarter_status
            .filter(pl.col("quarter_status") == "changed")
            .select(cfg.qtr_col)
            .sort(cfg.qtr_col)
            .to_series()
            .to_list()
        )

        identical_count = quarter_status.filter(pl.col("quarter_status") == "identical").height
        onesided_count = quarter_status.filter(pl.col("quarter_status").is_in(["left_only", "right_only"])).height
        total_quarters = quarter_status.height
        print(f"Phase 0: {identical_count}/{total_quarters} quarters identical — skipped.")
        print(f"Phase 0: {onesided_count} quarters one-sided.")
        print(f"Phase 0: {len(changed_quarters)} quarters require row-level analysis.")

        return changed_quarters, quarter_status

    def phase1_hash_extraction(
        self,
        cfg: ReconcileConfig,
        changed_quarters: list[Any],
        all_compare_cols: list[str],
        groups: list[list[str]],
    ) -> tuple[Any, Any]:
        """Row-level hash extraction using Polars."""
        print(f"Phase 1: Extracting row hashes for {len(changed_quarters)} quarters, {len(groups)} groups (Polars)...")

        def compute_hashes(table_name: str) -> pl.DataFrame:
            lf = _read_delta(table_name).filter(pl.col(cfg.qtr_col).is_in(changed_quarters))

            # Build group hash expressions
            hash_exprs = []
            all_hash_cols = []
            for gi, grp in enumerate(groups):
                hash_exprs.append(_hash_row_cols(lf, grp, f"gh_{gi}"))
                all_hash_cols.extend(grp)

            # Overall row hash
            row_hash_expr = _hash_row_cols(lf, all_hash_cols, "row_hash_all")

            select_cols = [pl.col(c) for c in cfg.key_cols] + [row_hash_expr] + hash_exprs
            return lf.select(select_cols).collect()

        left_hashes = compute_hashes(cfg.left_table_name)
        print("Phase 1: Left hashes computed.")

        right_hashes = compute_hashes(cfg.right_table_name)
        print("Phase 1: Right hashes computed.")

        return left_hashes, right_hashes

    def phase2_key_recon(
        self,
        cfg: ReconcileConfig,
        left_hashes: Any,
        right_hashes: Any,
        num_groups: int,
    ) -> tuple[Any, Any]:
        """Key reconciliation and row triage using Polars."""
        print("Phase 2: Key reconciliation and row triage (Polars)...")

        key_cols = list(cfg.key_cols)

        # Full outer join
        joined = left_hashes.join(
            right_hashes, on=key_cols, how="full", suffix="_r"
        )

        # Determine row status
        left_present = pl.col("row_hash_all").is_not_null()
        right_present = pl.col("row_hash_all_r").is_not_null()

        joined = joined.with_columns(
            pl.when(left_present & right_present).then(pl.lit("matched"))
            .when(left_present & ~right_present).then(pl.lit("left_only"))
            .when(~left_present & right_present).then(pl.lit("right_only"))
            .otherwise(pl.lit("unknown"))
            .alias("row_status"),
            pl.lit(cfg.run_id).alias("run_id"),
            pl.lit(cfg.source_label).alias("source_label"),
        )

        # Row status counts
        row_status_counts = (
            joined
            .group_by("run_id", "source_label", cfg.qtr_col, "row_status")
            .agg(pl.len().alias("row_count"))
        )
        _write_delta_append(row_status_counts, _table_ref(cfg, "row_status_counts"))

        # Total matched per quarter
        total_matched_per_qtr = (
            joined
            .filter(pl.col("row_status") == "matched")
            .group_by(cfg.qtr_col)
            .agg(pl.len().alias("total_matched_count"))
        )

        # Changed keys: matched rows where row_hash differs
        matched = joined.filter(pl.col("row_status") == "matched")
        changed_keys = matched.filter(
            pl.col("row_hash_all") != pl.col("row_hash_all_r")
        )

        # Add per-group match flags
        gh_match_exprs = []
        for gi in range(num_groups):
            gh_match_exprs.append(
                (pl.col(f"gh_{gi}") == pl.col(f"gh_{gi}_r")).alias(f"gh_{gi}_match")
            )
        changed_keys = changed_keys.select(
            [pl.col(c) for c in key_cols] + gh_match_exprs
        )

        changed_count = changed_keys.height
        matched_count = matched.height
        print(
            f"Phase 2: {matched_count} matched rows, {changed_count} have differences "
            f"({100.0 * changed_count / max(matched_count, 1):.1f}%)."
        )

        return changed_keys, total_matched_per_qtr

    def phase2b_nonnull_counts(
        self,
        cfg: ReconcileConfig,
        all_compare_cols: list[str],
        quarter_status: Any,
    ) -> Any:
        """Compute per-column nonnull counts using vectorized Polars operations."""
        both_sided = quarter_status.filter(
            pl.col("quarter_status").is_in(["identical", "changed"])
        ).select(cfg.qtr_col).to_series().to_list()

        if not both_sided:
            return pl.DataFrame(schema={cfg.qtr_col: pl.Date, "column": pl.Utf8, "nonnull_count": pl.Int64})

        key_cols = list(cfg.key_cols)

        left_lf = _read_delta(cfg.left_table_name).filter(pl.col(cfg.qtr_col).is_in(both_sided))
        right_lf = _read_delta(cfg.right_table_name).filter(pl.col(cfg.qtr_col).is_in(both_sided))

        # Inner join on keys — only select key cols + compare cols to reduce memory
        left_select = key_cols + [c for c in all_compare_cols if c not in key_cols]
        right_select = key_cols + [c for c in all_compare_cols if c not in key_cols]
        joined = (
            left_lf.select(left_select)
            .join(right_lf.select(right_select), on=key_cols, how="inner", suffix="_r")
            .collect()
        )

        # Vectorized: build "both non-null" expression per column, group by quarter
        both_nn_exprs = []
        for col in all_compare_cols:
            rcol = f"{col}_r" if f"{col}_r" in joined.columns else col
            if rcol in joined.columns and col in joined.columns:
                both_nn_exprs.append(
                    (pl.col(col).is_not_null() & pl.col(rcol).is_not_null())
                    .cast(pl.UInt32).alias(col)
                )

        if not both_nn_exprs:
            return pl.DataFrame(schema={cfg.qtr_col: pl.Date, "column": pl.Utf8, "nonnull_count": pl.Int64})

        # Single group_by: quarter → sum of each both-nonnull column
        nonnull_wide = (
            joined
            .select([pl.col(cfg.qtr_col)] + both_nn_exprs)
            .group_by(cfg.qtr_col)
            .agg([pl.col(col).sum() for col in all_compare_cols])
        )

        # Unpivot to long form: quarter_date, column, nonnull_count
        nonnull_df = nonnull_wide.unpivot(
            index=cfg.qtr_col,
            on=all_compare_cols,
            variable_name="column",
            value_name="nonnull_count",
        ).with_columns(pl.col("nonnull_count").cast(pl.Int64))

        print(f"Phase 2b: Nonnull counts computed for {len(all_compare_cols)} columns across {len(both_sided)} quarters.")
        return nonnull_df

    def phase3_group_triage(
        self,
        cfg: ReconcileConfig,
        changed_keys: Any,
        num_groups: int,
    ) -> dict[int, Any]:
        """Group triage using Polars."""
        print(f"Phase 3: Triaging {num_groups} column groups (Polars)...")

        key_cols = list(cfg.key_cols)
        group_changed_keys: dict[int, pl.DataFrame] = {}

        for gi in range(num_groups):
            match_col = f"gh_{gi}_match"
            keys_for_group = changed_keys.filter(
                pl.col(match_col) == False  # noqa: E712
            ).select(key_cols)
            group_changed_keys[gi] = keys_for_group

        nonzero = sum(1 for gi in range(num_groups) if group_changed_keys[gi].height > 0)
        print(f"Phase 3: {nonzero}/{num_groups} groups have changes. Total changed rows: {changed_keys.height}.")

        for gi in range(num_groups):
            cnt = group_changed_keys[gi].height
            if cnt > 0:
                print(f"  Group {gi}: {cnt} rows changed.")

        return group_changed_keys

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
        """Targeted column comparison using Polars."""
        print("Phase 4: Targeted column comparison (Polars)...")

        key_cols = list(cfg.key_cols)
        batch_count = 0

        for gi, grp_cols in enumerate(groups):
            changed_keys_df = group_changed_keys[gi]
            if changed_keys_df.height == 0:
                continue

            print(f"  Group {gi}: {len(grp_cols)} columns, {changed_keys_df.height} changed rows...")

            # Read source data for changed quarters, filter to changed keys
            left_data = (
                _read_delta(cfg.left_table_name)
                .filter(pl.col(cfg.qtr_col).is_in(changed_quarters))
                .select([pl.col(c) for c in key_cols + grp_cols])
                .collect()
                .join(changed_keys_df, on=key_cols, how="inner")
            )

            right_data = (
                _read_delta(cfg.right_table_name)
                .filter(pl.col(cfg.qtr_col).is_in(changed_quarters))
                .select([pl.col(c) for c in key_cols + grp_cols])
                .collect()
                .join(changed_keys_df, on=key_cols, how="inner")
            )

            # Join left and right on keys
            joined = left_data.join(right_data, on=key_cols, how="inner", suffix="_r")

            # Per-column comparison
            summary_rows = []
            mismatch_rows = []

            for col in grp_cols:
                rcol = f"{col}_r"
                if rcol not in joined.columns:
                    continue

                # Compute mismatch conditions
                lcol_s = joined.get_column(col)
                rcol_s = joined.get_column(rcol)

                both_null = lcol_s.is_null() & rcol_s.is_null()
                one_null = (lcol_s.is_null() & rcol_s.is_not_null()) | (lcol_s.is_not_null() & rcol_s.is_null())

                # Check if numeric
                is_numeric = lcol_s.dtype in (pl.Float64, pl.Float32, pl.Int64, pl.Int32, pl.Int16, pl.Int8)
                tolerance = float(cfg.tolerances.get(col, cfg.default_numeric_tolerance if is_numeric else 0.0))

                if is_numeric:
                    diff = rcol_s.cast(pl.Float64) - lcol_s.cast(pl.Float64)
                    abs_diff = diff.abs()
                    value_mismatch = (~both_null) & (~one_null) & (abs_diff > tolerance)
                    mismatch_mask = one_null | value_mismatch
                else:
                    mismatch_mask = ~(lcol_s == rcol_s) & ~both_null
                    diff = None
                    abs_diff = None

                # Per-quarter summary
                for qtr_val in changed_quarters:
                    qtr_mask = joined.get_column(cfg.qtr_col) == qtr_val
                    qtr_mismatches = (mismatch_mask & qtr_mask).sum()
                    qtr_null_mm = (one_null & qtr_mask).sum()
                    qtr_changed_rows = qtr_mask.sum()
                    qtr_nonnull = ((~one_null) & (~both_null) & qtr_mask).sum()

                    # Get matched count for this quarter
                    matched_row = total_matched_per_qtr.filter(pl.col(cfg.qtr_col) == qtr_val)
                    matched_count = matched_row.item(0, "total_matched_count") if matched_row.height > 0 else qtr_changed_rows

                    # Get nonnull count from precomputed
                    nn_row = nonnull_counts.filter(
                        (pl.col(cfg.qtr_col) == qtr_val) & (pl.col("column") == col)
                    )
                    nonnull_compared = nn_row.item(0, "nonnull_count") if nn_row.height > 0 else qtr_nonnull

                    max_abs = None
                    if is_numeric and abs_diff is not None:
                        qtr_abs = abs_diff.filter(qtr_mask & mismatch_mask)
                        if len(qtr_abs) > 0:
                            max_abs = qtr_abs.max()

                    mismatch_pct = qtr_mismatches / matched_count if matched_count > 0 else None
                    null_mm_pct = qtr_null_mm / matched_count if matched_count > 0 else None

                    summary_rows.append({
                        "run_id": cfg.run_id,
                        "source_label": cfg.source_label,
                        cfg.qtr_col: qtr_val,
                        "column": col,
                        "is_numeric": is_numeric,
                        "tolerance": tolerance,
                        "changed_row_count": int(qtr_changed_rows),
                        "nonnull_compared_count": int(nonnull_compared),
                        "mismatch_count": int(qtr_mismatches),
                        "null_mismatch_count": int(qtr_null_mm),
                        "max_abs_diff": float(max_abs) if max_abs is not None else None,
                        "matched_row_count": int(matched_count),
                        "mismatch_pct": float(mismatch_pct) if mismatch_pct is not None else None,
                        "null_mismatch_pct": float(null_mm_pct) if null_mm_pct is not None else None,
                    })

                # Mismatch samples
                if cfg.detail_mode in {"sample", "full_direct"}:
                    mm_indices = mismatch_mask
                    mm_data = joined.filter(mm_indices)
                    if mm_data.height > 0:
                        for qtr_val in changed_quarters:
                            qtr_mm = mm_data.filter(pl.col(cfg.qtr_col) == qtr_val)
                            sample = qtr_mm.head(cfg.sample_per_column)
                            for row in sample.iter_rows(named=True):
                                mismatch_rows.append({
                                    "run_id": cfg.run_id,
                                    "source_label": cfg.source_label,
                                    **{k: row[k] for k in key_cols},
                                    "column": col,
                                    cfg.left_label: str(row[col]) if row[col] is not None else None,
                                    cfg.right_label: str(row[rcol]) if row[rcol] is not None else None,
                                })

            # Write summary
            if summary_rows:
                summary_df = pl.DataFrame(summary_rows)
                _write_delta_append(summary_df, _table_ref(cfg, "column_summary_by_quarter"))

            # Write mismatch samples
            if mismatch_rows:
                mm_df = pl.DataFrame(mismatch_rows)
                _write_delta_append(mm_df, _table_ref(cfg, "mismatch_sample"))

            batch_count += 1

        print(f"Phase 4: Complete ({batch_count} group batches processed).")

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
        """Zero-fill and rollups using Polars."""
        print("Phase 5: Building rollups (Polars)...")

        # Zero-fill for identical quarters
        identical = quarter_status.filter(pl.col("quarter_status") == "identical")
        if identical.height > 0:
            zero_rows = []
            for row in identical.iter_rows(named=True):
                qtr_val = row[cfg.qtr_col]
                matched = int(row["left_row_count"]) if row["left_row_count"] is not None else 0
                for col in all_compare_cols:
                    nn_row = nonnull_counts.filter(
                        (pl.col(cfg.qtr_col) == qtr_val) & (pl.col("column") == col)
                    )
                    nonnull = nn_row.item(0, "nonnull_count") if nn_row.height > 0 else matched
                    zero_rows.append({
                        "run_id": cfg.run_id,
                        "source_label": cfg.source_label,
                        cfg.qtr_col: qtr_val,
                        "column": col,
                        "is_numeric": False,
                        "tolerance": 0.0,
                        "changed_row_count": 0,
                        "nonnull_compared_count": int(nonnull),
                        "mismatch_count": 0,
                        "null_mismatch_count": 0,
                        "max_abs_diff": None,
                        "matched_row_count": matched,
                        "mismatch_pct": 0.0,
                        "null_mismatch_pct": 0.0,
                    })
            if zero_rows:
                _write_delta_append(pl.DataFrame(zero_rows), _table_ref(cfg, "column_summary_by_quarter"))

        # Build all-quarter rollups
        summary_ref = _table_ref(cfg, "column_summary_by_quarter")
        try:
            summary = _read_delta(summary_ref).filter(
                pl.col("run_id") == cfg.run_id
            ).collect()
        except Exception:
            summary = pl.DataFrame()

        if summary.height > 0:
            rollup = (
                summary
                .group_by("run_id", "source_label", "column")
                .agg(
                    pl.col("matched_row_count").sum().alias("matched_row_count"),
                    pl.col("nonnull_compared_count").sum().alias("nonnull_compared_count"),
                    pl.col("mismatch_count").sum().alias("mismatch_count"),
                    pl.col("null_mismatch_count").sum().alias("null_mismatch_count"),
                    pl.col("max_abs_diff").max().alias("max_abs_diff"),
                )
                .with_columns(
                    pl.when(pl.col("matched_row_count") > 0)
                    .then(pl.col("mismatch_count") / pl.col("matched_row_count"))
                    .otherwise(None)
                    .alias("mismatch_pct"),
                    pl.when(pl.col("matched_row_count") > 0)
                    .then(pl.col("null_mismatch_count") / pl.col("matched_row_count"))
                    .otherwise(None)
                    .alias("null_mismatch_pct"),
                )
            )
            _write_delta_append(rollup, _table_ref(cfg, "column_summary_all_quarters"))

            # Noisy column detection
            noisy = rollup.filter(
                (pl.col("matched_row_count") > 0)
                & (pl.col("mismatch_pct") >= cfg.noisy_column_threshold)
            ).with_columns(pl.lit("change_rate_above_threshold").alias("suspected_reason"))

            if noisy.height > 0:
                _write_delta_append(
                    noisy.select("run_id", "source_label", "column", "matched_row_count",
                                 "mismatch_count", "mismatch_pct", "suspected_reason"),
                    _table_ref(cfg, "noisy_columns"),
                )
                print(f"Phase 5: {noisy.height} columns flagged as noisy.")
            else:
                print("Phase 5: No noisy columns detected.")

        print("Phase 5: Rollups complete.")

    def cleanup(self, cfg: ReconcileConfig) -> None:
        """Remove temp tables / directories."""
        if _is_databricks():
            from ..helpers import get_spark
            spark = get_spark()
            safe_run = cfg.run_id.replace("-", "_").replace(":", "_").replace(" ", "_")
            prefix = f"{cfg.temp_prefix}_"
            try:
                tables = spark.sql(
                    f"SHOW TABLES IN `{cfg.output_catalog}`.`{cfg.output_schema}` LIKE '{prefix}*{safe_run}'"
                ).collect()
                for row in tables:
                    tname = row["tableName"]
                    spark.sql(f"DROP TABLE IF EXISTS `{cfg.output_catalog}`.`{cfg.output_schema}`.`{tname}`")
            except Exception as exc:
                print(f"WARNING: Cleanup failed: {exc}")
        else:
            import shutil
            base = cfg.output_schema
            safe_run = cfg.run_id.replace("-", "_").replace(":", "_").replace(" ", "_")
            prefix = f"{cfg.temp_prefix}_"
            if os.path.isdir(base):
                for entry in os.listdir(base):
                    if entry.startswith(prefix) and safe_run in entry:
                        path = os.path.join(base, entry)
                        shutil.rmtree(path, ignore_errors=True)

        print("Cleanup: Temp tables removed.")

    def write_run_metadata(
        self, cfg: ReconcileConfig, all_compare_cols: list[str], noncritical_cols: list[str]
    ) -> None:
        """Write initial run metadata."""
        meta = pl.DataFrame([{
            "run_id": cfg.run_id,
            "source_label": cfg.source_label,
            "left_table_name": cfg.left_table_name,
            "right_table_name": cfg.right_table_name,
            "qtr_col": cfg.qtr_col,
            "critical_column_count": len(cfg.critical_cols),
            "noncritical_column_count": len(noncritical_cols),
            "total_compare_column_count": len(all_compare_cols),
            "hash_group_size": cfg.hash_group_size,
            "detail_mode": cfg.detail_mode,
            "started_at": datetime.now().isoformat(),
            "completed_at": None,
            "status": "RUNNING",
        }]).cast({
            "critical_column_count": pl.Int64,
            "noncritical_column_count": pl.Int64,
            "total_compare_column_count": pl.Int64,
            "hash_group_size": pl.Int64,
        })
        _write_delta_append(meta, _table_ref(cfg, "run_metadata"))

    def mark_run_complete(self, cfg: ReconcileConfig, status: str) -> None:
        """Update run metadata with final status."""
        # For Delta tables with Polars, we append a new row with the updated status.
        # A proper implementation would use DeltaTable merge, but for simplicity:
        meta = pl.DataFrame([{
            "run_id": cfg.run_id,
            "source_label": cfg.source_label,
            "left_table_name": cfg.left_table_name,
            "right_table_name": cfg.right_table_name,
            "qtr_col": cfg.qtr_col,
            "critical_column_count": len(cfg.critical_cols),
            "noncritical_column_count": 0,
            "total_compare_column_count": 0,
            "hash_group_size": cfg.hash_group_size,
            "detail_mode": cfg.detail_mode,
            "started_at": None,
            "completed_at": datetime.now().isoformat(),
            "status": status,
        }]).cast({
            "critical_column_count": pl.Int64,
            "noncritical_column_count": pl.Int64,
            "total_compare_column_count": pl.Int64,
            "hash_group_size": pl.Int64,
        })
        try:
            _write_delta_append(meta, _table_ref(cfg, "run_metadata"))
        except Exception as exc:
            print(f"WARNING: Could not update run_metadata: {exc}")
