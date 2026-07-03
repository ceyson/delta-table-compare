"""
Reconciliation phases 0–5.

Phase 0: Quarter-level checksum screening.
Phase 1: Single-scan row-level + group-level hash extraction.
Phase 2: Narrow join — key reconciliation + row triage.
Phase 2b: Accurate per-column nonnull counts.
Phase 3: Group triage — identify which column groups changed per row.
Phase 4: Targeted column comparison on changed rows × changed groups.
Phase 5: Rollups, zero-fill for identical data, noisy-column detection.
"""

from __future__ import annotations

from typing import Any, Sequence

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import ReconcileConfig
from .helpers import (
    get_spark,
    safe_unpersist,
    quote_name,
    colq,
    chunk_list,
    final_table,
    tmp_table,
    overwrite_delta_table,
    write_delta_append,
    get_schema_map,
    is_numeric_type,
    normalize_for_hash,
    build_column_groups,
    build_feature_meta,
)


# ===========================================================================
# Phase 0 — Quarter-level checksums
# ===========================================================================


def compute_quarter_checksums(
    table_name: str,
    cfg: ReconcileConfig,
    all_compare_cols: list[str],
    schema_map: dict[str, T.DataType],
) -> DataFrame:
    """Compute a single aggregate checksum per quarter for a table.

    Uses SUM of per-row xxhash64 so the result is order-independent.
    Returns DataFrame: (qtr_col, quarter_checksum).
    """
    hash_inputs: list[F.Column] = []
    for c in all_compare_cols:
        hash_inputs.append(F.lit(c))
        hash_inputs.append(normalize_for_hash(colq(c), schema_map[c], cfg))

    # Include key columns in the row hash so row identity is part of the checksum.
    key_hash_inputs: list[F.Column] = []
    for c in cfg.key_cols:
        key_hash_inputs.append(F.lit(c))
        key_hash_inputs.append(colq(c).cast("string"))

    row_hash = F.xxhash64(*(key_hash_inputs + hash_inputs))

    return (
        get_spark().table(table_name)
        .select(
            colq(cfg.qtr_col).alias(cfg.qtr_col),
            row_hash.alias("_row_hash"),
        )
        .groupBy(cfg.qtr_col)
        .agg(
            F.sum(F.col("_row_hash").cast("long")).alias("quarter_checksum"),
            F.count(F.lit(1)).alias("row_count"),
        )
    )


def phase0_quarter_screening(
    cfg: ReconcileConfig,
    all_compare_cols: list[str],
) -> tuple[list[Any], DataFrame]:
    """Compare quarter-level checksums between left and right tables.

    Returns:
        changed_quarters: list of quarter values that differ or are one-sided.
        quarter_status_df: DataFrame with per-quarter status for reporting.
    """
    left_schema = get_schema_map(cfg.left_table_name)
    right_schema = get_schema_map(cfg.right_table_name)

    # Use left schema for normalization (types should match for common cols).
    schema_map = {c: left_schema.get(c, right_schema.get(c)) for c in all_compare_cols}

    print("Phase 0: Computing quarter-level checksums...")
    left_ck = compute_quarter_checksums(
        cfg.left_table_name, cfg, all_compare_cols, schema_map
    ).alias("l")
    right_ck = compute_quarter_checksums(
        cfg.right_table_name, cfg, all_compare_cols, schema_map
    ).alias("r")

    joined = left_ck.join(right_ck, on=cfg.qtr_col, how="full_outer")

    quarter_status = joined.select(
        F.lit(cfg.run_id).alias("run_id"),
        F.lit(cfg.source_label).alias("source_label"),
        colq(cfg.qtr_col).alias(cfg.qtr_col),
        F.col("l.quarter_checksum").alias("left_checksum"),
        F.col("r.quarter_checksum").alias("right_checksum"),
        F.col("l.row_count").alias("left_row_count"),
        F.col("r.row_count").alias("right_row_count"),
        F.when(F.col("l.quarter_checksum").isNull(), F.lit("right_only"))
        .when(F.col("r.quarter_checksum").isNull(), F.lit("left_only"))
        .when(
            F.col("l.quarter_checksum") == F.col("r.quarter_checksum"),
            F.lit("identical"),
        )
        .otherwise(F.lit("changed"))
        .alias("quarter_status"),
    )

    quarter_status = quarter_status.cache()
    write_delta_append(quarter_status, final_table(cfg, "quarter_checksums"))

    # Collect quarters that need row-level hashing (both sides have data but differ).
    changed_rows = (
        quarter_status.where(F.col("quarter_status") == "changed")
        .select(cfg.qtr_col)
        .orderBy(cfg.qtr_col)
        .collect()
    )
    changed_quarters = [r[cfg.qtr_col] for r in changed_rows]

    # One-sided quarters are handled separately (row_status emitted from Phase 0 data).
    onesided_rows = (
        quarter_status.where(F.col("quarter_status").isin("left_only", "right_only"))
        .select(cfg.qtr_col)
        .collect()
    )

    identical_rows = (
        quarter_status.where(F.col("quarter_status") == "identical")
        .select(cfg.qtr_col, "left_row_count")
        .collect()
    )

    total_quarters = quarter_status.count()
    identical_count = len(identical_rows)
    onesided_count = len(onesided_rows)
    print(f"Phase 0: {identical_count}/{total_quarters} quarters identical — skipped.")
    print(f"Phase 0: {onesided_count} quarters one-sided.")
    print(f"Phase 0: {len(changed_quarters)} quarters require row-level analysis.")

    safe_unpersist(quarter_status)
    return changed_quarters, quarter_status


def emit_row_status_for_identical_quarters(
    cfg: ReconcileConfig, quarter_status_df: DataFrame
) -> None:
    """Emit row_status_counts rows for quarters whose checksums matched.

    For each identical quarter, writes a single 'matched' row with
    row_count equal to the quarter's left_row_count.

    Args:
        cfg: Active reconciliation configuration.
        quarter_status_df: DataFrame produced by :func:`phase0_quarter_screening`
            containing quarter_status and row counts.
    """
    identical = quarter_status_df.where(F.col("quarter_status") == "identical").select(
        cfg.qtr_col, "left_row_count"
    )
    identical_collected = identical.collect()
    if not identical_collected:
        return

    rows = [
        (
            cfg.run_id,
            cfg.source_label,
            r[cfg.qtr_col],
            "matched",
            int(r["left_row_count"]),
        )
        for r in identical_collected
    ]
    qtr_type = get_spark().table(cfg.left_table_name).schema[cfg.qtr_col].dataType
    schema = T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("source_label", T.StringType(), True),
            T.StructField(cfg.qtr_col, qtr_type, True),
            T.StructField("row_status", T.StringType(), False),
            T.StructField("row_count", T.LongType(), False),
        ]
    )
    df = get_spark().createDataFrame(rows, schema=schema)
    write_delta_append(df, final_table(cfg, "row_status_counts"))


def emit_row_status_for_onesided_quarters(
    cfg: ReconcileConfig, quarter_status_df: DataFrame
) -> None:
    """Emit row_status_counts rows for one-sided quarters.

    Quarters present only in the left table get status 'left_only';
    those only in the right table get 'right_only'.

    Args:
        cfg: Active reconciliation configuration.
        quarter_status_df: DataFrame produced by :func:`phase0_quarter_screening`
            containing quarter_status, left_row_count, and right_row_count.
    """
    onesided = quarter_status_df.where(
        F.col("quarter_status").isin("left_only", "right_only")
    ).select(cfg.qtr_col, "quarter_status", "left_row_count", "right_row_count")
    onesided_collected = onesided.collect()
    if not onesided_collected:
        return

    rows = []
    for r in onesided_collected:
        status = r["quarter_status"]
        count = (
            int(r["left_row_count"])
            if status == "left_only"
            else int(r["right_row_count"])
        )
        rows.append((cfg.run_id, cfg.source_label, r[cfg.qtr_col], status, count))

    qtr_type = get_spark().table(cfg.left_table_name).schema[cfg.qtr_col].dataType
    schema = T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("source_label", T.StringType(), True),
            T.StructField(cfg.qtr_col, qtr_type, True),
            T.StructField("row_status", T.StringType(), False),
            T.StructField("row_count", T.LongType(), False),
        ]
    )
    df = get_spark().createDataFrame(rows, schema=schema)
    write_delta_append(df, final_table(cfg, "row_status_counts"))


# ===========================================================================
# Phase 1 — Single-scan row-level + group-level hash extraction
# ===========================================================================


def compute_row_hashes(
    table_name: str,
    cfg: ReconcileConfig,
    quarters: list[Any],
    all_compare_cols: list[str],
    groups: list[list[str]],
    schema_map: dict[str, T.DataType],
    side_label: str,
) -> DataFrame:
    """Read the table once for the given quarters.  Produce a narrow hash table:
    (key_cols..., row_hash_all, gh_0, gh_1, ..., gh_N)
    """
    # Build hash expressions for each group.
    group_hash_exprs: list[F.Column] = []
    all_hash_inputs: list[F.Column] = []

    for gi, grp in enumerate(groups):
        grp_inputs: list[F.Column] = []
        for c in grp:
            grp_inputs.append(F.lit(c))
            grp_inputs.append(normalize_for_hash(colq(c), schema_map[c], cfg))
        group_hash_exprs.append(F.xxhash64(*grp_inputs).alias(f"gh_{gi}"))
        all_hash_inputs.extend(grp_inputs)

    row_hash_all = F.xxhash64(*all_hash_inputs).alias("row_hash_all")

    key_selects = [colq(c).alias(c) for c in cfg.key_cols]

    return (
        get_spark().table(table_name)
        .where(colq(cfg.qtr_col).isin(quarters))
        .select(*key_selects, row_hash_all, *group_hash_exprs)
    )


def phase1_hash_extraction(
    cfg: ReconcileConfig,
    changed_quarters: list[Any],
    all_compare_cols: list[str],
    groups: list[list[str]],
) -> tuple[str, str]:
    """Run Phase 1: single-scan hash extraction for both tables.

    Returns: (left_hash_table_name, right_hash_table_name) — temp Delta tables.
    """
    left_schema = get_schema_map(cfg.left_table_name)
    right_schema = get_schema_map(cfg.right_table_name)
    schema_map = {c: left_schema.get(c, right_schema.get(c)) for c in all_compare_cols}

    print(
        f"Phase 1: Extracting row hashes for {len(changed_quarters)} quarters, {len(groups)} column groups..."
    )

    left_hashes = compute_row_hashes(
        cfg.left_table_name,
        cfg,
        changed_quarters,
        all_compare_cols,
        groups,
        schema_map,
        "left",
    )
    left_hash_table = tmp_table(cfg, "left_hashes")
    overwrite_delta_table(left_hashes, left_hash_table)
    print("Phase 1: Left hashes written.")

    right_hashes = compute_row_hashes(
        cfg.right_table_name,
        cfg,
        changed_quarters,
        all_compare_cols,
        groups,
        schema_map,
        "right",
    )
    right_hash_table = tmp_table(cfg, "right_hashes")
    overwrite_delta_table(right_hashes, right_hash_table)
    print("Phase 1: Right hashes written.")

    return left_hash_table, right_hash_table


# ===========================================================================
# Phase 2 — Narrow join: key reconciliation + row triage
# ===========================================================================


def phase2_key_recon_and_row_triage(
    cfg: ReconcileConfig,
    left_hash_table: str,
    right_hash_table: str,
    num_groups: int,
) -> tuple[str, DataFrame]:
    """Join the two narrow hash tables on key_cols.

    Produces:
        - row_status_counts (matched / left_only / right_only) written to output.
        - optional row_status_detail written to output.
        - changed_keys_table: temp Delta table of keys where row_hash_all differs,
          including per-group hash comparison flags.

    Returns: (changed_keys_table_name, total_matched_per_qtr_df)
    """
    print("Phase 2: Joining hash tables for key reconciliation and row triage...")

    l = get_spark().table(left_hash_table).alias("l")
    r = get_spark().table(right_hash_table).alias("r")

    join_cond = [
        F.col(f"l.{quote_name(c)}") == F.col(f"r.{quote_name(c)}") for c in cfg.key_cols
    ]

    output_keys = [
        F.coalesce(F.col(f"l.{quote_name(c)}"), F.col(f"r.{quote_name(c)}")).alias(c)
        for c in cfg.key_cols
    ]

    left_present = F.lit(True)
    right_present = F.lit(True)
    for c in cfg.key_cols:
        left_present = left_present & F.col(f"l.{quote_name(c)}").isNotNull()
        right_present = right_present & F.col(f"r.{quote_name(c)}").isNotNull()

    # Build select expressions.
    select_exprs = [
        F.lit(cfg.run_id).alias("run_id"),
        F.lit(cfg.source_label).alias("source_label"),
        *output_keys,
        F.when(left_present & right_present, F.lit("matched"))
        .when(left_present & ~right_present, F.lit("left_only"))
        .when(~left_present & right_present, F.lit("right_only"))
        .otherwise(F.lit("unknown"))
        .alias("row_status"),
    ]

    # For matched rows, compare row_hash_all.
    hash_match = F.col("l.row_hash_all").eqNullSafe(F.col("r.row_hash_all"))

    select_exprs.append(
        F.when(left_present & right_present, hash_match)
        .otherwise(F.lit(None).cast("boolean"))
        .alias("row_identical")
    )

    # Per-group hash comparison flags for matched rows.
    for gi in range(num_groups):
        gh_match = F.col(f"l.gh_{gi}").eqNullSafe(F.col(f"r.gh_{gi}"))
        select_exprs.append(
            F.when(left_present & right_present, gh_match)
            .otherwise(F.lit(None).cast("boolean"))
            .alias(f"gh_{gi}_match")
        )

    full_joined = l.join(r, join_cond, "full_outer").select(*select_exprs)
    full_joined = full_joined.cache()

    # --- Row status counts ---
    row_status_counts = full_joined.groupBy(
        "run_id", "source_label", cfg.qtr_col, "row_status"
    ).agg(F.count(F.lit(1)).alias("row_count"))
    write_delta_append(row_status_counts, final_table(cfg, "row_status_counts"))

    if cfg.write_row_status_detail:
        row_detail = full_joined.select(
            "run_id",
            "source_label",
            *[colq(c) for c in cfg.key_cols],
            "row_status",
        )
        write_delta_append(row_detail, final_table(cfg, "row_status_detail"))

    # --- Total matched rows per quarter (for accurate matched_row_count in summaries) ---
    total_matched_per_qtr = (
        full_joined.where(F.col("row_status") == "matched")
        .groupBy(cfg.qtr_col)
        .agg(F.count(F.lit(1)).alias("total_matched_count"))
    )

    # --- Changed keys table: matched rows where row_hash differs ---
    changed_keys_cols = ["run_id"] + list(cfg.key_cols)
    gh_match_cols = [f"gh_{gi}_match" for gi in range(num_groups)]
    changed_keys_cols.extend(gh_match_cols)

    changed_keys = full_joined.where(
        (F.col("row_status") == "matched") & (F.col("row_identical") == False)
    ).select(*[F.col(c) for c in changed_keys_cols])  # noqa: E712

    changed_keys_table = tmp_table(cfg, "changed_keys")
    overwrite_delta_table(changed_keys, changed_keys_table)

    changed_count = get_spark().table(changed_keys_table).count()
    matched_count = full_joined.where(F.col("row_status") == "matched").count()
    print(
        f"Phase 2: {matched_count} matched rows, {changed_count} have differences ({100.0 * changed_count / max(matched_count, 1):.1f}%)."
    )

    safe_unpersist(full_joined)
    return changed_keys_table, total_matched_per_qtr


# ===========================================================================
# Phase 2b — Accurate per-column nonnull counts across all matched rows
# ===========================================================================


def compute_nonnull_counts(
    cfg: ReconcileConfig,
    all_compare_cols: list[str],
    quarter_status_df: DataFrame,
) -> str:
    """Compute per-(quarter, column) nonnull counts across ALL matched rows.

    For 'identical' and 'changed' quarters (both sides have data), inner-joins
    the two source tables on key_cols and counts rows where both sides are
    non-null for each compare column.  One-sided quarters are skipped (no
    matched rows exist).

    Returns: temp table name with schema (quarter, column, nonnull_count).
    """
    # Quarters that have data on both sides.
    both_sided = (
        quarter_status_df.where(F.col("quarter_status").isin("identical", "changed"))
        .select(cfg.qtr_col)
        .collect()
    )
    both_sided_quarters = [r[cfg.qtr_col] for r in both_sided]

    output_table = tmp_table(cfg, "nonnull_counts")
    key_cols = list(cfg.key_cols)

    if not both_sided_quarters:
        # No quarters with data on both sides — write empty table.
        qtr_type = get_spark().table(cfg.left_table_name).schema[cfg.qtr_col].dataType
        schema = T.StructType(
            [
                T.StructField(cfg.qtr_col, qtr_type, True),
                T.StructField("column", T.StringType(), False),
                T.StructField("nonnull_count", T.LongType(), True),
            ]
        )
        empty = get_spark().createDataFrame([], schema)
        overwrite_delta_table(empty, output_table)
        return output_table

    col_batches = chunk_list(all_compare_cols, cfg.comparison_batch_size)
    print(
        f"Phase 2b: Computing nonnull counts for {len(all_compare_cols)} columns across {len(both_sided_quarters)} quarters ({len(col_batches)} batches)..."
    )

    for batch_idx, col_batch in enumerate(col_batches):
        proj = [colq(c).alias(c) for c in key_cols + col_batch]

        l = (
            get_spark().table(cfg.left_table_name)
            .where(colq(cfg.qtr_col).isin(both_sided_quarters))
            .select(*proj)
            .alias("l")
        )
        r = (
            get_spark().table(cfg.right_table_name)
            .where(colq(cfg.qtr_col).isin(both_sided_quarters))
            .select(*proj)
            .alias("r")
        )

        joined = l.join(r, on=key_cols, how="inner")

        # Per-column both-non-null count, grouped by quarter.
        nonnull_aggs = []
        for c in col_batch:
            lcol = F.col(f"l.{quote_name(c)}")
            rcol = F.col(f"r.{quote_name(c)}")
            nonnull_aggs.append(
                F.sum(
                    F.when(lcol.isNotNull() & rcol.isNotNull(), 1).otherwise(0)
                ).alias(f"nn__{c}")
            )

        counts_wide = joined.groupBy(colq(cfg.qtr_col).alias(cfg.qtr_col)).agg(
            *nonnull_aggs
        )

        # Unpivot to long form: (quarter, column, nonnull_count).
        structs = [
            F.struct(
                F.lit(c).alias("column"),
                F.col(f"nn__{c}").cast("long").alias("nonnull_count"),
            )
            for c in col_batch
        ]
        counts_long = counts_wide.select(
            cfg.qtr_col, F.explode(F.array(*structs)).alias("s")
        ).select(
            cfg.qtr_col,
            F.col("s.column").alias("column"),
            F.col("s.nonnull_count").alias("nonnull_count"),
        )

        if batch_idx == 0:
            overwrite_delta_table(counts_long, output_table)
        else:
            write_delta_append(counts_long, output_table)

    print(f"Phase 2b: Nonnull counts written to {output_table}.")
    return output_table


# ===========================================================================
# Phase 3 — Group triage: identify which column groups changed per row
# ===========================================================================


def phase3_group_triage(
    cfg: ReconcileConfig,
    changed_keys_table: str,
    num_groups: int,
) -> dict[int, DataFrame]:
    """For each column group, find the subset of changed keys where that group differs.

    Returns: dict mapping group_index -> DataFrame of keys where that group changed.
             Each DataFrame contains only key_cols.
    """
    print(f"Phase 3: Triaging {num_groups} column groups across changed rows...")

    changed_keys = get_spark().table(changed_keys_table)
    changed_keys = changed_keys.cache()

    group_changed_keys: dict[int, DataFrame] = {}
    key_col_selects = [colq(c).alias(c) for c in cfg.key_cols]

    for gi in range(num_groups):
        match_col = f"gh_{gi}_match"
        keys_for_group = changed_keys.where(F.col(match_col) == False).select(  # noqa: E712
            *key_col_selects
        )
        group_changed_keys[gi] = keys_for_group

    # Report group-level change counts.
    group_counts: list[tuple[int, int]] = []
    for gi in range(num_groups):
        cnt = group_changed_keys[gi].count()
        group_counts.append((gi, cnt))

    total_changed = get_spark().table(changed_keys_table).count()
    nonzero_groups = sum(1 for _, cnt in group_counts if cnt > 0)
    print(
        f"Phase 3: {nonzero_groups}/{num_groups} groups have changes. Total changed rows: {total_changed}."
    )

    for gi, cnt in group_counts:
        if cnt > 0:
            print(f"  Group {gi}: {cnt} rows changed.")

    safe_unpersist(changed_keys)
    return group_changed_keys


# ===========================================================================
# Phase 4 — Targeted column comparison on changed rows × changed groups
# ===========================================================================


def compare_columns_for_keys(
    cfg: ReconcileConfig,
    changed_keys_df: DataFrame,
    cols_to_compare: list[str],
    changed_quarters: list[Any],
) -> tuple[DataFrame, DataFrame]:
    """Compare specific columns for a specific set of changed keys.

    Returns: (summary_df, mismatch_long_df)
        summary_df: per-column, per-quarter mismatch stats (for changed rows only).
        mismatch_long_df: long-form mismatch rows (column, left_val, right_val, ...).
                          None if detail_mode == "summary".
    """
    meta = build_feature_meta(cfg, cols_to_compare)
    key_cols = list(cfg.key_cols)

    left_proj = [colq(c).alias(c) for c in key_cols + cols_to_compare]
    right_proj = [colq(c).alias(c) for c in key_cols + cols_to_compare]

    l = (
        get_spark().table(cfg.left_table_name)
        .where(colq(cfg.qtr_col).isin(changed_quarters))
        .select(*left_proj)
        .join(changed_keys_df.hint("broadcast"), on=key_cols, how="inner")
        .alias("l")
    )

    r = (
        get_spark().table(cfg.right_table_name)
        .where(colq(cfg.qtr_col).isin(changed_quarters))
        .select(*right_proj)
        .join(changed_keys_df.hint("broadcast"), on=key_cols, how="inner")
        .alias("r")
    )

    joined = l.join(r, on=key_cols, how="inner")

    # --- Build per-column comparison expressions ---
    summary_aggs = [F.count(F.lit(1)).alias("__changed_row_count")]
    summary_structs: list[F.Column] = []

    # For mismatch detail we build one per-row struct per column (null when no
    # mismatch). We then explode and drop nulls using only standard Column API
    # (no higher-order SQL lambdas) to stay shared-cluster compatible.
    mismatch_structs: list[F.Column] = []

    for col_name in cols_to_compare:
        lcol = F.col(f"l.{quote_name(col_name)}")
        rcol = F.col(f"r.{quote_name(col_name)}")

        both_null = lcol.isNull() & rcol.isNull()
        one_null = (lcol.isNull() & rcol.isNotNull()) | (
            lcol.isNotNull() & rcol.isNull()
        )

        if meta[col_name]["is_numeric"]:
            tol = float(meta[col_name]["tolerance"])
            left_d = lcol.cast("double")
            right_d = rcol.cast("double")
            signed_diff = right_d - left_d
            abs_diff = F.abs(signed_diff)

            pct_diff = F.when(
                left_d.isNotNull() & right_d.isNotNull() & (left_d != 0),
                signed_diff / left_d,
            ).otherwise(F.lit(None).cast("double"))

            pct_diff_pct = F.when(
                left_d.isNotNull() & right_d.isNotNull() & (left_d != 0),
                pct_diff * F.lit(100.0),
            ).otherwise(F.lit(None).cast("double"))

            value_mismatch = (~both_null) & (~one_null) & (abs_diff > F.lit(tol))
            mismatch_cond = one_null | value_mismatch

            diff_value = F.when(
                (~both_null) & (~one_null), F.round(signed_diff, 4)
            ).otherwise(F.lit(None).cast("double"))
            abs_diff_value = F.when(
                (~both_null) & (~one_null), F.round(abs_diff, 4)
            ).otherwise(F.lit(None).cast("double"))
            pct_diff_value = F.when(
                (~both_null) & (~one_null), F.round(pct_diff, 4)
            ).otherwise(F.lit(None).cast("double"))
            pct_diff_pct_value = F.when(
                (~both_null) & (~one_null), F.round(pct_diff_pct, 4)
            ).otherwise(F.lit(None).cast("double"))
            max_abs_diff_expr = F.max(abs_diff_value)
        else:
            mismatch_cond = ~lcol.eqNullSafe(rcol)
            diff_value = F.lit(None).cast("double")
            abs_diff_value = F.lit(None).cast("double")
            pct_diff_value = F.lit(None).cast("double")
            pct_diff_pct_value = F.lit(None).cast("double")
            max_abs_diff_expr = F.max(F.lit(None).cast("double"))

        null_mm_alias = f"{col_name}__null_mismatch_count"
        mm_alias = f"{col_name}__mismatch_count"
        nonnull_alias = f"{col_name}__nonnull_compared_count"
        max_abs_alias = f"{col_name}__max_abs_diff"

        summary_aggs.extend(
            [
                F.sum(F.when(one_null, 1).otherwise(0)).alias(null_mm_alias),
                F.sum(F.when(mismatch_cond, 1).otherwise(0)).alias(mm_alias),
                F.sum(F.when((~one_null) & (~both_null), 1).otherwise(0)).alias(
                    nonnull_alias
                ),
                max_abs_diff_expr.alias(max_abs_alias),
            ]
        )

        summary_structs.append(
            F.struct(
                F.lit(col_name).alias("column"),
                F.lit(str(meta[col_name]["left_type"])).alias("left_type"),
                F.lit(str(meta[col_name]["right_type"])).alias("right_type"),
                F.lit(bool(meta[col_name]["is_numeric"])).alias("is_numeric"),
                F.lit(float(meta[col_name]["tolerance"])).alias("tolerance"),
                F.col("__changed_row_count").alias("changed_row_count"),
                F.col(nonnull_alias).alias("nonnull_compared_count"),
                F.col(mm_alias).alias("mismatch_count"),
                F.col(null_mm_alias).alias("null_mismatch_count"),
                F.col(max_abs_alias).alias("max_abs_diff"),
            )
        )

        if cfg.detail_mode in {"sample", "full_direct"}:
            mismatch_structs.append(
                F.when(
                    mismatch_cond,
                    F.struct(
                        F.lit(col_name).alias("column"),
                        lcol.cast("string").alias(cfg.left_label),
                        rcol.cast("string").alias(cfg.right_label),
                        F.when(one_null, F.lit(True))
                        .otherwise(F.lit(False))
                        .alias("null_mismatch"),
                        diff_value.alias("diff"),
                        abs_diff_value.alias("abs_diff"),
                        pct_diff_value.alias("pct_diff"),
                        pct_diff_pct_value.alias("pct_diff_pct"),
                        F.lit(float(meta[col_name]["tolerance"]))
                        .cast("double")
                        .alias("tolerance_used"),
                    ),
                )
            )

    # --- Summary aggregation ---
    summary_wide = joined.groupBy(colq(cfg.qtr_col).alias(cfg.qtr_col)).agg(
        *summary_aggs
    )

    summary_df = summary_wide.select(
        F.lit(cfg.run_id).alias("run_id"),
        F.lit(cfg.source_label).alias("source_label"),
        colq(cfg.qtr_col).alias(cfg.qtr_col),
        F.explode(F.array(*summary_structs)).alias("s"),
    ).select(
        "run_id",
        "source_label",
        cfg.qtr_col,
        F.col("s.column").alias("column"),
        F.col("s.left_type").alias("left_type"),
        F.col("s.right_type").alias("right_type"),
        F.col("s.is_numeric").alias("is_numeric"),
        F.col("s.tolerance").alias("tolerance"),
        F.col("s.changed_row_count").alias("changed_row_count"),
        F.col("s.nonnull_compared_count").alias("nonnull_compared_count"),
        F.col("s.mismatch_count").alias("mismatch_count"),
        F.col("s.null_mismatch_count").alias("null_mismatch_count"),
        F.col("s.max_abs_diff").alias("max_abs_diff"),
    )

    # --- Mismatch detail/sample ---
    mismatch_long_df = None
    if cfg.detail_mode in {"sample", "full_direct"} and mismatch_structs:
        output_key_exprs = [colq(c).alias(c) for c in cfg.key_cols]
        mismatch_long_df = (
            joined.select(
                F.lit(cfg.run_id).alias("run_id"),
                F.lit(cfg.source_label).alias("source_label"),
                *output_key_exprs,
                F.explode(F.array(*mismatch_structs)).alias("m"),
            )
            .where(F.col("m").isNotNull())
            .select(
                "run_id",
                "source_label",
                *cfg.key_cols,
                F.col("m.column").alias("column"),
                F.col(f"m.`{cfg.left_label}`").alias(cfg.left_label),
                F.col(f"m.`{cfg.right_label}`").alias(cfg.right_label),
                F.col("m.null_mismatch").alias("null_mismatch"),
                F.col("m.diff").alias("diff"),
                F.col("m.abs_diff").alias("abs_diff"),
                F.col("m.pct_diff").alias("pct_diff"),
                F.col("m.pct_diff_pct").alias("pct_diff_pct"),
                F.col("m.tolerance_used").alias("tolerance_used"),
            )
        )

    return summary_df, mismatch_long_df


def _enrich_and_write_summary(
    cfg: ReconcileConfig,
    summary_df: DataFrame,
    total_matched_per_qtr: DataFrame,
    nonnull_counts_table: str,
) -> None:
    """Enrich and persist a column-comparison summary batch.

    Joins the raw summary from :func:`compare_columns_for_keys` with:
    - total matched row counts per quarter (denominator for pct metrics).
    - precomputed nonnull counts (accurate nonnull_compared_count).

    Computes mismatch_pct and null_mismatch_pct, then appends to the
    ``column_summary_by_quarter`` output table.

    Args:
        cfg: Active reconciliation configuration.
        summary_df: Raw per-column summary for one batch of columns.
        total_matched_per_qtr: DataFrame with (qtr_col, total_matched_count).
        nonnull_counts_table: Temp table name with (qtr_col, column, nonnull_count).
    """
    nn = get_spark().table(nonnull_counts_table).alias("nn")

    enriched = (
        summary_df.alias("s")
        .join(total_matched_per_qtr.alias("t"), on=cfg.qtr_col, how="left")
        .join(
            nn,
            on=(F.col(f"s.{cfg.qtr_col}") == F.col(f"nn.{cfg.qtr_col}"))
            & (F.col("s.column") == F.col("nn.column")),
            how="left",
        )
        .select(
            F.col("s.run_id"),
            F.col("s.source_label"),
            F.col(f"s.{cfg.qtr_col}"),
            F.col("s.column"),
            F.col("s.left_type"),
            F.col("s.right_type"),
            F.col("s.is_numeric"),
            F.col("s.tolerance"),
            F.col("s.changed_row_count"),
            F.coalesce(
                F.col("nn.nonnull_count"), F.col("s.nonnull_compared_count")
            ).alias("nonnull_compared_count"),
            F.col("s.mismatch_count"),
            F.col("s.null_mismatch_count"),
            F.col("s.max_abs_diff"),
            F.coalesce(
                F.col("t.total_matched_count"), F.col("s.changed_row_count")
            ).alias("matched_row_count"),
        )
    )

    enriched = enriched.withColumn(
        "mismatch_pct",
        F.when(
            F.col("matched_row_count") > 0,
            F.col("mismatch_count") / F.col("matched_row_count"),
        ).otherwise(F.lit(None).cast("double")),
    ).withColumn(
        "null_mismatch_pct",
        F.when(
            F.col("matched_row_count") > 0,
            F.col("null_mismatch_count") / F.col("matched_row_count"),
        ).otherwise(F.lit(None).cast("double")),
    )

    write_delta_append(enriched, final_table(cfg, "column_summary_by_quarter"))


def _sample_and_write_mismatch(
    cfg: ReconcileConfig,
    mismatch_long_df: DataFrame,
) -> None:
    """Sample and persist mismatch detail rows for one column batch.

    Applies a row-number window to cap samples at ``cfg.sample_per_column``
    per (quarter, column) and writes to ``mismatch_sample``.  If detail_mode
    is 'full_direct', also writes the unsampled rows to ``mismatch_detail``.

    Args:
        cfg: Active reconciliation configuration.
        mismatch_long_df: Long-form mismatch DataFrame from
            :func:`compare_columns_for_keys`.
    """
    sample_window = Window.partitionBy(cfg.qtr_col, "column").orderBy(
        *[colq(c) for c in cfg.key_cols]
    )
    mismatch_sample = (
        mismatch_long_df.withColumn(
            "sample_row_num", F.row_number().over(sample_window)
        )
        .where(F.col("sample_row_num") <= F.lit(cfg.sample_per_column))
        .drop("sample_row_num")
    )
    write_delta_append(mismatch_sample, final_table(cfg, "mismatch_sample"))

    if cfg.detail_mode == "full_direct":
        write_delta_append(mismatch_long_df, final_table(cfg, "mismatch_detail"))


def phase4_targeted_comparison(
    cfg: ReconcileConfig,
    changed_quarters: list[Any],
    groups: list[list[str]],
    group_changed_keys: dict[int, DataFrame],
    all_compare_cols: list[str],
    total_matched_per_qtr: DataFrame,
    nonnull_counts_table: str,
) -> None:
    """Phase 4: For each column group that has changes, compare actual values
    only for the changed rows in that group.  Produces column_summary_by_quarter
    and mismatch_sample / mismatch_detail outputs.

    Each batch is enriched, sampled, and written directly — no cross-batch
    unionByName is needed (avoids _jdf access on shared clusters).
    """
    print("Phase 4: Targeted column comparison on changed rows...")

    batch_count = 0

    for gi, grp_cols in enumerate(groups):
        changed_keys_df = group_changed_keys[gi]

        # Check if this group has any changes (cheaply, from Phase 3 counts).
        grp_count = changed_keys_df.limit(1).count()
        if grp_count == 0:
            print(f"  Group {gi}: no changes, emitting zero-fill summary.")
            # Zero-fill summary for unchanged groups will be handled in Phase 5.
            continue

        # Batch columns within the group for Catalyst plan size management.
        col_batches = chunk_list(grp_cols, cfg.comparison_batch_size)

        for cb_idx, col_batch in enumerate(col_batches):
            print(
                f"  Group {gi}, column batch {cb_idx + 1}/{len(col_batches)}: {len(col_batch)} columns, comparing..."
            )

            summary_df, mismatch_long_df = compare_columns_for_keys(
                cfg, changed_keys_df, col_batch, changed_quarters
            )

            # Enrich with total matched row counts and accurate nonnull counts.
            _enrich_and_write_summary(
                cfg, summary_df, total_matched_per_qtr, nonnull_counts_table
            )

            # Sample and write mismatch detail immediately.
            if mismatch_long_df is not None:
                _sample_and_write_mismatch(cfg, mismatch_long_df)

            batch_count += 1

    print(f"Phase 4: column_summary_by_quarter written ({batch_count} batches).")
    if cfg.detail_mode in {"sample", "full_direct"}:
        print("Phase 4: mismatch_sample written.")
    print("Phase 4: Complete.")


# ===========================================================================
# Phase 5 — Rollups, zero-fill for identical data, noisy-column detection
# ===========================================================================


def emit_zero_fill_for_identical_quarters(
    cfg: ReconcileConfig,
    quarter_status_df: DataFrame,
    all_compare_cols: list[str],
    nonnull_counts_table: str,
) -> None:
    """For quarters that were identical in Phase 0, emit column_summary rows
    with zero mismatches.  matched_row_count comes from the quarter checksum
    row counts.  nonnull_compared_count comes from the precomputed nonnull
    counts table.
    """
    identical = quarter_status_df.where(F.col("quarter_status") == "identical").select(
        cfg.qtr_col, F.col("left_row_count").alias("matched_row_count")
    )

    identical_collected = identical.collect()
    if not identical_collected:
        return

    # Build lookup: (quarter, column) -> nonnull_count
    identical_qtrs = [r[cfg.qtr_col] for r in identical_collected]
    nn_rows = (
        get_spark().table(nonnull_counts_table)
        .where(colq(cfg.qtr_col).isin(identical_qtrs))
        .collect()
    )
    nn_lookup: dict[tuple, int] = {
        (r[cfg.qtr_col], r["column"]): int(r["nonnull_count"]) for r in nn_rows
    }

    meta = build_feature_meta(cfg, all_compare_cols)

    rows = []
    for qtr_row in identical_collected:
        qtr_val = qtr_row[cfg.qtr_col]
        matched = int(qtr_row["matched_row_count"])
        for c in all_compare_cols:
            nonnull = nn_lookup.get((qtr_val, c), matched)
            rows.append(
                (
                    cfg.run_id,
                    cfg.source_label,
                    qtr_val,
                    c,
                    str(meta[c]["left_type"]),
                    str(meta[c]["right_type"]),
                    bool(meta[c]["is_numeric"]),
                    float(meta[c]["tolerance"]),
                    0,  # changed_row_count
                    nonnull,  # nonnull_compared_count
                    0,  # mismatch_count
                    0,  # null_mismatch_count
                    None,  # max_abs_diff
                    matched,  # matched_row_count
                    0.0,  # mismatch_pct
                    0.0,  # null_mismatch_pct
                )
            )

    if not rows:
        return

    qtr_type = get_spark().table(cfg.left_table_name).schema[cfg.qtr_col].dataType

    schema = T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("source_label", T.StringType(), True),
            T.StructField(cfg.qtr_col, qtr_type, True),
            T.StructField("column", T.StringType(), False),
            T.StructField("left_type", T.StringType(), True),
            T.StructField("right_type", T.StringType(), True),
            T.StructField("is_numeric", T.BooleanType(), True),
            T.StructField("tolerance", T.DoubleType(), True),
            T.StructField("changed_row_count", T.LongType(), True),
            T.StructField("nonnull_compared_count", T.LongType(), True),
            T.StructField("mismatch_count", T.LongType(), True),
            T.StructField("null_mismatch_count", T.LongType(), True),
            T.StructField("max_abs_diff", T.DoubleType(), True),
            T.StructField("matched_row_count", T.LongType(), True),
            T.StructField("mismatch_pct", T.DoubleType(), True),
            T.StructField("null_mismatch_pct", T.DoubleType(), True),
        ]
    )

    # Write in chunks to avoid driver OOM for very large column × quarter combos.
    chunk_size = 500_000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        df = get_spark().createDataFrame(chunk, schema=schema)
        write_delta_append(df, final_table(cfg, "column_summary_by_quarter"))

    print(
        f"Phase 5: Zero-fill summary written for {len(identical_collected)} identical quarters × {len(all_compare_cols)} columns."
    )


def emit_zero_fill_for_unchanged_groups(
    cfg: ReconcileConfig,
    changed_quarters: list[Any],
    groups: list[list[str]],
    group_changed_keys: dict[int, DataFrame],
    total_matched_per_qtr: DataFrame,
    nonnull_counts_table: str,
) -> None:
    """For each column group, emit zero-mismatch summary rows for any changed
    quarter where that group has NO changed keys.  This covers two cases:
      1. Groups with zero changes globally (all changed quarters need zero-fill).
      2. Groups with changes in some quarters but not others (the gap quarters
         need zero-fill so column_summary_all_quarters sums correctly).
    """
    total_collected = total_matched_per_qtr.collect()
    if not total_collected:
        return

    # Build lookup: quarter_value -> total_matched_count
    qtr_matched_map = {
        row[cfg.qtr_col]: int(row["total_matched_count"]) for row in total_collected
    }

    # Build lookup: (quarter, column) -> nonnull_count
    nn_rows = (
        get_spark().table(nonnull_counts_table)
        .where(colq(cfg.qtr_col).isin(changed_quarters))
        .collect()
    )
    nn_lookup: dict[tuple, int] = {
        (r[cfg.qtr_col], r["column"]): int(r["nonnull_count"]) for r in nn_rows
    }

    meta = build_feature_meta(cfg, [c for grp in groups for c in grp])

    rows = []
    for gi, grp_cols in enumerate(groups):
        # Determine which changed quarters already have Phase 4 data for this group.
        grp_count = group_changed_keys[gi].limit(1).count()
        if grp_count > 0:
            qtrs_with_changes = set(
                row[cfg.qtr_col]
                for row in group_changed_keys[gi]
                .select(cfg.qtr_col)
                .distinct()
                .collect()
            )
        else:
            qtrs_with_changes = set()

        # Emit zero-fill for every changed quarter where this group has no changes
        # (Phase 4 did not write a summary row for these quarter×column pairs).
        for qtr_val, matched in qtr_matched_map.items():
            if qtr_val in qtrs_with_changes:
                continue
            for c in grp_cols:
                nonnull = nn_lookup.get((qtr_val, c), matched)
                rows.append(
                    (
                        cfg.run_id,
                        cfg.source_label,
                        qtr_val,
                        c,
                        str(meta[c]["left_type"]),
                        str(meta[c]["right_type"]),
                        bool(meta[c]["is_numeric"]),
                        float(meta[c]["tolerance"]),
                        0,  # changed_row_count
                        nonnull,  # nonnull_compared_count
                        0,  # mismatch_count
                        0,  # null_mismatch_count
                        None,  # max_abs_diff
                        matched,  # matched_row_count
                        0.0,  # mismatch_pct
                        0.0,  # null_mismatch_pct
                    )
                )

    if not rows:
        return

    qtr_type = get_spark().table(cfg.left_table_name).schema[cfg.qtr_col].dataType

    schema = T.StructType(
        [
            T.StructField("run_id", T.StringType(), False),
            T.StructField("source_label", T.StringType(), True),
            T.StructField(cfg.qtr_col, qtr_type, True),
            T.StructField("column", T.StringType(), False),
            T.StructField("left_type", T.StringType(), True),
            T.StructField("right_type", T.StringType(), True),
            T.StructField("is_numeric", T.BooleanType(), True),
            T.StructField("tolerance", T.DoubleType(), True),
            T.StructField("changed_row_count", T.LongType(), True),
            T.StructField("nonnull_compared_count", T.LongType(), True),
            T.StructField("mismatch_count", T.LongType(), True),
            T.StructField("null_mismatch_count", T.LongType(), True),
            T.StructField("max_abs_diff", T.DoubleType(), True),
            T.StructField("matched_row_count", T.LongType(), True),
            T.StructField("mismatch_pct", T.DoubleType(), True),
            T.StructField("null_mismatch_pct", T.DoubleType(), True),
        ]
    )

    chunk_size = 500_000
    for i in range(0, len(rows), chunk_size):
        chunk = rows[i : i + chunk_size]
        df = get_spark().createDataFrame(chunk, schema=schema)
        write_delta_append(df, final_table(cfg, "column_summary_by_quarter"))

    print(
        f"Phase 5: Zero-fill summary written for {len(rows)} unchanged group×quarter×column entries."
    )


def build_rollups(cfg: ReconcileConfig) -> None:
    """Build cross-quarter aggregate outputs.

    Produces two output tables from the current run's
    ``column_summary_by_quarter`` data:

    - **column_summary_all_quarters**: Per-column totals across all quarters.
    - **noisy_columns**: Columns whose mismatch_pct exceeds the configured
      threshold (``cfg.noisy_column_threshold``).

    Args:
        cfg: Active reconciliation configuration.
    """
    print("Phase 5: Building rollups...")

    summary = get_spark().table(final_table(cfg, "column_summary_by_quarter")).where(
        F.col("run_id") == F.lit(cfg.run_id)
    )

    # --- column_summary_all_quarters ---
    column_summary_all = (
        summary.groupBy(
            "run_id",
            "source_label",
            "column",
            "left_type",
            "right_type",
            "is_numeric",
            "tolerance",
        )
        .agg(
            F.sum("matched_row_count").alias("matched_row_count"),
            F.sum("nonnull_compared_count").alias("nonnull_compared_count"),
            F.sum("mismatch_count").alias("mismatch_count"),
            F.sum("null_mismatch_count").alias("null_mismatch_count"),
            F.max("max_abs_diff").alias("max_abs_diff"),
        )
        .withColumn(
            "mismatch_pct",
            F.when(
                F.col("matched_row_count") > 0,
                F.col("mismatch_count") / F.col("matched_row_count"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .withColumn(
            "null_mismatch_pct",
            F.when(
                F.col("matched_row_count") > 0,
                F.col("null_mismatch_count") / F.col("matched_row_count"),
            ).otherwise(F.lit(None).cast("double")),
        )
        .select(
            "run_id",
            "source_label",
            "column",
            "left_type",
            "right_type",
            "is_numeric",
            "tolerance",
            "matched_row_count",
            "nonnull_compared_count",
            "mismatch_count",
            "mismatch_pct",
            "null_mismatch_count",
            "null_mismatch_pct",
            "max_abs_diff",
        )
    )
    write_delta_append(
        column_summary_all, final_table(cfg, "column_summary_all_quarters")
    )

    # --- Noisy column detection ---
    noisy_columns = (
        column_summary_all.where(
            (F.col("matched_row_count") > 0)
            & (F.col("mismatch_pct") >= F.lit(cfg.noisy_column_threshold))
        )
        .select(
            "run_id",
            "source_label",
            "column",
            "matched_row_count",
            "mismatch_count",
            "mismatch_pct",
        )
        .withColumn("suspected_reason", F.lit("change_rate_above_threshold"))
    )

    noisy_count = noisy_columns.count()
    if noisy_count > 0:
        write_delta_append(noisy_columns, final_table(cfg, "noisy_columns"))
        print(
            f"Phase 5: {noisy_count} columns flagged as suspected systematic/noisy (>= {cfg.noisy_column_threshold:.0%} change rate)."
        )
    else:
        print("Phase 5: No noisy columns detected.")

    print("Phase 5: Rollups complete.")
