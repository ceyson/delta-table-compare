from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from .config import ReconcileConfig
from .helpers import get_spark, write_delta_append, final_table


def columns_comp(
    left_df: DataFrame,
    right_df: DataFrame,
    left_name: str,
    right_name: str,
    cfg: Optional[ReconcileConfig] = None,
    normalize_case: bool = True,
    strip_whitespace: bool = True,
) -> dict:
    """
    Print comparative descriptives for two input DataFrames reporting:
        1. Row and column counts
        2. Column names unique to either input DataFrame
        3. Count of shared set of column names between both input DataFames

    Additionally writes a `recon_descriptives` Delta table artifact when
    cfg is provided, containing run_id and source_label for traceability.

    Args:
        left_df (DataFrame): Left DataFrame to compare.
        right_df (DataFrame): Right DataFrame to compare.
        left_name (str): Alias for name of left DataFrame.
        right_name (str): Alias for name of right DataFrame.
        cfg (ReconcileConfig, optional): If provided, persists results to Delta.
        normalize_case (bool, optional): Optionally lowercase column names. Defaults to True.
        strip_whitespace (bool, optional): Optionally remove leading and trailing blanks from column names. Defaults to True.

    Returns:
        Dict with descriptive metrics for programmatic access.
    """

    def normalize_col(col: str) -> str:
        if strip_whitespace:
            col = col.strip()
        if normalize_case:
            col = col.lower()
        return col

    left_col_map = {normalize_col(c): c for c in left_df.columns}
    right_col_map = {normalize_col(c): c for c in right_df.columns}

    left_cols = set(left_col_map.keys())
    right_cols = set(right_col_map.keys())

    left_only_keys = sorted(left_cols - right_cols)
    right_only_keys = sorted(right_cols - left_cols)
    shared_keys = left_cols & right_cols

    left_only = [left_col_map[c] for c in left_only_keys]
    right_only = [right_col_map[c] for c in right_only_keys]

    left_total = len(left_df.columns)
    left_rows = left_df.count()
    right_total = len(right_df.columns)
    right_rows = right_df.count()
    shared_count = len(shared_keys)

    row_delta = left_rows - right_rows
    col_delta = left_total - right_total

    # --- Print to notebook (retained) ---
    print("=" * 80)
    print("COLUMN PRESENCES / ROW COUNT REPORT")
    print("=" * 80)

    print("\nDeltas")
    print("-" * len("Deltas"))
    print(f"Row delta: {abs(row_delta):,}")
    print(f"Column delta: {abs(col_delta):,}")

    print(f"\n{left_name}")
    print("-" * len(left_name))
    print(f"Total rows: {left_rows:,}")
    print(f"Total columns: {left_total}")
    print(f"N columns only in {left_name}: {len(left_only)}")
    print(f"Unique column names only in {left_name}:")
    print(left_only)

    print(f"\n{right_name}")
    print("-" * len(right_name))
    print(f"Total rows: {right_rows:,}")
    print(f"Total columns: {right_total}")
    print(f"N columns only in {right_name}: {len(right_only)}")
    print(f"Unique column names only in {right_name}:")
    print(right_only)

    print("\nShared Columns")
    print("-" * 14)
    print(f"Shared column count: {shared_count}")

    print("\nMath Check")
    print("-" * 10)
    print(
        f"{left_name}: {shared_count} shared + "
        f"len{left_only} unique = {shared_count + len(left_only)} columns"
    )
    print(
        f"{right_name}: {shared_count} shared + "
        f"len{right_only} unique = {shared_count + len(right_only)} columns"
    )

    # --- Build result dict ---
    result = {
        "left_name": left_name,
        "right_name": right_name,
        "left_row_count": left_rows,
        "right_row_count": right_rows,
        "left_column_count": left_total,
        "right_column_count": right_total,
        "shared_column_count": shared_count,
        "left_only_column_count": len(left_only),
        "right_only_column_count": len(right_only),
        "row_delta": abs(row_delta),
        "column_delta": abs(col_delta),
        "left_only_columns": left_only,
        "right_only_columns": right_only,
    }

    # --- Write Delta artifact if cfg provided ---
    if cfg is not None:
        _write_descriptives_artifact(cfg, result)

    return result


def _write_descriptives_artifact(cfg: ReconcileConfig, metrics: dict) -> None:
    """Persist descriptives as a recon_descriptives Delta table.

    Schema includes run_id and source_label for consistency with
    all other recon_ prefixed artifacts.
    """
    spark = get_spark()

    schema = T.StructType([
        T.StructField("run_id", T.StringType(), False),
        T.StructField("source_label", T.StringType(), True),
        T.StructField("left_name", T.StringType(), True),
        T.StructField("right_name", T.StringType(), True),
        T.StructField("left_row_count", T.LongType(), True),
        T.StructField("right_row_count", T.LongType(), True),
        T.StructField("left_column_count", T.IntegerType(), True),
        T.StructField("right_column_count", T.IntegerType(), True),
        T.StructField("shared_column_count", T.IntegerType(), True),
        T.StructField("left_only_column_count", T.IntegerType(), True),
        T.StructField("right_only_column_count", T.IntegerType(), True),
        T.StructField("row_delta", T.LongType(), True),
        T.StructField("column_delta", T.IntegerType(), True),
        T.StructField("left_only_columns", T.ArrayType(T.StringType()), True),
        T.StructField("right_only_columns", T.ArrayType(T.StringType()), True),
    ])

    row = (
        cfg.run_id,
        cfg.source_label,
        metrics["left_name"],
        metrics["right_name"],
        int(metrics["left_row_count"]),
        int(metrics["right_row_count"]),
        metrics["left_column_count"],
        metrics["right_column_count"],
        metrics["shared_column_count"],
        metrics["left_only_column_count"],
        metrics["right_only_column_count"],
        int(metrics["row_delta"]),
        metrics["column_delta"],
        metrics["left_only_columns"],
        metrics["right_only_columns"],
    )

    df = spark.createDataFrame([row], schema=schema)
    write_delta_append(df, final_table(cfg, "descriptives"))
