"""
Synthetic test data generator for the recon framework.

Generates two Delta tables (left/right) with configurable dimensions:
- Number of quarters
- Rows per quarter (grows by rows_per_quarter_increment each quarter)
- Column types: numeric (double), string, date, boolean
- Reproducible via seed

The "right" table starts as an exact copy of the left. Use inject_differences()
to introduce controlled mutations for testing.
"""

from __future__ import annotations

import random
import string
from datetime import date, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T


def _quarter_dates(n_quarters: int, start_year: int = 2020) -> list[date]:
    """Generate a list of quarter-start dates."""
    quarters = []
    for i in range(n_quarters):
        year = start_year + i // 4
        month = (i % 4) * 3 + 1
        quarters.append(date(year, month, 1))
    return quarters


def _build_schema(
    n_numeric_cols: int,
    n_string_cols: int,
    n_date_cols: int,
    n_bool_cols: int,
) -> T.StructType:
    """Build a StructType for the test table."""
    fields = [
        T.StructField("policy_id", T.IntegerType(), False),
        T.StructField("quarter_date", T.DateType(), False),
    ]
    for i in range(n_numeric_cols):
        fields.append(T.StructField(f"num_col_{i:03d}", T.DoubleType(), True))
    for i in range(n_string_cols):
        fields.append(T.StructField(f"str_col_{i:03d}", T.StringType(), True))
    for i in range(n_date_cols):
        fields.append(T.StructField(f"date_col_{i:03d}", T.DateType(), True))
    for i in range(n_bool_cols):
        fields.append(T.StructField(f"bool_col_{i:03d}", T.BooleanType(), True))
    return T.StructType(fields)


def generate_test_data(
    spark: SparkSession,
    output_path: str,
    n_quarters: int = 5,
    base_rows_per_quarter: int = 5000,
    rows_per_quarter_increment: int = 5000,
    n_numeric_cols: int = 160,
    n_string_cols: int = 20,
    n_date_cols: int = 10,
    n_bool_cols: int = 10,
    seed: int = 42,
    start_year: int = 2020,
) -> tuple[str, str, list[str]]:
    """Generate left and right Delta tables with identical data.

    Args:
        spark: Active SparkSession.
        output_path: Base directory for Delta tables.
        n_quarters: Number of quarters to generate.
        base_rows_per_quarter: Rows in the first quarter.
        rows_per_quarter_increment: Additional rows per subsequent quarter.
        n_numeric_cols: Number of numeric (double) columns.
        n_string_cols: Number of string columns.
        n_date_cols: Number of date columns.
        n_bool_cols: Number of boolean columns.
        seed: Random seed for reproducibility.
        start_year: Starting year for quarter dates.

    Returns:
        (left_table_path, right_table_path, critical_cols) where critical_cols
        is a list of the first min(200, n_numeric_cols) numeric column names.
    """
    rng = random.Random(seed)
    quarters = _quarter_dates(n_quarters, start_year)

    schema = _build_schema(n_numeric_cols, n_string_cols, n_date_cols, n_bool_cols)
    total_cols = n_numeric_cols + n_string_cols + n_date_cols + n_bool_cols

    all_rows = []
    policy_counter = 1

    for qi, qtr in enumerate(quarters):
        n_rows = base_rows_per_quarter + qi * rows_per_quarter_increment
        for _ in range(n_rows):
            row = [policy_counter, qtr]
            # Numeric columns
            for _ in range(n_numeric_cols):
                if rng.random() < 0.05:  # 5% null rate
                    row.append(None)
                else:
                    row.append(round(rng.gauss(100.0, 50.0), 4))
            # String columns
            for _ in range(n_string_cols):
                if rng.random() < 0.05:
                    row.append(None)
                else:
                    row.append("".join(rng.choices(string.ascii_uppercase, k=8)))
            # Date columns
            for _ in range(n_date_cols):
                if rng.random() < 0.05:
                    row.append(None)
                else:
                    offset = rng.randint(-365, 365)
                    row.append(qtr + timedelta(days=offset))
            # Boolean columns
            for _ in range(n_bool_cols):
                if rng.random() < 0.05:
                    row.append(None)
                else:
                    row.append(rng.choice([True, False]))

            all_rows.append(tuple(row))
            policy_counter += 1

    df = spark.createDataFrame(all_rows, schema=schema)

    left_path = f"{output_path}/left_table"
    right_path = f"{output_path}/right_table"

    df.write.format("delta").mode("overwrite").save(left_path)
    df.write.format("delta").mode("overwrite").save(right_path)

    # Critical cols: first N numeric columns
    n_critical = min(200, n_numeric_cols)
    critical_cols = [f"num_col_{i:03d}" for i in range(n_critical)]

    return left_path, right_path, critical_cols


def inject_differences(
    spark: SparkSession,
    source_path: str,
    output_path: str,
    change_rate: float = 0.05,
    change_cols: Optional[list[str]] = None,
    quarters_affected: Optional[list[date]] = None,
    add_rows: int = 0,
    remove_rows: int = 0,
    null_inject_rate: float = 0.0,
    seed: int = 123,
) -> str:
    """Create a modified version of a Delta table with injected differences.

    Args:
        spark: Active SparkSession.
        source_path: Path to the source Delta table.
        output_path: Path to write the modified Delta table.
        change_rate: Fraction of rows to modify (0.0 to 1.0).
        change_cols: Columns to modify. If None, modifies all numeric columns.
        quarters_affected: Specific quarters to target. None = all quarters.
        add_rows: Number of new rows to add (right_only rows).
        remove_rows: Number of rows to remove (left_only rows).
        null_inject_rate: Fraction of changed values to set to null.
        seed: Random seed.

    Returns:
        Path to the modified Delta table.
    """
    rng = random.Random(seed)
    df = spark.read.format("delta").load(source_path)
    rows = df.collect()
    schema = df.schema

    # Determine which columns to change
    if change_cols is None:
        change_cols = [f.name for f in schema.fields if isinstance(f.dataType, T.DoubleType)]

    # Determine which rows to modify
    target_rows = rows
    if quarters_affected is not None:
        target_rows = [r for r in rows if r["quarter_date"] in quarters_affected]

    n_change = max(1, int(len(target_rows) * change_rate))
    rows_to_change_indices = set(rng.sample(range(len(target_rows)), min(n_change, len(target_rows))))

    # Build modified rows
    all_row_dicts = [r.asDict() for r in rows]

    # Map target rows back to all_rows indices
    if quarters_affected is not None:
        target_indices_in_all = [
            i for i, r in enumerate(rows) if r["quarter_date"] in quarters_affected
        ]
    else:
        target_indices_in_all = list(range(len(rows)))

    for local_idx in rows_to_change_indices:
        global_idx = target_indices_in_all[local_idx]
        row_dict = all_row_dicts[global_idx]
        for col in change_cols:
            if col in row_dict:
                if rng.random() < null_inject_rate:
                    row_dict[col] = None
                elif isinstance(schema[col].dataType, T.DoubleType):
                    old_val = row_dict[col]
                    if old_val is not None:
                        row_dict[col] = round(old_val + rng.gauss(0, 10.0), 4)
                    else:
                        row_dict[col] = round(rng.gauss(100.0, 50.0), 4)
                elif isinstance(schema[col].dataType, T.StringType):
                    row_dict[col] = "".join(rng.choices(string.ascii_uppercase, k=8))
                elif isinstance(schema[col].dataType, T.BooleanType):
                    row_dict[col] = not row_dict[col] if row_dict[col] is not None else True

    # Remove rows (left_only scenario)
    if remove_rows > 0:
        remove_indices = set(rng.sample(range(len(all_row_dicts)), min(remove_rows, len(all_row_dicts))))
        all_row_dicts = [r for i, r in enumerate(all_row_dicts) if i not in remove_indices]

    # Add rows (right_only scenario)
    if add_rows > 0:
        max_policy_id = max(r["policy_id"] for r in all_row_dicts)
        sample_row = all_row_dicts[0]
        for i in range(add_rows):
            new_row = dict(sample_row)
            new_row["policy_id"] = max_policy_id + i + 1
            # Randomize numeric values
            for col in change_cols:
                if col in new_row and isinstance(schema[col].dataType, T.DoubleType):
                    new_row[col] = round(rng.gauss(100.0, 50.0), 4)
            all_row_dicts.append(new_row)

    # Write modified data
    modified_df = spark.createDataFrame(
        [tuple(r[f.name] for f in schema.fields) for r in all_row_dicts],
        schema=schema,
    )
    modified_df.write.format("delta").mode("overwrite").save(output_path)

    return output_path
