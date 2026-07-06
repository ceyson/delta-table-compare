"""
Polars-native synthetic test data generator — no PySpark dependency.

Generates two Delta tables (left/right) with configurable dimensions
using Polars + deltalake for I/O.
"""

from __future__ import annotations

import os
import random
import string
from datetime import date, timedelta
from typing import Optional

import polars as pl
from deltalake import write_deltalake


def _quarter_dates(n_quarters: int, start_year: int = 2020) -> list[date]:
    """Generate a list of quarter-start dates."""
    quarters = []
    for i in range(n_quarters):
        year = start_year + i // 4
        month = (i % 4) * 3 + 1
        quarters.append(date(year, month, 1))
    return quarters


def generate_test_data(
    output_path: str,
    n_quarters: int = 5,
    base_rows_per_quarter: int = 5000,
    rows_per_quarter_increment: int = 0,
    n_numeric_cols: int = 10,
    n_string_cols: int = 3,
    n_date_cols: int = 2,
    n_bool_cols: int = 2,
    seed: int = 42,
    start_year: int = 2020,
) -> tuple[str, str, list[str]]:
    """Generate left and right Delta tables with identical data.

    Returns:
        (left_table_path, right_table_path, critical_cols)
    """
    rng = random.Random(seed)
    quarters = _quarter_dates(n_quarters, start_year)

    policy_ids: list[int] = []
    quarter_dates: list[date] = []
    numeric_data: dict[str, list[Optional[float]]] = {
        f"num_col_{i:03d}": [] for i in range(n_numeric_cols)
    }
    string_data: dict[str, list[Optional[str]]] = {
        f"str_col_{i:03d}": [] for i in range(n_string_cols)
    }
    date_data: dict[str, list[Optional[date]]] = {
        f"date_col_{i:03d}": [] for i in range(n_date_cols)
    }
    bool_data: dict[str, list[Optional[bool]]] = {
        f"bool_col_{i:03d}": [] for i in range(n_bool_cols)
    }

    policy_counter = 1
    for qi, qtr in enumerate(quarters):
        n_rows = base_rows_per_quarter + qi * rows_per_quarter_increment
        for _ in range(n_rows):
            policy_ids.append(policy_counter)
            quarter_dates.append(qtr)

            for col in numeric_data:
                if rng.random() < 0.05:
                    numeric_data[col].append(None)
                else:
                    numeric_data[col].append(round(rng.gauss(100.0, 50.0), 4))

            for col in string_data:
                if rng.random() < 0.05:
                    string_data[col].append(None)
                else:
                    string_data[col].append(
                        "".join(rng.choices(string.ascii_uppercase, k=8))
                    )

            for col in date_data:
                if rng.random() < 0.05:
                    date_data[col].append(None)
                else:
                    offset = rng.randint(-365, 365)
                    date_data[col].append(qtr + timedelta(days=offset))

            for col in bool_data:
                if rng.random() < 0.05:
                    bool_data[col].append(None)
                else:
                    bool_data[col].append(rng.choice([True, False]))

            policy_counter += 1

    df = pl.DataFrame(
        {"policy_id": policy_ids, "quarter_date": quarter_dates}
        | numeric_data
        | string_data
        | date_data
        | bool_data
    )

    left_path = os.path.join(output_path, "left_table")
    right_path = os.path.join(output_path, "right_table")

    os.makedirs(left_path, exist_ok=True)
    os.makedirs(right_path, exist_ok=True)

    write_deltalake(left_path, df.to_arrow(), mode="overwrite")
    write_deltalake(right_path, df.to_arrow(), mode="overwrite")

    n_critical = min(200, n_numeric_cols)
    critical_cols = [f"num_col_{i:03d}" for i in range(n_critical)]

    return left_path, right_path, critical_cols


def inject_differences(
    source_path: str,
    output_path: str,
    change_rate: float = 0.05,
    change_cols: Optional[list[str]] = None,
    add_rows: int = 0,
    remove_rows: int = 0,
    seed: int = 123,
) -> str:
    """Create a modified Delta table with injected differences.

    Returns:
        Path to the modified Delta table.
    """
    rng = random.Random(seed)
    df = pl.read_delta(source_path)

    if change_cols is None:
        change_cols = [c for c in df.columns if c.startswith("num_col_")]

    n_rows = df.height
    n_change = max(1, int(n_rows * change_rate))
    change_indices = set(rng.sample(range(n_rows), min(n_change, n_rows)))

    # Build mutation masks and new values per column
    for col in change_cols:
        if col not in df.columns:
            continue
        dtype = df[col].dtype

        new_values = df[col].to_list()
        for idx in change_indices:
            if dtype in (pl.Float64, pl.Float32):
                old = new_values[idx]
                if old is not None:
                    new_values[idx] = round(old + rng.gauss(0, 10.0), 4)
                else:
                    new_values[idx] = round(rng.gauss(100.0, 50.0), 4)
            elif dtype == pl.Utf8:
                new_values[idx] = "".join(rng.choices(string.ascii_uppercase, k=8))
            elif dtype == pl.Boolean:
                old = new_values[idx]
                new_values[idx] = not old if old is not None else True

        df = df.with_columns(pl.Series(col, new_values, dtype=dtype))

    # Remove rows
    if remove_rows > 0:
        keep_mask = [True] * n_rows
        for idx in rng.sample(range(n_rows), min(remove_rows, n_rows)):
            keep_mask[idx] = False
        df = df.filter(pl.Series(keep_mask))

    # Add rows
    if add_rows > 0:
        max_pid = df["policy_id"].max()
        sample_row = df.head(1)
        new_rows = []
        for i in range(add_rows):
            row_data: dict = {}
            for col in df.columns:
                if col == "policy_id":
                    row_data[col] = max_pid + i + 1
                else:
                    row_data[col] = sample_row[col][0]
            new_rows.append(row_data)
        df = pl.concat([df, pl.DataFrame(new_rows, schema=df.schema)])

    os.makedirs(output_path, exist_ok=True)
    write_deltalake(output_path, df.to_arrow(), mode="overwrite")

    return output_path
