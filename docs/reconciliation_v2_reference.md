# Spark Data Reconciliation V2 — Reference Guide

## 1. Overview

`run_reconciliation(cfg)` compares two large Spark DataFrames (Delta tables) column-by-column and quarter-by-quarter, producing a suite of output tables that describe **what changed, where, and by how much**. It is designed for tables with millions of rows and thousands of columns, using a hash-tiered strategy to minimize full-table scans.

### Execution flow

| Phase | Name | What it does |
|-------|------|--------------|
| 0 | Quarter screening | One aggregate checksum per quarter per table (2 scans). Identical quarters are skipped entirely. |
| 1 | Hash extraction | For changed quarters only, reads each table once and produces a narrow hash table per row and per column group. |
| 2 | Key reconciliation & row triage | Joins the two narrow hash tables. Classifies every row as `matched`, `left_only`, or `right_only`. Among matched rows, identifies which have different hashes (changed). |
| 3 | Group triage | Compares per-group hashes on changed rows to determine which column groups actually differ — no wide-table I/O. |
| 4 | Targeted comparison | Reads source tables filtered to changed keys and changed column groups only. Produces column-level mismatch statistics and sample rows. |
| 5 | Rollups & noise detection | Emits zero-mismatch summaries for identical quarters and unchanged groups. Builds cross-quarter rollups. Flags noisy columns. |

### Key terminology

The reconciliation operates at two distinct levels — **row-level** and **column-level** — and uses specific terms at each. Understanding these definitions is essential for interpreting every output table.

#### Row-level terms

- **Matched row:** A row whose composite key (the full set of `key_cols`, e.g. `(id, quarter_date)`) exists in **both** the left and right tables. "Matched" refers exclusively to key presence — it says nothing about whether the row's column values are the same or different. A matched row may have zero column differences or many.
- **Left-only row:** A row whose composite key exists in the left table but **not** in the right table (the row was deleted or is missing from the new data).
- **Right-only row:** A row whose composite key exists in the right table but **not** in the left table (a new row was added).
- **Identical row:** A matched row where **every** compared column value is exactly the same on both sides (verified by row-level hash). No column-level comparison is performed for identical rows.
- **Changed row:** A matched row where at least one compared column value differs between left and right (row-level hash differs). Phase 4 examines these rows column-by-column.

#### Column-level terms (apply per column, per matched row)

- **Column-level mismatch:** For a given column on a matched row, the left value and right value are **not equivalent**. Equivalence rules depend on column type:
  - *Numeric columns:* values are equivalent if `|left − right| ≤ tolerance`. If the absolute difference exceeds the tolerance, it is a mismatch.
  - *Non-numeric columns:* values are equivalent if they are null-safe equal (`left <=> right` in SQL). Any difference in value — including case, whitespace, or content — is a mismatch.
  - *Null-vs-non-null:* If one side is `null` and the other is non-null, it is **always** a mismatch regardless of column type. This is a special case called a **null mismatch**.
- **Null mismatch:** A subset of column-level mismatches where exactly one side is `null` and the other is non-null. Tracked separately because null appearance/disappearance often has a different root cause than a value change.
- **Non-null compared (nonnull_compared_count):** The number of matched rows where **both** the left value and the right value for this column are non-null. Rows where either side is null are excluded from this count. This tells you how many rows had a meaningful value-to-value comparison (as opposed to a null-vs-value or null-vs-null situation).
- **Both-null rows:** Rows where the column is `null` on **both** sides. These are **not** counted as mismatches and are **not** included in `nonnull_compared_count`. They are silent — no action needed.

#### Counts hierarchy (for a single column in a single quarter)

```
matched_row_count
├── identical rows (row hash matched — no per-column check needed, assumed 0 mismatches)
└── changed rows (row hash differed — examined column-by-column)
    ├── both-null rows .............. not a mismatch, not in nonnull_compared_count
    ├── null mismatch rows .......... one side null, other non-null → counted in mismatch_count AND null_mismatch_count
    ├── non-null equivalent rows .... both non-null, within tolerance → in nonnull_compared_count, NOT a mismatch
    └── non-null mismatch rows ...... both non-null, beyond tolerance → in nonnull_compared_count AND mismatch_count
```

---

## 2. `ReconcileConfig` Parameters

### Required

| Parameter | Type | Description |
|-----------|------|-------------|
| `left_table_name` | `str` | Fully qualified name of the left (baseline / old) Delta table. |
| `right_table_name` | `str` | Fully qualified name of the right (target / new) Delta table. |
| `output_catalog` | `str` | Unity Catalog catalog where output tables are written. |
| `output_schema` | `str` | Schema (database) within the catalog for output tables. |
| `key_cols` | `Sequence[str]` | Columns that together uniquely identify a row (e.g., `["id", "quarter_date"]`). Must include `qtr_col`. |
| `qtr_col` | `str` | The date/period partition column (e.g., `"quarter_date"`). Used to partition work and skip unchanged periods. |
| `critical_cols` | `Sequence[str]` | High-priority feature columns. These are placed into the first hash group so they are always compared in the first batch. |

### Optional — Column selection

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `all_feature_cols` | `Sequence[str]` or `None` | `None` | Explicit list of feature columns to compare. When `None`, all columns common to both tables (minus key columns) are compared. |
| `noncritical_cols` | `Sequence[str]` or `None` | `None` | Derived automatically as `all_feature_cols − critical_cols`. Rarely set manually. |

### Optional — Source identification

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_label` | `str` or `None` | `None` | Free-form label identifying the data source or warehouse (e.g., `"EDW_PROD"`, `"WH_EAST"`). When set, the value is written as a nullable `source_label` column in **every** output table, enabling direct SQL filtering like `WHERE source_label = 'EDW_PROD'` without joining to `run_metadata`. When `None`, the column is still present but contains `null`. |

### Optional — Labels & tolerances

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `left_label` | `str` | `"old_value"` | Display label for the left table's values in mismatch detail output. |
| `right_label` | `str` | `"new_value"` | Display label for the right table's values in mismatch detail output. |
| `tolerances` | `Mapping[str, float]` | `{}` | Per-column numeric tolerance. A numeric difference ≤ tolerance is not counted as a mismatch. |
| `default_numeric_tolerance` | `float` | `0.0` | Default tolerance applied to all numeric columns not listed in `tolerances`. |

### Optional — Execution tuning

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `run_id` | `str` or `None` | auto-generated timestamp | Unique identifier for this reconciliation run. All output rows carry this value. |
| `hash_group_size` | `int` | `100` | Number of feature columns per hash group. Controls granularity of group-level triage (Phase 3). |
| `comparison_batch_size` | `int` | `200` | Max columns compared in a single Spark job within Phase 4. Keeps Catalyst plan sizes manageable. |
| `sample_per_column` | `int` | `10` | Max mismatch sample rows retained per (quarter, column) pair. |
| `detail_mode` | `str` | `"sample"` | Controls mismatch row output: `"summary"` = stats only, `"sample"` = stats + capped samples, `"full_direct"` = stats + samples + every mismatch row. |
| `write_row_status_detail` | `bool` | `False` | If `True`, writes one row per key with its status (`matched`, `left_only`, `right_only`) to `row_status_detail`. |
| `noisy_column_threshold` | `float` | `0.95` | Columns with a mismatch rate ≥ this threshold (0.0–1.0) are flagged as suspected systematic/noisy. |
| `cleanup_tmp_tables` | `bool` | `True` | Drop temporary intermediate tables after a successful run. |

### Optional — Hash normalization

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `trim_strings_for_hash` | `bool` | `False` | Trim leading/trailing whitespace from string columns before hashing. |
| `lower_strings_for_hash` | `bool` | `False` | Lowercase string columns before hashing (case-insensitive comparison). |
| `float_hash_round_scale` | `int` or `None` | `None` | Round float/double columns to this many decimal places before hashing. Useful when insignificant precision differences should be ignored. |

### Internal / advanced

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `temp_prefix` | `str` | `"recon_tmp"` | Prefix for temporary intermediate tables. |
| `final_prefix` | `str` | `"recon"` | Prefix for output artifact tables. |

---

## 3. Return value

`run_reconciliation` returns a `dict[str, str]` mapping artifact names to their fully qualified table names:

```python
{
    "run_id":                       "20250504_143000",
    "run_metadata":                 "catalog.schema.recon_run_metadata",
    "quarter_checksums":            "catalog.schema.recon_quarter_checksums",
    "row_status_counts":            "catalog.schema.recon_row_status_counts",
    "row_status_detail":            "catalog.schema.recon_row_status_detail",
    "column_summary_by_quarter":    "catalog.schema.recon_column_summary_by_quarter",
    "column_summary_all_quarters":  "catalog.schema.recon_column_summary_all_quarters",
    "mismatch_sample":              "catalog.schema.recon_mismatch_sample",
    "mismatch_detail":              "catalog.schema.recon_mismatch_detail",
    "noisy_columns":                "catalog.schema.recon_noisy_columns",
}
```

---

## 4. Output Artifact Tables

All tables are Delta tables written with `mode("append")`. Each row carries a `run_id` and an optional `source_label` so multiple runs and data sources coexist in the same tables. Filter by `run_id` (and optionally `source_label`) when querying.

**Persistence contract for the batching dimension.** The **aggregate artifacts**
(`quarter_checksums`, `row_status_counts`, `column_summary_by_quarter`) persist
the batching dimension (`cfg.qtr_col`) as a single canonical column named
**`batch_key`**, always stored as a **string** (dates as `yyyy-MM-dd`,
timestamps as `yyyy-MM-dd'T'HH:mm:ss.SSSSSS`, integral values base-10). This
gives these shared tables an invariant schema, so runs whose source batching
column is typed differently (Date, Integer, Long, …) can append safely. The
**detail artifacts** (`row_status_detail`, `mismatch_sample`, `mismatch_detail`)
instead preserve the **full native-typed business key** (`cfg.key_cols`) for
row-level investigation and source-table joins; see the usage constraint in
`docs/spec.md` (shared detail tables require type-consistent business keys).

---

### 4.1 `run_metadata`

**Purpose:** Audit log of every reconciliation run. Records what was compared, when, and whether the run completed successfully.

**Analytical use:** Confirm run parameters before interpreting results. Track run duration. Filter downstream reports by `run_id`.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Unique run identifier. |
| `source_label` | string (nullable) | Optional label identifying the data source/warehouse (e.g., `"EDW_PROD"`). `null` if not set. |
| `left_table_name` | string | Fully qualified name of the left table. |
| `right_table_name` | string | Fully qualified name of the right table. |
| `key_cols` | array\<string\> | Key columns used for row matching. |
| `qtr_col` | string | Quarter/period partition column. |
| `critical_column_count` | int | Number of critical columns. |
| `noncritical_column_count` | int | Number of non-critical columns. |
| `total_compare_column_count` | int | Total feature columns compared. |
| `hash_group_size` | int | Columns per hash group. |
| `detail_mode` | string | `"summary"`, `"sample"`, or `"full_direct"`. |
| `started_at` | timestamp | When the run began. |
| `completed_at` | timestamp | When the run finished (null if still running). |
| `status` | string | `"RUNNING"`, `"COMPLETED"`, or `"FAILED"`. |

---

### 4.2 `quarter_checksums`

**Purpose:** Shows the Phase 0 screening result for every quarter — whether each period was identical, changed, or present on only one side.

**Analytical use:** Quickly identify which quarters have differences without examining row-level data. Understand data coverage (which periods exist in each table). A high proportion of `identical` quarters indicates limited change scope and a fast run.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Run identifier. |
| `source_label` | string (nullable) | Source/warehouse label. `null` if not set. |
| `batch_key` | string | Batching dimension — canonical string form of `cfg.qtr_col` (e.g. `2020-03-31`). |
| `left_checksum` | long | Aggregate hash for this quarter in the left table. `null` if the quarter does not exist on the left. |
| `right_checksum` | long | Aggregate hash for this quarter in the right table. `null` if the quarter does not exist on the right. |
| `left_row_count` | long | Number of rows in this quarter on the left side. `null` if absent. |
| `right_row_count` | long | Number of rows in this quarter on the right side. `null` if absent. |
| `quarter_status` | string | One of: `"identical"` (checksums match), `"changed"` (both exist, checksums differ), `"left_only"`, `"right_only"`. |

**Example interpretation:** If `quarter_status = "identical"` for Q1 2020, you know every row and every column value in that quarter is unchanged — no further investigation needed.

---

### 4.3 `row_status_counts`

**Purpose:** Per-quarter summary of how many rows fall into each match category. This is the highest-level row-matching report.

**Analytical use:** Quickly assess the scale of change per quarter. A quarter with 100,000 `matched` rows and 50 `left_only` rows tells you that 50 rows were deleted. Useful for executive dashboards and data quality KPIs.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Run identifier. |
| `source_label` | string (nullable) | Source/warehouse label. `null` if not set. |
| `batch_key` | string | Batching dimension — canonical string form of `cfg.qtr_col` (e.g. `2020-03-31`). |
| `row_status` | string | One of: `"matched"` (composite key exists in **both** tables — says nothing about whether column values are the same), `"left_only"` (composite key exists only on the left — row was deleted or is absent from the right table), `"right_only"` (composite key exists only on the right — row is new or absent from the left table). |
| `row_count` | long | Number of rows with this status in this quarter. |

**Example interpretation:** If Q3 2023 has `matched = 58,000`, `left_only = 12`, `right_only = 5`, then 12 rows were removed and 5 new rows appeared, while 58,000 rows are present on both sides. Note: `matched` does **not** mean the values are identical — some of those 58,000 rows may have column-level differences (see `column_summary_by_quarter` for details).

> **Tip:** To get grand totals across all quarters (equivalent to the removed `row_status_counts_all_quarters` table), use:
> ```sql
> SELECT run_id, source_label, row_status, SUM(row_count) AS row_count
> FROM recon_row_status_counts WHERE run_id = '<run_id>'
> GROUP BY run_id, source_label, row_status
> ```

---

### 4.4 `row_status_detail`

**Purpose:** One row per key showing its match status. Only written when `write_row_status_detail = True`.

**Analytical use:** Identify exactly which rows are missing from one side. Join back to source tables to investigate root causes of missing/extra rows.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Run identifier. |
| `source_label` | string (nullable) | Source/warehouse label. `null` if not set. |
| `<key_cols...>` | varies | One column per key column (e.g., `id`, `quarter_date`). |
| `row_status` | string | `"matched"` (composite key exists in both tables — values may or may not differ), `"left_only"` (key only on left), or `"right_only"` (key only on right). |

---

### 4.5 `column_summary_by_quarter`

**Purpose:** The core analytical output. For every (quarter, column) pair, reports how many rows were compared, how many had mismatches, the mismatch rate, and the maximum absolute difference (for numerics).

**Analytical use:** Pinpoint exactly which columns changed in which quarters. Sort by `mismatch_count` descending to find the most impacted columns. Filter by `is_numeric = true` and sort by `max_abs_diff` to find the largest numeric movements. Compare `mismatch_pct` across quarters to detect quarter-specific data issues.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Run identifier. |
| `source_label` | string (nullable) | Source/warehouse label. `null` if not set. |
| `batch_key` | string | Batching dimension — canonical string form of `cfg.qtr_col` (e.g. `2020-03-31`). |
| `column` | string | Feature column name. |
| `left_type` | string | Data type of this column in the left table (e.g., `"DoubleType"`, `"StringType"`). |
| `right_type` | string | Data type of this column in the right table. |
| `is_numeric` | boolean | Whether the column was treated as numeric for comparison. |
| `tolerance` | double | Numeric tolerance applied. For numeric columns, two non-null values are considered equivalent if `|left − right| ≤ tolerance`; values differing by more than this are counted as mismatches. Always `0.0` for non-numeric columns. |
| `changed_row_count` | long | Number of key-matched rows whose overall row hash differed (i.e., at least one column in the row changed). These are the rows that were actually examined column-by-column. |
| `nonnull_compared_count` | long | Among changed rows, the number where **both** the left value and right value of this column are non-null. Rows where either side is null are excluded. This is the population of value-to-value comparisons. |
| `mismatch_count` | long | Total rows where this column's left and right values are **not equivalent**. This includes two disjoint sub-populations: (a) both sides non-null but the values differ beyond tolerance, and (b) one side is null while the other is non-null (a "null mismatch"). Rows that are null on both sides are **not** mismatches. |
| `null_mismatch_count` | long | Subset of `mismatch_count` counting only rows where exactly one side is `null` and the other is non-null. Useful for isolating null-appearance/disappearance issues from value-change issues. |
| `max_abs_diff` | double | Largest absolute numeric difference (`|right − left|`) observed across all compared rows for this column in this quarter. `null` for non-numeric columns or when no non-null pair exists. |
| `matched_row_count` | long | Total rows whose composite key exists on **both** sides in this quarter, regardless of whether their values are the same or different. Equals `changed_row_count` (rows with at least one difference) + identical rows (rows with zero differences). This is the true comparison population. |
| `mismatch_pct` | double | `mismatch_count / matched_row_count`. The fraction of **all** key-matched rows (not just changed rows) where this column's values are not equivalent. |
| `null_mismatch_pct` | double | `null_mismatch_count / matched_row_count`. The fraction of all key-matched rows where this column has a null-vs-non-null discrepancy. |

**Example interpretation:** Column `revenue` in Q2 2024 has `mismatch_count = 42`, `matched_row_count = 58,000`, `mismatch_pct = 0.0007`, `max_abs_diff = 1523.45`. This means 42 out of 58,000 matched rows had a revenue change, the largest being $1,523.45. A low `mismatch_pct` suggests a targeted correction rather than a systemic recalculation.

> **Tip:** To get a per-quarter cell-level rollup (equivalent to the removed `rollup_by_quarter` table), use:
> ```sql
> SELECT run_id, source_label, batch_key,
>        SUM(matched_row_count) AS matched_cell_checks,
>        SUM(mismatch_count) AS mismatch_cell_count,
>        SUM(null_mismatch_count) AS null_mismatch_cell_count,
>        SUM(mismatch_count) / NULLIF(SUM(matched_row_count), 0) AS mismatch_cell_pct
> FROM recon_column_summary_by_quarter WHERE run_id = '<run_id>'
> GROUP BY run_id, source_label, batch_key
> ```

---

### 4.6 `column_summary_all_quarters`

**Purpose:** Cross-quarter rollup of `column_summary_by_quarter`. One row per column, aggregating across all quarters.

**Analytical use:** Identify which columns are most impacted overall. Rank columns by total `mismatch_count` to prioritize investigation. Compare `mismatch_pct` across columns to understand the breadth of changes.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Run identifier. |
| `source_label` | string (nullable) | Source/warehouse label. `null` if not set. |
| `column` | string | Feature column name. |
| `left_type` | string | Data type on the left. |
| `right_type` | string | Data type on the right. |
| `is_numeric` | boolean | Whether the column is numeric. |
| `tolerance` | double | Tolerance applied. |
| `matched_row_count` | long | Sum of `matched_row_count` across all quarters — total key-matched rows (both identical and changed) across the entire dataset. |
| `nonnull_compared_count` | long | Sum across all quarters of rows where both left and right values for this column were non-null. |
| `mismatch_count` | long | Total column-level mismatches (value differences beyond tolerance + null-vs-non-null cases) across all quarters. |
| `mismatch_pct` | double | `mismatch_count / matched_row_count` across all quarters. Fraction of all key-matched rows where this column differs. |
| `null_mismatch_count` | long | Total null-vs-non-null mismatches (one side null, other non-null) across all quarters. |
| `null_mismatch_pct` | double | `null_mismatch_count / matched_row_count` across all quarters. |
| `max_abs_diff` | double | Largest absolute numeric difference (`|right − left|`) observed in any quarter. `null` for non-numeric columns. |

**Example interpretation:** Column `risk_score` has `mismatch_count = 3,500,000`, `matched_row_count = 3,500,000`, `mismatch_pct = 1.0`. Every single matched row changed — this column was completely recalculated and will appear in the `noisy_columns` table.

---

### 4.7 `mismatch_sample`

**Purpose:** A small set of example rows for each (quarter, column) mismatch, showing both the old and new values. Capped at `sample_per_column` rows per combination. Only written when `detail_mode` is `"sample"` or `"full_direct"`.

**Analytical use:** Quickly inspect what the actual value changes look like without scanning the full data. Confirm whether changes are corrections, recalculations, or data errors. Share specific examples with stakeholders.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Run identifier. |
| `source_label` | string (nullable) | Source/warehouse label. `null` if not set. |
| `<key_cols...>` | varies | Key columns identifying the row. |
| `column` | string | The feature column that mismatched. |
| `<left_label>` | string | Left (old) value, cast to string. Column name is `cfg.left_label` (default `"old_value"`). |
| `<right_label>` | string | Right (new) value, cast to string. Column name is `cfg.right_label` (default `"new_value"`). |
| `null_mismatch` | boolean | `true` if exactly one side is `null` and the other is non-null; `false` if both sides are non-null but differ in value. |
| `diff` | double | Signed numeric difference (`right − left`). `null` for non-numeric columns or when either side is null. |
| `abs_diff` | double | Absolute numeric difference (`|right − left|`). `null` for non-numeric columns or when either side is null. |
| `pct_diff` | double | `(right − left) / left`. `null` if left is zero or column is non-numeric. |
| `pct_diff_pct` | double | `pct_diff × 100` (percentage form). |
| `tolerance_used` | double | The numeric tolerance that was applied. |

**Example interpretation:** Row `id=12345, quarter=2024-06-30`, column `premium`: `old_value = "1000.00"`, `new_value = "1050.00"`, `diff = 50.0`, `pct_diff_pct = 5.0`. The premium increased by 5% for this policy.

---

### 4.8 `mismatch_detail`

**Purpose:** Every mismatch row, not just a sample. Only written when `detail_mode = "full_direct"`.

**Analytical use:** Full audit trail for downstream consumption. Feed into automated validation pipelines or join with source tables for root-cause analysis.

**Schema:** Identical to `mismatch_sample` (see above) but without the per-column row-count cap.

> **Warning:** This table can be very large for high-change-rate reconciliations. Use `detail_mode = "sample"` unless you specifically need every row.

---

### 4.9 `noisy_columns`

**Purpose:** Flags columns where the mismatch rate exceeds the `noisy_column_threshold` (default 95%). These are suspected systematic recalculations, timestamp updates, or other non-informative changes.

**Analytical use:** Exclude noisy columns from downstream quality assessments. If `risk_score` changes in 100% of rows, it was likely recalculated globally and is not a data quality issue. Consider adding such columns to `tolerances` or excluding them from future runs.

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | string | Run identifier. |
| `source_label` | string (nullable) | Source/warehouse label. `null` if not set. |
| `column` | string | The noisy column name. |
| `matched_row_count` | long | Total matched rows compared for this column. |
| `mismatch_count` | long | Number of mismatches. |
| `mismatch_pct` | double | Mismatch rate (≥ `noisy_column_threshold`). |
| `suspected_reason` | string | Always `"change_rate_above_threshold"`. |

**Example interpretation:** Column `last_modified_ts` has `mismatch_pct = 1.0`. Every row's timestamp changed. This is expected for a timestamp column and should be excluded from quality metrics.

---

## 5. Quick-start query recipes

### Top 10 most impacted columns

```sql
SELECT column, mismatch_count, mismatch_pct, max_abs_diff
FROM   recon_column_summary_all_quarters
WHERE  run_id = '<run_id>'
ORDER BY mismatch_count DESC
LIMIT 10
```

### Top 10 most impacted columns — filtered by source

```sql
SELECT column, mismatch_count, mismatch_pct, max_abs_diff
FROM   recon_column_summary_all_quarters
WHERE  run_id = '<run_id>'
  AND  source_label = 'EDW_PROD'
ORDER BY mismatch_count DESC
LIMIT 10
```

### Quarters with the most row-level changes

```sql
SELECT batch_key, row_status, row_count
FROM   recon_row_status_counts
WHERE  run_id = '<run_id>'
ORDER BY batch_key, row_status
```

### Sample mismatches for a specific column

```sql
SELECT *
FROM   recon_mismatch_sample
WHERE  run_id = '<run_id>'
  AND  column = 'revenue'
ORDER BY quarter_date
```

### Excluding noisy columns from quality metrics

```sql
SELECT s.*
FROM   recon_column_summary_all_quarters s
LEFT JOIN recon_noisy_columns n
  ON  s.run_id = n.run_id AND s.column = n.column
WHERE  s.run_id = '<run_id>'
  AND  n.column IS NULL
ORDER BY s.mismatch_count DESC
```
