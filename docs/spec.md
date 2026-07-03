# Specification

## Configuration (`ReconcileConfig`)

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `left_table_name` | str | *required* | FQN or path of the left (baseline) Delta table |
| `right_table_name` | str | *required* | FQN or path of the right (comparison) Delta table |
| `output_catalog` | str | *required* | Catalog for output tables (e.g., `spark_catalog` or `local`) |
| `output_schema` | str | *required* | Schema/database or path for output tables |
| `key_cols` | list[str] | *required* | Composite key columns that uniquely identify a row |
| `qtr_col` | str | *required* | The quarter/partition column for phased screening |
| `critical_cols` | list[str] | *required* | Columns of primary interest (placed in group 0) |
| `all_feature_cols` | list[str] | `[]` | All feature columns to compare (if empty, auto-resolved) |
| `run_id` | str | auto-generated | Unique identifier for this reconciliation run |
| `source_label` | str | `""` | Label for multi-source filtering (e.g., `EDW_PROD`) |
| `engine` | str | `"spark"` | Engine to use: `"spark"` or `"polars"` |
| `detail_mode` | str | `"sample"` | `"summary"`, `"sample"`, or `"full_direct"` |
| `sample_per_column` | int | `5` | Max mismatch samples per column per quarter |
| `hash_group_size` | int | `200` | Max columns per hash group |
| `comparison_batch_size` | int | `200` | Columns per Phase 4 batch (Spark only) |
| `default_numeric_tolerance` | float | `0.0` | Absolute tolerance for numeric comparisons |
| `tolerances` | dict[str, float] | `{}` | Per-column tolerance overrides |
| `noisy_column_threshold` | float | `0.95` | Mismatch rate above which a column is flagged |
| `cleanup_tmp_tables` | bool | `True` | Whether to drop temp tables after completion |
| `temp_prefix` | str | `"recon_tmp"` | Prefix for temporary table names |
| `left_label` | str | `"left_value"` | Column name for left values in mismatch output |
| `right_label` | str | `"right_value"` | Column name for right values in mismatch output |

## Phase Contracts

### Phase 0: Quarter Screening

**Input**: Left table, Right table, compare columns
**Output**: `quarter_checksums` table, list of changed quarters, quarter status DataFrame

Logic:
- For each quarter value, compute `SUM(xxhash64(col1, col2, ...))` over all compare columns.
- Compare left vs. right checksum per quarter.
- Classify: `identical` (same checksum), `changed` (different), `left_only`, `right_only`.

### Phase 1: Hash Extraction

**Input**: Changed quarters, compare columns, column groups
**Output**: Left hash table, Right hash table (keyed by composite key)

Per row:
- `row_hash_all`: hash of all compare columns (single value)
- `gh_0`, `gh_1`, ...: hash of each column group

### Phase 2: Key Reconciliation

**Input**: Left hashes, Right hashes
**Output**: `row_status_counts` table, changed keys DataFrame, total matched per quarter

Logic:
- Full outer join on key columns.
- Classify each key: `matched`, `left_only`, `right_only`.
- For matched rows: compare `row_hash_all` — if different, the row has changes.

### Phase 2b: Nonnull Counts

**Input**: Both-sided quarters, compare columns
**Output**: Nonnull count per (quarter, column) — used as denominator for mismatch percentages

Logic:
- Inner join left and right on keys.
- Per column: count rows where both left and right values are non-null.

### Phase 3: Group Triage

**Input**: Changed keys with per-group match flags
**Output**: Dict mapping group index → DataFrame of keys that changed in that group

### Phase 4: Targeted Comparison

**Input**: Changed keys per group, source tables, column groups
**Output**: `column_summary_by_quarter`, `mismatch_sample`

For each group with changes:
- Read only the relevant columns for the changed keys.
- Per column: count mismatches, null mismatches, compute max absolute difference.
- Sample mismatched rows for inspection.

### Phase 5: Rollups

**Input**: All per-quarter summaries
**Output**: `column_summary_all_quarters`, `noisy_columns`

Logic:
- Aggregate per-column stats across all quarters.
- Zero-fill for identical quarters and unchanged groups (0 mismatches, full nonnull counts).
- Flag columns exceeding `noisy_column_threshold`.

## Output Table Schemas

### `run_metadata`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Unique run identifier |
| source_label | string | Multi-source label |
| left_table_name | string | Left table FQN |
| right_table_name | string | Right table FQN |
| key_cols | array[string] | Key columns used |
| qtr_col | string | Quarter column |
| critical_column_count | int | Number of critical columns |
| noncritical_column_count | int | Number of non-critical columns |
| total_compare_column_count | int | Total columns compared |
| hash_group_size | int | Columns per group |
| detail_mode | string | Detail output mode |
| started_at | timestamp | Run start time |
| completed_at | timestamp | Run completion time |
| status | string | RUNNING / COMPLETED / FAILED |

### `quarter_checksums`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Run identifier |
| source_label | string | Source label |
| quarter_date | date | Quarter value |
| left_checksum | long | Left table checksum |
| right_checksum | long | Right table checksum |
| left_row_count | long | Left row count |
| right_row_count | long | Right row count |
| quarter_status | string | identical / changed / left_only / right_only |

### `row_status_counts`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Run identifier |
| source_label | string | Source label |
| quarter_date | date | Quarter value |
| row_status | string | matched / left_only / right_only |
| row_count | long | Count of rows in this status |

### `column_summary_by_quarter`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Run identifier |
| source_label | string | Source label |
| quarter_date | date | Quarter value |
| column | string | Column name |
| is_numeric | boolean | Whether column is numeric |
| tolerance | double | Tolerance applied |
| changed_row_count | int | Rows compared (with changes) |
| nonnull_compared_count | int | Rows where both sides non-null |
| mismatch_count | int | Count of mismatches |
| null_mismatch_count | int | Count of null-vs-value mismatches |
| max_abs_diff | double | Maximum absolute difference (numeric only) |
| matched_row_count | int | Total matched rows in quarter |
| mismatch_pct | double | mismatch_count / matched_row_count |
| null_mismatch_pct | double | null_mismatch_count / matched_row_count |

### `column_summary_all_quarters`

Same schema as `column_summary_by_quarter` but without `quarter_date` — aggregated across all quarters.

### `mismatch_sample`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Run identifier |
| source_label | string | Source label |
| (key columns) | varies | Composite key values |
| column | string | Column with mismatch |
| left_value | string | Left value (cast to string) |
| right_value | string | Right value (cast to string) |

### `noisy_columns`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Run identifier |
| source_label | string | Source label |
| column | string | Column name |
| mismatch_pct | double | Overall mismatch rate |
| threshold | double | Threshold used for flagging |

## Benchmark Output Schema

### `benchmark_results_delta`

| Column | Type | Description |
|--------|------|-------------|
| timestamp | string | ISO timestamp of benchmark run |
| engine | string | spark / polars |
| profile | string | reconciliation / data_generation / etc. |
| n_quarters | int | Number of quarters in test data |
| n_rows_total | int | Total rows across all quarters |
| n_columns | int | Number of compare columns |
| change_rate | double | Fraction of rows with injected changes |
| phase | string | Phase name or "total" / "table_writes" |
| elapsed_seconds | double | Wall-clock time for this phase |
| status | string | success / error |
| error | string | Error message if failed |
| notes | string | Additional context |

### `write_timings_detail_delta`

| Column | Type | Description |
|--------|------|-------------|
| table_name | string | Target table path or name |
| operation | string | append / overwrite |
| elapsed_seconds | double | Write duration |
| row_count | int | Rows written |
