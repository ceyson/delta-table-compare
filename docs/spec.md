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

### Persistence contracts (batching dimension)

Output tables fall into two categories with different, deliberate contracts for
how the batching dimension (`cfg.qtr_col`, e.g. `quarter_date`) is persisted:

**Aggregate artifacts** — `quarter_checksums`, `row_status_counts`,
`column_summary_by_quarter`:
- The batching dimension is persisted as a single canonical column named
  **`batch_key`**, always stored as a **string** (dates as `yyyy-MM-dd`,
  timestamps as `yyyy-MM-dd'T'HH:mm:ss.SSSSSS`, integral values as their
  base-10 string).
- Because `batch_key` is always a string, these shared tables have an
  **invariant schema**. Reconciliation runs whose source batching column is
  represented differently (Date, Integer, Long, …) can safely append into the
  same tables without `DELTA_FAILED_TO_MERGE_FIELDS`.

**Detail artifacts** — `mismatch_sample`, `mismatch_detail`,
`row_status_detail`:
- These persist the **complete native-typed business key** (`cfg.key_cols`),
  of which the batching column is one member.
- Native key datatypes are **intentionally preserved** to support row-level
  investigation and type-faithful joins back to the source tables.
- Consequently, detail tables are intended for reconciliation runs whose
  business-key datatypes remain **consistent** within a shared output schema.

> **Usage constraint — shared detail tables require consistent business-key
> datatypes.** Do not append to the same shared detail tables from
> reconciliations whose business-key datatypes differ (e.g. a `Date`
> `quarter_date` in one run and an `Integer` `quarter_date` in another). Doing
> so raises `DELTA_FAILED_TO_MERGE_FIELDS` on the native key columns. If key
> schemas must differ, use **separate output schemas** per dataset, or disable
> detail output with **`detail_mode="summary"`** (and leave
> `write_row_status_detail=False`, the default). This constraint does not apply
> to the aggregate artifacts, whose `batch_key` schema is invariant.

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
| batch_key | string | Batching dimension (canonical string of `qtr_col`) |
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
| batch_key | string | Batching dimension (canonical string of `qtr_col`) |
| row_status | string | matched / left_only / right_only |
| row_count | long | Count of rows in this status |

### `column_summary_by_quarter`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Run identifier |
| source_label | string | Source label |
| batch_key | string | Batching dimension (canonical string of `qtr_col`) |
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

Same schema as `column_summary_by_quarter` but without `batch_key` — aggregated across all quarters.

### `mismatch_sample`

| Column | Type | Description |
|--------|------|-------------|
| run_id | string | Run identifier |
| source_label | string | Source label |
| (key columns) | varies (native) | Full business key, preserved with native source datatypes (includes the batching column) |
| column | string | Column with mismatch |
| left_value | string | Left value (cast to string) |
| right_value | string | Right value (cast to string) |

> **Note:** Unlike the aggregate artifacts, detail tables keep the batching
> column in its **native type** as part of the composite key (see *Persistence
> contracts* above). `mismatch_detail` (written when `detail_mode="full_direct"`)
> and `row_status_detail` (written when `write_row_status_detail=True`) follow
> the same native-key contract.

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

## Migration Notes — `batch_key`

The batching dimension in the three shared **aggregate artifacts** is now
persisted as the canonical `batch_key` string column (previously the native
`qtr_col`, e.g. `quarter_date`). One-time migration when upgrading an existing
output schema:

- **Regenerate the three aggregate artifacts.** Existing
  `recon_quarter_checksums`, `recon_row_status_counts`, and
  `recon_column_summary_by_quarter` tables carry the old native-typed batching
  column and are incompatible with the new `batch_key STRING` schema. Drop them
  (they are fully regenerated on the next reconciliation run) — e.g. via the
  `cleanup_recon_tables` utility, or `DROP TABLE` on each. Appending into the
  old tables without dropping would leave a mixed/polluted schema.
- **Detail-table schemas are unchanged.** `mismatch_sample`, `mismatch_detail`,
  and `row_status_detail` retain their existing native-typed composite key and
  require **no** migration.
- **Downstream queries** against the aggregate artifacts must reference
  `batch_key` instead of `quarter_date` (values are the canonical string form of
  the period; e.g. `2020-03-31`).
