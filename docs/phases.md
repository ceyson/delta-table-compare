# Reconciliation Steps & Artifact Map

## Overview

A run is orchestrated by `run_reconciliation()` in `recon/runner.py` and mirrored,
step-for-step, by the benchmark harness `run_single_benchmark()` in
`benchmarks/databricks_benchmark.py` (and the Polars path in
`benchmarks/bench_runner.py`). Each `_timed(...)` / `_result(...)` label is exactly
the step name shown in the benchmark reporting.

**Execution order** (as timed): `setup` -> `phase0_quarter_screening` ->
`phase2b_nonnull_counts` -> `phase1_hash_extraction` -> `phase2_key_recon` ->
`phase3_group_triage` -> `phase4_targeted_comparison` -> `phase5_rollups` ->
`cleanup`.

> Note: the nonnull-count step is reported as `phase2b_nonnull_counts`, and although
> it carries the "2b" label it *executes right after Phase 0*, before Phase 1.

## Artifact naming

- **Final outputs** — `catalog.schema.recon_<logical_name>` via `final_table()`
  (`recon/helpers.py`).
- **Temp intermediates** — `catalog.schema.recon_tmp_<logical_name>_<run_id>` via
  `tmp_table()` (`recon/helpers.py`). Dropped in `cleanup`.

---

## Steps

### `setup`

- **Code**: `run_reconciliation()` setup block, `recon/runner.py`.
- **Does**: Validates schema/columns, resolves compare columns, splits them into
  hash groups (`build_column_groups`), and writes the initial `RUNNING` metadata row
  (`create_run_metadata`).
- **Artifact**: `recon_run_metadata` (initial row: tables, key cols, column counts,
  `hash_group_size`, `detail_mode`, `started_at`, `status=RUNNING`).

### `phase0_quarter_screening`

- **Code**: `phase0_quarter_screening()`, `recon/phases.py`.
- **Does**: Computes one order-independent `xxhash64` checksum + row count per quarter
  for each side (`compute_quarter_checksums`), full-outer joins them, and classifies
  each quarter as `identical`, `changed`, `left_only`, or `right_only`. Returns the
  list of changed quarters to narrow all later phases.
- **Artifact**: `recon_quarter_checksums` (per-quarter `left/right_checksum`,
  `left/right_row_count`, `quarter_status`).

### `phase2b_nonnull_counts`

- **Code**: `compute_nonnull_counts()`, `recon/phases.py`.
- **Does**: For quarters with data on both sides (`identical` + `changed`),
  inner-joins the sources on key cols and counts rows where both sides are non-null,
  per (quarter, column). Provides accurate denominators for later `mismatch_pct`.
- **Artifact**: `recon_tmp_nonnull_counts_<run_id>` (temp; schema
  `(qtr, column, nonnull_count)`).

### `phase1_hash_extraction`

- **Code**: `phase1_hash_extraction()`, `recon/phases.py`.
- **Does**: Single-scan per side (changed quarters only) producing one full row hash
  (`row_hash_all`) plus one hash per column group (`gh_0..gh_N`) per row.
- **Artifacts**: `recon_tmp_left_hashes_<run_id>` and
  `recon_tmp_right_hashes_<run_id>` (temp).

### `phase2_key_recon`

- **Code**: `phase2_key_recon_and_row_triage()`, `recon/phases.py`.
- **Does**: Full-outer joins the two narrow hash tables on key cols. Classifies each
  row `matched` / `left_only` / `right_only`; for matched rows compares `row_hash_all`
  and each `gh_i`. Emits counts, optional per-row detail, and isolates keys whose row
  hash differs.
- **Artifacts**:
  - `recon_row_status_counts` (per quarter x `row_status` counts).
  - `recon_row_status_detail` (per-row status; only when `cfg.write_row_status_detail`).
  - `recon_tmp_changed_keys_<run_id>` (temp; changed keys + per-group `gh_i_match`
    flags).

### `phase3_group_triage`

- **Code**: `phase3_group_triage()`, `recon/phases.py`.
- **Does**: In one aggregation over `changed_keys`, determines which column groups
  actually differ, and builds a per-group key set (empty sentinel for unchanged
  groups). No persisted artifact — returns in-memory `group_changed_keys` consumed by
  Phase 4.

### `phase4_targeted_comparison`

- **Code**: `phase4_targeted_comparison()`, `recon/phases.py` (uses
  `compare_columns_for_keys`, `_enrich_and_write_summary`,
  `_sample_and_write_mismatch`).
- **Does**: For each changed group, reads only its columns for only its changed rows,
  computes per-column mismatch stats (numeric tolerance / null mismatches / max abs
  diff), enriches with matched-row and nonnull denominators, and samples mismatch rows.
- **Artifacts**:
  - `recon_column_summary_by_quarter` (per quarter x column stats: `mismatch_count`,
    `null_mismatch_count`, `mismatch_pct`, `max_abs_diff`, etc.).
  - `recon_mismatch_sample` (capped at `sample_per_column`; `detail_mode` in
    `sample`/`full_direct`).
  - `recon_mismatch_detail` (full unsampled rows; only `detail_mode == full_direct`).

### `phase5_rollups`

- **Code**: `emit_zero_fill_for_identical_quarters()`,
  `emit_zero_fill_for_unchanged_groups()`, `build_rollups()`, `recon/phases.py`.
- **Does**: Emits zero-mismatch summary rows for identical quarters and for unchanged
  group x quarter pairs so aggregates sum correctly, then rolls up cross-quarter
  totals and flags noisy columns.
- **Artifacts**:
  - `recon_column_summary_by_quarter` (appends zero-fill rows).
  - `recon_column_summary_all_quarters` (per-column totals across quarters).
  - `recon_noisy_columns` (columns with `mismatch_pct >= cfg.noisy_column_threshold`;
    only if any qualify).
  - Also emits `recon_row_status_counts` rows for identical/one-sided quarters.

### `cleanup`

- **Code**: `cleanup_temp_tables_for_run()`, `mark_run_complete()`, `recon/runner.py`.
- **Does**: Drops all `recon_tmp_*_<run_id>` temp tables (when
  `cfg.cleanup_tmp_tables`) and updates the metadata row to `COMPLETED` (or `FAILED`
  on error).
- **Artifacts**: Removes temp tables (`left_hashes`, `right_hashes`, `changed_keys`,
  `nonnull_counts`); finalizes `recon_run_metadata` (`completed_at`, `status`).

---

## Step -> Artifact matrix

| Step | Final artifacts | Temp artifacts |
|------|-----------------|----------------|
| `setup` | `recon_run_metadata` (RUNNING) | — |
| `phase0_quarter_screening` | `recon_quarter_checksums` | — |
| `phase2b_nonnull_counts` | — | `recon_tmp_nonnull_counts` |
| `phase1_hash_extraction` | — | `recon_tmp_left_hashes`, `recon_tmp_right_hashes` |
| `phase2_key_recon` | `recon_row_status_counts`, `recon_row_status_detail`* | `recon_tmp_changed_keys` |
| `phase3_group_triage` | — (in-memory) | — |
| `phase4_targeted_comparison` | `recon_column_summary_by_quarter`, `recon_mismatch_sample`, `recon_mismatch_detail`** | — |
| `phase5_rollups` | `recon_column_summary_all_quarters`, `recon_noisy_columns`***, + zero-fill / `recon_row_status_counts` | — |
| `cleanup` | `recon_run_metadata` (COMPLETED) | drops all temp |

`*` only if `write_row_status_detail`. `**` `mismatch_sample` when `detail_mode` in
{`sample`, `full_direct`}; `mismatch_detail` only `full_direct`. `***` only when noisy
columns exist.
