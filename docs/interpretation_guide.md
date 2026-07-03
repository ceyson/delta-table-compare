# Interpretation Guide

How to read and act on reconciliation results — for both technical users and business stakeholders.

---

## For Business Stakeholders

### What does this tool do?

The reconciliation framework compares two versions of the same dataset (e.g., this month's extract vs. last month's) and answers:

1. **How many records changed?** — broken down by quarter
2. **Which columns changed?** — with counts and percentages
3. **How big are the differences?** — maximum absolute difference for numeric columns
4. **Are any columns "noisy"?** — changing in >95% of rows (likely a systematic issue)
5. **Show me examples** — sample rows with before/after values

### Key metrics to look at

| Metric | What it means | Action threshold |
|--------|--------------|-----------------|
| `mismatch_pct` | % of matched rows where this column differs | > 1% warrants investigation |
| `null_mismatch_pct` | % where one side is null and the other isn't | > 0.1% may indicate ETL issues |
| `max_abs_diff` | Largest numeric difference seen | Context-dependent (e.g., $0.01 for currency) |
| Noisy column flag | Column changes in >95% of rows | Likely a computed/timestamp column — exclude or investigate |

### How to read `column_summary_all_quarters`

This table gives you the "big picture" across all time periods:

```sql
SELECT column, mismatch_pct, null_mismatch_pct, max_abs_diff
FROM recon_column_summary_all_quarters
WHERE run_id = '<latest_run_id>'
ORDER BY mismatch_pct DESC
```

- **Top of the list** = columns with the most changes. These need attention.
- **Near zero** = columns that are stable. Good news.
- **Near 100%** = systematic changes (e.g., a recalculated field). Check `noisy_columns`.

### How to read `row_status_counts`

```sql
SELECT quarter_date, row_status, row_count
FROM recon_row_status_counts
WHERE run_id = '<latest_run_id>'
```

| Status | Meaning | Concern level |
|--------|---------|---------------|
| `matched` | Row exists in both tables | Expected (should be ~100%) |
| `left_only` | Row in left but not right | Data loss? Filtering change? |
| `right_only` | Row in right but not left | New data? Duplication? |

### How to investigate specific mismatches

```sql
SELECT *
FROM recon_mismatch_sample
WHERE run_id = '<latest_run_id>'
  AND column = 'premium'
ORDER BY quarter_date
```

This shows you actual before/after values for the column of interest.

---

## For Technical Users

### Understanding the phase timings

Each reconciliation run reports per-phase elapsed time when `collect_timings=True`:

| Phase | What it does | Scales with |
|-------|-------------|-------------|
| `setup` | Schema creation, column resolution, metadata write | Constant |
| `phase0_quarter_screening` | Checksum per quarter | rows × cols |
| `phase2b_nonnull_counts` | Join + nonnull boolean aggregation | rows × cols |
| `phase1_hash_extraction` | Compute per-row, per-group hashes | rows × cols |
| `phase2_key_recon` | Full outer join on keys, classify rows | rows |
| `phase3_group_triage` | Filter changed keys by group match flags | changed_rows × groups |
| `phase4_targeted_comparison` | Read + compare actual values | changed_rows × changed_cols |
| `phase5_rollups` | Aggregate + zero-fill | quarters × cols |
| `table_writes` | Delta I/O (separated from compute) | num_writes × data_size |

### Performance characteristics

**Polars (single-node)**:
- Compute scales linearly with `rows × cols`.
- Table writes are negligible (~20ms regardless of scale).
- Memory-bound: requires ~2× dataset size for the join in phase 2b.
- At typical scale (100K × 200): **~2 seconds**.

**Spark (local mode)**:
- Large fixed overhead (~25s) from JVM startup, codegen, and Delta transaction log.
- Table writes dominate: 50%+ of total time.
- Compute scales sub-linearly due to Spark's parallelism (even on single node).
- At typical scale: **~45 seconds**.

**Spark (Databricks cluster)**:
- Fixed overhead reduced to ~5s (warm JVM, no codegen delay).
- Table writes benefit from distributed storage (Unity Catalog).
- Expected 5–10x faster than local mode for large datasets.
- At typical scale: **~5–10 seconds** (estimated).

### Benchmark Delta tables

Query benchmark results directly:

```python
import polars as pl

# Load results
results = pl.read_delta("benchmarks/results/benchmark_results_delta")

# Pivot: phase timings by scale
results.filter(
    pl.col("engine") == "polars"
).pivot(
    on="n_rows_total",
    index="phase",
    values="elapsed_seconds"
).sort("phase")
```

### Write timing analysis

```python
# Per-table write breakdown
wt = pl.read_delta("benchmarks/results/write_timings_detail_delta")
wt.group_by("operation").agg(
    pl.col("elapsed_seconds").sum(),
    pl.col("row_count").sum(),
)
```

### Interpreting scaling behavior

From the benchmark grid, you can determine:

- **Linear**: If doubling rows doubles time → compute-bound, predictable scaling.
- **Sub-linear**: Spark with AQE may show this for small increases.
- **Super-linear**: Memory pressure or hash collision effects (rare).
- **Constant**: Fixed overhead phases (setup, cleanup, triage for small change sets).

The Polars engine shows near-perfect linear scaling for compute phases. The dominant cost driver is `rows × columns` — the "work unit" count.

### Tolerance configuration

For numeric columns, mismatches are only flagged if `|left - right| > tolerance`:

```python
cfg = ReconcileConfig(
    # Global default
    default_numeric_tolerance=0.01,
    # Per-column overrides
    tolerances={
        "premium": 0.001,
        "calculated_field": 1.0,  # Allow larger swings
    },
)
```

Set tolerances to avoid flagging floating-point noise from different computation paths.

### Multi-run comparison

All output tables include `run_id` and `source_label`. To compare across runs:

```sql
SELECT a.column, a.mismatch_pct AS run1_pct, b.mismatch_pct AS run2_pct
FROM recon_column_summary_all_quarters a
JOIN recon_column_summary_all_quarters b ON a.column = b.column
WHERE a.run_id = 'run_2025_01' AND b.run_id = 'run_2025_02'
ORDER BY ABS(a.mismatch_pct - b.mismatch_pct) DESC
```

This helps identify columns that are degrading (or improving) over time.
