# Architecture

## Design Philosophy

The reconciliation framework is designed around three principles:

1. **Progressive narrowing** — Each phase eliminates work for subsequent phases, so only the minimum necessary data is compared at cell level.
2. **Engine abstraction** — The orchestration logic and configuration are shared; only the data-processing primitives differ between engines.
3. **Delta-first output** — All artifacts are Delta tables, making results queryable, appendable, and versioned.

## System Context

```
┌─────────────────────────────────────────────────────────────┐
│                    Databricks / Local                        │
│                                                             │
│  ┌──────────┐     ┌───────────────┐     ┌───────────────┐  │
│  │  Left    │     │    recon       │     │   Output      │  │
│  │  Delta   │────▶│  Framework    │────▶│   Delta       │  │
│  │  Table   │     │               │     │   Tables (9)  │  │
│  └──────────┘     │  ┌─────────┐  │     └───────────────┘  │
│                   │  │ Engine  │  │                         │
│  ┌──────────┐     │  │ Spark / │  │     ┌───────────────┐  │
│  │  Right   │────▶│  │ Polars  │  │────▶│  Benchmark    │  │
│  │  Delta   │     │  └─────────┘  │     │  Results      │  │
│  │  Table   │     └───────────────┘     │  (Delta)      │  │
│  └──────────┘                           └───────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

## Data Flow (Phase Pipeline)

```
Input Tables (Left, Right)
    │
    ▼
Phase 0: Quarter Checksums ──────────────────▶ Skip identical quarters
    │
    ▼ (changed quarters only)
Phase 1: Hash Extraction ───────────────────▶ One row-hash + N group-hashes per row
    │
    ▼
Phase 2: Key Reconciliation ────────────────▶ Classify: matched / left_only / right_only
    │                                          Identify rows with any difference
    ▼
Phase 2b: Nonnull Counts ──────────────────▶ Accurate denominators for mismatch %
    │
    ▼ (changed rows only)
Phase 3: Group Triage ──────────────────────▶ Per-row: which column groups differ?
    │
    ▼ (changed rows × changed groups only)
Phase 4: Targeted Comparison ───────────────▶ Cell-level: which columns differ, by how much?
    │
    ▼
Phase 5: Rollups ───────────────────────────▶ Cross-quarter aggregations, noisy column detection
    │
    ▼
Output Delta Tables (9 tables)
```

See [phases.md](phases.md) for a step-by-step breakdown of each pipeline stage (including the benchmark-reported step names) and a mapping of every step to the Delta artifacts it produces.

## Engine Abstraction

The framework uses a strategy pattern for engine selection:

```python
class ReconEngine:
    """Abstract interface for reconciliation engines."""
    def setup(cfg) -> None
    def validate_tables(cfg) -> None
    def resolve_compare_cols(cfg) -> list[str]
    def write_run_metadata(cfg, ...) -> None
    def phase0_quarter_screening(cfg, ...) -> (changed_quarters, quarter_status)
    def phase1_hash_extraction(cfg, ...) -> (left_hashes, right_hashes)
    def phase2_key_recon(cfg, ...) -> (changed_keys, total_matched_per_qtr)
    def phase2b_nonnull_counts(cfg, ...) -> nonnull_counts
    def phase3_group_triage(cfg, ...) -> group_changed_keys
    def phase4_targeted_comparison(cfg, ...) -> None
    def phase5_rollups(cfg, ...) -> None
    def cleanup(cfg) -> None
    def mark_run_complete(cfg, status) -> None
```

### PySpark Engine

- Uses `pyspark.sql.DataFrame` throughout.
- Writes via `write_delta_append` / `overwrite_delta_table` helpers that handle both Databricks managed tables and local path-based Delta.
- Hash computation uses Spark's `xxhash64` function.
- Caches intermediate results via temp Delta tables (not Spark cache, to avoid executor memory pressure).

### Polars Engine

- Uses `polars.DataFrame` and `polars.LazyFrame` for all compute.
- Hash computation uses Polars' built-in `hash()` with `reinterpret(signed=True)` for consistent i64 representation.
- All operations are vectorized — no Python-level row iteration.
- **I/O adapts to environment** (auto-detected via `DATABRICKS_RUNTIME_VERSION`):

| Operation | Local | Databricks |
|-----------|-------|------------|
| Read | `pl.scan_delta(path)` via deltalake | `spark.table()` → `.toPandas()` → `pl.from_pandas().lazy()` |
| Write | `write_deltalake(path, ...)` via deltalake | `df.to_pandas()` → `spark.createDataFrame()` → `.saveAsTable()` |
| Table refs | Filesystem paths | Unity Catalog FQN (`catalog.schema.table`) |
| Cleanup | `shutil.rmtree()` temp dirs | `DROP TABLE IF EXISTS` via Spark SQL |

This hybrid approach leverages Polars' vectorized compute speed while delegating credential-authenticated storage access to the Spark runtime on Databricks.

## Column Grouping Strategy

With potentially thousands of columns, computing a single all-column hash is expensive and provides poor triage granularity. Instead:

1. Columns are split into groups of `hash_group_size` (default 200).
2. Critical columns are placed first (filling group 0 first).
3. Each row gets one hash per group (`gh_0`, `gh_1`, ...).
4. Phase 3 identifies which groups differ per row.
5. Phase 4 only reads the columns in the affected groups for affected rows.

This reduces I/O by `(1 - change_rate) * (1 - 1/num_groups)` in typical workloads.

## Write Timing Instrumentation

All Delta write operations (both engines) are instrumented via `WriteTimingCollector`:

```python
@dataclass
class WriteTimingRecord:
    table_name: str
    operation: str      # "append" or "overwrite"
    elapsed_seconds: float
    row_count: int = 0
```

This enables separation of compute time from I/O time in benchmarks, which is critical for understanding performance on different storage backends.

## Local vs. Databricks Differences

| Aspect | Local | Databricks |
|--------|-------|------------|
| Spark catalog | `spark-warehouse/` path-based | Unity Catalog |
| Delta writes (Spark engine) | Path-mode + `CREATE TABLE USING DELTA LOCATION` | Managed tables via `saveAsTable` |
| Delta writes (Polars engine) | `deltalake` library (delta-rs) | Polars → Pandas → Spark → `saveAsTable` |
| Delta reads (Polars engine) | `pl.scan_delta()` (delta-rs) | `spark.table()` → Arrow → Polars |
| File I/O | Direct filesystem | No DBFS write access |
| Parallelism | Single-node, limited cores | Multi-node cluster |
| Benchmark output | Delta tables + CSV/JSON | Delta tables only |
| Credentials | N/A (local files) | Handled by Spark runtime (AAD/MSI) |

The `_is_databricks()` helper (present in both `helpers.py` and `polars_engine.py`) detects the environment and adapts I/O strategies accordingly.
