# Decision Log

Chronological record of key architectural and implementation decisions.

---

## D-001: Multi-engine architecture (Polars + PySpark)

**Date**: 2025-07  
**Context**: The reconciliation framework was originally PySpark-only, tightly coupled to Databricks. Local testing required spinning up a full JVM SparkSession, making iteration slow (25–45s per run even for trivial datasets).

**Decision**: Implement a second engine using Polars for local/CI execution, sharing configuration and output schemas but with independent phase implementations.

**Rationale**:
- Polars provides 10–100x faster execution on single-node for datasets up to millions of rows.
- Same Delta Lake format for I/O (via `deltalake` Python bindings).
- Faster test feedback loop (< 2s for typical workload vs. 45s with Spark local).
- DuckDB was considered but deferred — Polars has better Delta integration and a more natural DataFrame API for this workload.

**Trade-offs**: Two code paths to maintain. Mitigated by shared config, shared test data generator, and identical output schemas.

---

## D-002: Column grouping for hash-based triage

**Date**: 2025-06  
**Context**: With up to 5,000 columns, computing per-column comparisons for all rows is prohibitively expensive.

**Decision**: Split columns into groups of `hash_group_size` (default 200). Compute one hash per group per row. Only compare actual values for groups where the hash differs.

**Rationale**:
- At 5% change rate affecting 10 columns, only 1 of 25 groups needs cell-level comparison.
- Reduces Phase 4 I/O and compute by ~96% in this scenario.
- Critical columns are placed in group 0 first, ensuring they're always checked.

**Trade-offs**: Hash collisions can cause false negatives (two different values hashing the same). Extremely unlikely with xxhash64 / Polars hash.

---

## D-003: Delta tables as primary output (no DBFS file writes)

**Date**: 2025-07  
**Context**: Databricks environments do not permit direct DBFS file writes (CSV, JSON). The original benchmark code wrote CSV/JSON files which would fail in production.

**Decision**: All output — reconciliation results, benchmark results, and write timing detail — is written as Delta tables. CSV/JSON is only written as a local convenience when `_is_databricks()` returns False.

**Rationale**:
- Delta tables are queryable via SQL, versionable, and compatible with Unity Catalog.
- Eliminates DBFS permission failures.
- Benchmark results are machine-readable and can be tracked over time.

---

## D-004: Vectorized nonnull count computation (Polars optimization)

**Date**: 2025-07  
**Context**: The initial Polars `phase2b_nonnull_counts` used a Python nested loop over quarters × columns, filtering the joined DataFrame per iteration. At 20 quarters × 200 columns = 4,000 iterations, this took 14.1s for 100K rows.

**Decision**: Rewrite using a single vectorized `group_by` + `unpivot`:
1. Build boolean expressions `(left.is_not_null() & right.is_not_null())` for all columns.
2. Single `group_by(quarter)` with `.sum()` on all boolean columns.
3. `unpivot()` to long form.

**Result**: 14.1s → 0.15s (94x improvement). The operation is now O(1) Polars operations regardless of column count.

---

## D-005: Per-phase timing instrumentation

**Date**: 2025-07  
**Context**: Stakeholders need to understand where time is spent — especially separating compute from I/O — to make informed decisions about infrastructure sizing.

**Decision**: 
- `run_reconciliation()` accepts `collect_timings=True` and returns phase timings via context-manager instrumentation.
- All Delta write operations are instrumented via `WriteTimingCollector` (both engines).
- Benchmarks emit per-phase `BenchmarkResult` records alongside totals.

**Rationale**: Enables identification of bottlenecks (e.g., Spark local mode spends 50%+ on Delta writes), accurate projections, and Databricks sizing recommendations.

---

## D-006: Lazy SparkSession singleton

**Date**: 2025-07  
**Context**: Multiple modules needed a SparkSession, but creating one eagerly at import time caused failures in Polars-only test paths.

**Decision**: `get_spark()` lazily creates a singleton SparkSession. `set_spark()` allows test injection. All modules call `get_spark()` instead of using a global `spark` variable.

**Rationale**:
- Polars tests don't need JVM startup.
- Tests can inject a pre-configured SparkSession with Delta support.
- Thread-safe singleton pattern.

---

## D-007: Path-based Delta writes for local mode

**Date**: 2025-07  
**Context**: Spark's `saveAsTable` with Delta requires a Hive metastore and fails with `AnalysisException` in local mode for certain operations.

**Decision**: In local mode, write Delta using path-based `save()`, then register tables via `CREATE TABLE ... USING DELTA LOCATION`. On Databricks, use standard `saveAsTable`.

**Rationale**: Allows the full reconciliation pipeline to run locally without a persistent metastore, while maintaining table-name-based queries in the phase logic.

---

## D-008: Benchmark scaling ladder with max-scale control

**Date**: 2025-07  
**Context**: The max workload (4M rows × 5K cols × 220 quarters) would take ~28 minutes with Polars and hours with local Spark. Running this every time during development is impractical.

**Decision**: Define a scaling ladder (tiny → max) and a `--max-scale` CLI flag that defaults to `typical`. Full max-scale runs are opt-in.

**Scaling profiles**:
| Label | Quarters | Columns | Rows |
|-------|----------|---------|------|
| tiny | 2 | 50 | 2K |
| small | 5 | 100 | 10K |
| medium | 10 | 200 | 50K |
| typical | 20 | 200 | 100K |
| large | 50 | 500 | 500K |
| xlarge | 100 | 1,000 | 1M |
| max | 220 | 5,000 | 4M |

---

## D-009: Linear projection model for max-scale estimates

**Date**: 2025-07  
**Context**: Running the full max-scale benchmark takes too long for routine testing. Stakeholders still need runtime estimates.

**Decision**: Fit a linear model `time = slope * (rows * cols) + intercept` from measured data points (tiny → typical). Use this to project max-scale runtimes.

**Validation**: The measured data shows near-linear scaling for Polars compute phases. Spark has a large fixed overhead (~25s) that dominates at small scales, with linear growth beyond that.

**Caveat**: Projections assume single-node execution and no memory pressure. At 4M × 5K, memory requirements (~150 GB for a full join) may require streaming or partitioned processing.
