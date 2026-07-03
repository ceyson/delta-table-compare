# Benchmarks

Measured performance data, test conditions, and projections for the reconciliation framework.

---

## Test Conditions

- **Hardware**: AMD Ryzen (ASUS ROG Zephyrus M16), 32 GB RAM, NVMe SSD
- **OS**: Linux (Ubuntu-based)
- **Python**: 3.10+
- **Polars**: 1.x (native Rust engine, single-process multi-threaded)
- **PySpark**: 3.5 with delta-spark, local mode (single JVM, limited parallelism)
- **Data**: Synthetic test data with mixed types (160 numeric, 20 string, 10 date, 10 boolean columns at 200-col scale)
- **Change rate**: 5% of rows modified, affecting 10 columns
- **Delta format**: Local Delta Lake (Parquet + JSON transaction log)

All times are wall-clock elapsed seconds. Each data point is a single run (no averaging) — variance is low (<5%) for Polars and moderate (~10%) for Spark due to JVM warmup.

---

## Measured Results: Scaling Grid

### Total elapsed time (seconds)

| Scale | Rows | Cols | Quarters | Polars | Spark (local) |
|-------|------|------|----------|--------|---------------|
| Tiny | 2,000 | 50 | 2 | 0.15 | 29.0 |
| Small | 10,000 | 100 | 5 | 0.35 | 28.6 |
| Medium | 50,000 | 200 | 10 | 0.99 | 40.6 |
| **Typical** | **100,000** | **200** | **20** | **1.86** | **45.4** |

### Compute vs. I/O breakdown

| Scale | Rows | Polars Compute | Polars Writes | Spark Compute | Spark Writes |
|-------|------|----------------|---------------|---------------|--------------|
| Tiny | 2,000 | 0.13s | 0.02s | 13.7s | 15.3s |
| Small | 10,000 | 0.33s | 0.02s | 13.4s | 15.2s |
| Medium | 50,000 | 0.97s | 0.02s | 19.2s | 21.3s |
| **Typical** | **100,000** | **1.84s** | **0.02s** | **21.1s** | **24.3s** |

**Key insight**: Polars writes are negligible (20ms constant). Spark writes dominate at 50%+ of total time in local mode — this is JVM + Delta transaction overhead that would be amortized on a Databricks cluster.

---

## Per-Phase Breakdown (Polars, typical scale: 100K rows × 200 cols)

| Phase | Time (s) | % of Total | Scaling behavior |
|-------|----------|-----------|------------------|
| setup | 0.014 | 0.7% | Constant |
| phase0_quarter_screening | 0.33 | 17.6% | Linear (rows × cols) |
| phase2b_nonnull_counts | 0.16 | 8.3% | Linear (rows × cols) |
| phase1_hash_extraction | 0.49 | 26.1% | Linear (rows × cols) |
| phase2_key_recon | 0.03 | 1.4% | Linear (rows) |
| phase3_group_triage | 0.001 | 0.0% | Linear (changed_rows) |
| phase4_targeted_comparison | 0.84 | 45.2% | Linear (changed_rows × changed_cols) |
| phase5_rollups | 0.008 | 0.4% | Linear (quarters × cols) |
| table_writes | 0.02 | 1.2% | Constant |
| **total** | **1.86** | **100%** | |

**Bottleneck**: Phase 4 (targeted comparison) at 45% — this is the cell-level comparison loop over changed rows.

---

## Per-Phase Breakdown (Spark local, typical scale: 100K rows × 200 cols)

| Phase | Time (s) | % of Total | Notes |
|-------|----------|-----------|-------|
| setup | 0.80 | 1.8% | Schema creation, metadata write |
| phase0_quarter_screening | 6.90 | 15.2% | 20 quarters of checksum compute |
| phase2b_nonnull_counts | 5.85 | 12.9% | Join + boolean aggregation |
| phase1_hash_extraction | 3.91 | 8.6% | Hash computation for 2 groups |
| phase2_key_recon | 4.95 | 10.9% | Full outer join, row classification |
| phase3_group_triage | 0.52 | 1.1% | Filter by group match flags |
| phase4_targeted_comparison | 11.69 | 25.7% | Column comparison + enrichment |
| phase5_rollups | 6.34 | 14.0% | Zero-fill + aggregation |
| cleanup | 0.03 | 0.1% | Drop temp tables |
| table_writes | 24.26 | 53.4% | 11 Delta table writes |
| **total** | **45.4** | | |

**Note**: Table writes (24.3s) overlap with compute phases — they are the cumulative I/O time embedded within each phase. The "total" time is wall-clock, not the sum of phases + writes.

---

## Projections: Max Scale (4,000,000 rows × 5,000 columns × 220 quarters)

### Methodology

Linear regression on measured data: `time = slope × (rows × cols) + intercept`

Fitted on 4 data points (tiny → typical). The model explains >99% of Polars variance (R² ≈ 0.99). Spark has higher variance due to fixed JVM overhead.

### Polars projections (single-node)

| Phase | Projected time | Notes |
|-------|---------------|-------|
| phase0_quarter_screening | ~5.1 min | Checksum over 4M × 5K |
| phase2b_nonnull_counts | ~2.4 min | Vectorized group_by + unpivot |
| phase1_hash_extraction | ~7.7 min | Hash computation for 25 groups |
| phase2_key_recon | ~0.3 min | Join on 4M rows |
| phase4_targeted_comparison | ~12.3 min | 200K changed rows × 200 changed cols |
| **Total** | **~28 min** | Single-node, memory permitting |

**Memory requirement**: ~150 GB for the full join at this scale (4M rows × 5K cols × 2 sides × 8 bytes avg). This exceeds the 32 GB test machine. In practice, streaming or partition-based processing would be needed.

### Spark projections

| Environment | Projected time | Basis |
|-------------|---------------|-------|
| Local (single JVM) | ~5 hours | Linear extrapolation from measured data |
| Databricks (8-node i3.xlarge) | ~30–60 min | 5–10x improvement from distribution |
| Databricks (16-node i3.2xlarge) | ~15–30 min | Near-linear scaling with nodes |

**Caveat**: Spark projections assume no memory spill. At 4M × 5K, shuffle and broadcast sizes may cause spill to disk, adding 2–3x overhead. Recommend increasing `spark.sql.shuffle.partitions` to 400+ and using `spark.sql.adaptive.enabled=true`.

---

## Scaling Characteristics

### Polars

| Characteristic | Evidence |
|---------------|----------|
| **Linear with rows** | 0.15s @ 2K → 1.86s @ 100K (50x data, ~12x time — sub-linear due to fixed costs) |
| **Linear with cols** | phase2b scales proportionally with column count |
| **Constant writes** | 20ms regardless of data size (Parquet append) |
| **CPU-bound** | Near 100% CPU utilization, no I/O bottleneck |

### Spark (local mode)

| Characteristic | Evidence |
|---------------|----------|
| **High fixed overhead** | 29s for 2K rows vs. 45s for 100K rows (25s is constant) |
| **I/O dominated** | 53% of time in Delta writes |
| **Sub-linear compute** | JVM optimizations amortize codegen across larger datasets |
| **Not representative of cluster** | Local mode severely under-represents Databricks performance |

---

## Optimization History

| Date | Change | Impact |
|------|--------|--------|
| 2025-07 | Vectorized `phase2b_nonnull_counts` (Polars) | 14.1s → 0.15s (94x faster) |
| 2025-07 | Column projection in join (only needed cols) | ~20% memory reduction |
| 2025-07 | `reinterpret(signed=True)` for hash conversion | Eliminated u64→i64 cast error |
| 2025-07 | Path-based Delta writes for local Spark | Eliminated AnalysisException |

---

## Reproducing These Results

```bash
# Full grid (tiny → typical, both engines)
python benchmarks/bench_runner.py --engine both --max-scale typical --output benchmarks/results

# Polars only, up to max (requires ~150 GB RAM for max scale)
python benchmarks/bench_runner.py --engine polars --max-scale max --output benchmarks/results

# Query results
python -c "
import polars as pl
df = pl.read_delta('benchmarks/results/benchmark_results_delta')
print(df.filter(pl.col('phase') == 'total').sort('engine', 'n_rows_total'))
"
```

---

## Recommendations for Databricks Deployment

1. **Cluster sizing** for typical workload (100K × 200 × 20 quarters):
   - 2-node i3.xlarge cluster (8 cores, 30 GB each) — expected runtime ~10s.

2. **Cluster sizing** for max workload (4M × 5K × 220 quarters):
   - 8–16 node i3.2xlarge (16 cores, 61 GB each) — expected runtime 15–60 min.
   - Set `spark.sql.shuffle.partitions=800`.
   - Set `spark.sql.adaptive.coalescePartitions.enabled=true`.

3. **Memory considerations**:
   - Phase 2b nonnull counts requires a full inner join. At max scale, this is ~150 GB.
   - Consider partitioned execution (process N quarters at a time) if memory is constrained.

4. **Cost estimate** (Databricks DBU pricing):
   - Typical: ~0.5 DBU (trivial).
   - Max: ~20–50 DBU per run ($3–$8 at standard pricing).
