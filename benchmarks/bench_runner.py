"""
Benchmark runner for the reconciliation framework.

Runs scaling and change-rate benchmarks against both Spark and Polars engines.
Produces results as CSV, JSON, and Delta table artifacts.

Usage:
    python -m benchmarks.bench_runner [--engine spark|polars|both] [--profile scaling|change_rate|all]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import tempfile
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime
from typing import Optional

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


@dataclass
class BenchmarkResult:
    """Single benchmark measurement."""
    timestamp: str
    engine: str
    profile: str
    n_quarters: int
    n_rows_total: int
    n_columns: int
    change_rate: float
    phase: str  # "total", "phase0", "phase1", etc.
    elapsed_seconds: float
    status: str = "success"
    error: Optional[str] = None
    notes: str = ""


@dataclass
class BenchmarkConfig:
    """Configuration for a benchmark run."""
    engine: str = "spark"
    n_quarters: int = 5
    base_rows_per_quarter: int = 5000
    rows_per_quarter_increment: int = 5000
    n_numeric_cols: int = 160
    n_string_cols: int = 20
    n_date_cols: int = 10
    n_bool_cols: int = 10
    change_rate: float = 0.05
    change_cols_count: int = 10
    seed: int = 42


# ---------------------------------------------------------------------------
# Spark benchmark runner
# ---------------------------------------------------------------------------


def _run_spark_benchmark(bench_cfg: BenchmarkConfig, output_dir: str) -> list[BenchmarkResult]:
    """Run a single benchmark using the Spark engine."""
    from pyspark.sql import SparkSession
    from recon.helpers import get_spark, set_spark, get_write_timings
    from recon.config import ReconcileConfig
    from recon.runner import run_reconciliation
    from tests.data_generator import generate_test_data, inject_differences

    spark = get_spark()
    results = []
    timestamp = datetime.now().isoformat()

    data_dir = tempfile.mkdtemp(prefix="bench_data_")

    try:
        # Generate data
        t0 = time.perf_counter()
        left_path, right_path, critical_cols = generate_test_data(
            spark=spark,
            output_path=data_dir,
            n_quarters=bench_cfg.n_quarters,
            base_rows_per_quarter=bench_cfg.base_rows_per_quarter,
            rows_per_quarter_increment=bench_cfg.rows_per_quarter_increment,
            n_numeric_cols=bench_cfg.n_numeric_cols,
            n_string_cols=bench_cfg.n_string_cols,
            n_date_cols=bench_cfg.n_date_cols,
            n_bool_cols=bench_cfg.n_bool_cols,
            seed=bench_cfg.seed,
        )
        t_datagen = time.perf_counter() - t0

        # Calculate total rows
        total_rows = sum(
            bench_cfg.base_rows_per_quarter + i * bench_cfg.rows_per_quarter_increment
            for i in range(bench_cfg.n_quarters)
        )
        n_cols = bench_cfg.n_numeric_cols + bench_cfg.n_string_cols + bench_cfg.n_date_cols + bench_cfg.n_bool_cols

        results.append(BenchmarkResult(
            timestamp=timestamp, engine="spark", profile="data_generation",
            n_quarters=bench_cfg.n_quarters, n_rows_total=total_rows,
            n_columns=n_cols, change_rate=bench_cfg.change_rate,
            phase="data_generation", elapsed_seconds=t_datagen,
        ))

        # Inject differences
        change_cols = critical_cols[:bench_cfg.change_cols_count]
        t0 = time.perf_counter()
        modified_path = inject_differences(
            spark=spark,
            source_path=left_path,
            output_path=os.path.join(data_dir, "right_modified"),
            change_rate=bench_cfg.change_rate,
            change_cols=change_cols,
            seed=bench_cfg.seed + 1,
        )
        t_inject = time.perf_counter() - t0

        results.append(BenchmarkResult(
            timestamp=timestamp, engine="spark", profile="data_injection",
            n_quarters=bench_cfg.n_quarters, n_rows_total=total_rows,
            n_columns=n_cols, change_rate=bench_cfg.change_rate,
            phase="data_injection", elapsed_seconds=t_inject,
        ))

        # Register tables as temp views (local Delta doesn't support saveAsTable overwrite)
        spark.read.format("delta").load(left_path).createOrReplaceTempView("bench_left")
        spark.read.format("delta").load(modified_path).createOrReplaceTempView("bench_right")

        # Create output schema
        spark.sql("CREATE DATABASE IF NOT EXISTS bench_recon_output")

        run_id = f"bench_{bench_cfg.n_quarters}q_{bench_cfg.change_rate}cr_{int(time.time())}"

        cfg = ReconcileConfig(
            left_table_name="bench_left",
            right_table_name="bench_right",
            output_catalog="spark_catalog",
            output_schema="bench_recon_output",
            key_cols=["policy_id", "quarter_date"],
            qtr_col="quarter_date",
            critical_cols=critical_cols,
            all_feature_cols=critical_cols,
            run_id=run_id,
            source_label="BENCHMARK",
            detail_mode="sample",
            sample_per_column=5,
            cleanup_tmp_tables=True,
        )

        # Run reconciliation with per-phase timing
        write_timings = get_write_timings()
        write_timings.enable()

        t0 = time.perf_counter()
        outputs = run_reconciliation(cfg, collect_timings=True)
        t_total = time.perf_counter() - t0

        write_timings.disable()
        total_write_time = write_timings.total_write_seconds()

        def _result(phase, elapsed, notes=""):
            return BenchmarkResult(
                timestamp=timestamp, engine="spark", profile="reconciliation",
                n_quarters=bench_cfg.n_quarters, n_rows_total=total_rows,
                n_columns=n_cols, change_rate=bench_cfg.change_rate,
                phase=phase, elapsed_seconds=elapsed, notes=notes,
            )

        # Emit per-phase results
        for phase_name, phase_secs in outputs.get("phase_timings", []):
            results.append(_result(phase_name, phase_secs))

        results.append(_result("total", t_total))
        results.append(_result("table_writes", total_write_time,
                               f"{len(write_timings.records)} writes"))

        print(f"  Spark benchmark complete: {t_total:.2f}s total, {total_write_time:.2f}s writes ({bench_cfg.n_quarters} quarters, {total_rows:,} rows, {n_cols} cols, {bench_cfg.change_rate:.0%} change)")

    except Exception as e:
        results.append(BenchmarkResult(
            timestamp=timestamp, engine="spark", profile="reconciliation",
            n_quarters=bench_cfg.n_quarters, n_rows_total=0,
            n_columns=0, change_rate=bench_cfg.change_rate,
            phase="total", elapsed_seconds=0.0,
            status="error", error=str(e)[:500],
        ))
        print(f"  Spark benchmark FAILED: {e}")

    finally:
        import shutil
        shutil.rmtree(data_dir, ignore_errors=True)

    return results


# ---------------------------------------------------------------------------
# Polars benchmark runner
# ---------------------------------------------------------------------------


def _run_polars_benchmark(bench_cfg: BenchmarkConfig, output_dir: str) -> list[BenchmarkResult]:
    """Run a single benchmark using the Polars engine."""
    try:
        import polars as pl
        from deltalake import write_deltalake
    except ImportError:
        print("  Polars benchmark skipped: polars/deltalake not installed")
        return []

    from pyspark.sql import SparkSession
    from recon.helpers import get_spark, get_write_timings
    from recon.config import ReconcileConfig
    from recon.engines import get_engine
    from recon.helpers import build_column_groups, resolve_noncritical_cols
    from tests.data_generator import generate_test_data, inject_differences

    spark = get_spark()
    results = []
    timestamp = datetime.now().isoformat()

    data_dir = tempfile.mkdtemp(prefix="bench_polars_data_")

    try:
        # Generate data using Spark (shared generator)
        t0 = time.perf_counter()
        left_path, right_path, critical_cols = generate_test_data(
            spark=spark,
            output_path=data_dir,
            n_quarters=bench_cfg.n_quarters,
            base_rows_per_quarter=bench_cfg.base_rows_per_quarter,
            rows_per_quarter_increment=bench_cfg.rows_per_quarter_increment,
            n_numeric_cols=bench_cfg.n_numeric_cols,
            n_string_cols=bench_cfg.n_string_cols,
            n_date_cols=bench_cfg.n_date_cols,
            n_bool_cols=bench_cfg.n_bool_cols,
            seed=bench_cfg.seed,
        )
        t_datagen = time.perf_counter() - t0

        total_rows = sum(
            bench_cfg.base_rows_per_quarter + i * bench_cfg.rows_per_quarter_increment
            for i in range(bench_cfg.n_quarters)
        )
        n_cols = bench_cfg.n_numeric_cols + bench_cfg.n_string_cols + bench_cfg.n_date_cols + bench_cfg.n_bool_cols

        # Inject differences
        change_cols = critical_cols[:bench_cfg.change_cols_count]
        modified_path = inject_differences(
            spark=spark,
            source_path=left_path,
            output_path=os.path.join(data_dir, "right_modified"),
            change_rate=bench_cfg.change_rate,
            change_cols=change_cols,
            seed=bench_cfg.seed + 1,
        )

        # Polars output directory
        polars_output = os.path.join(data_dir, "polars_output")
        os.makedirs(polars_output, exist_ok=True)

        run_id = f"bench_polars_{bench_cfg.n_quarters}q_{bench_cfg.change_rate}cr_{int(time.time())}"

        cfg = ReconcileConfig(
            left_table_name=left_path,
            right_table_name=modified_path,
            output_catalog="local",
            output_schema=polars_output,
            key_cols=["policy_id", "quarter_date"],
            qtr_col="quarter_date",
            critical_cols=critical_cols,
            all_feature_cols=critical_cols,
            run_id=run_id,
            source_label="BENCHMARK",
            engine="polars",
            detail_mode="sample",
            sample_per_column=5,
            cleanup_tmp_tables=True,
        )

        # Run Polars reconciliation with per-phase timing
        engine = get_engine("polars")
        write_timings = get_write_timings()
        write_timings.enable()

        def _result(phase, elapsed, notes=""):
            return BenchmarkResult(
                timestamp=timestamp, engine="polars", profile="reconciliation",
                n_quarters=bench_cfg.n_quarters, n_rows_total=total_rows,
                n_columns=n_cols, change_rate=bench_cfg.change_rate,
                phase=phase, elapsed_seconds=elapsed, notes=notes,
            )

        t_total_start = time.perf_counter()

        t0 = time.perf_counter()
        engine.setup(cfg)
        engine.validate_tables(cfg)
        all_compare_cols = engine.resolve_compare_cols(cfg)
        noncritical_cols = [c for c in all_compare_cols if c not in set(cfg.critical_cols)]
        groups = build_column_groups(all_compare_cols, list(cfg.critical_cols), cfg.hash_group_size)
        engine.write_run_metadata(cfg, all_compare_cols, noncritical_cols)
        results.append(_result("setup", time.perf_counter() - t0))

        t0 = time.perf_counter()
        changed_quarters, quarter_status = engine.phase0_quarter_screening(cfg, all_compare_cols)
        results.append(_result("phase0_quarter_screening", time.perf_counter() - t0,
                               f"{len(changed_quarters)} changed"))

        t0 = time.perf_counter()
        nonnull_counts = engine.phase2b_nonnull_counts(cfg, all_compare_cols, quarter_status)
        results.append(_result("phase2b_nonnull_counts", time.perf_counter() - t0))

        if changed_quarters:
            t0 = time.perf_counter()
            left_hashes, right_hashes = engine.phase1_hash_extraction(
                cfg, changed_quarters, all_compare_cols, groups
            )
            results.append(_result("phase1_hash_extraction", time.perf_counter() - t0,
                                   f"{len(groups)} groups"))

            t0 = time.perf_counter()
            changed_keys, total_matched_per_qtr = engine.phase2_key_recon(
                cfg, left_hashes, right_hashes, len(groups)
            )
            results.append(_result("phase2_key_recon", time.perf_counter() - t0,
                                   f"{changed_keys.height} changed keys"))

            t0 = time.perf_counter()
            group_changed_keys = engine.phase3_group_triage(cfg, changed_keys, len(groups))
            results.append(_result("phase3_group_triage", time.perf_counter() - t0))

            t0 = time.perf_counter()
            engine.phase4_targeted_comparison(
                cfg, changed_quarters, groups, group_changed_keys,
                all_compare_cols, total_matched_per_qtr, nonnull_counts,
            )
            results.append(_result("phase4_targeted_comparison", time.perf_counter() - t0))

            t0 = time.perf_counter()
            engine.phase5_rollups(
                cfg, changed_quarters, quarter_status, all_compare_cols,
                groups, group_changed_keys, total_matched_per_qtr, nonnull_counts,
            )
            results.append(_result("phase5_rollups", time.perf_counter() - t0))
        else:
            t0 = time.perf_counter()
            engine.phase5_rollups(
                cfg, [], quarter_status, all_compare_cols,
                groups, {}, None, nonnull_counts,
            )
            results.append(_result("phase5_rollups", time.perf_counter() - t0))

        if cfg.cleanup_tmp_tables:
            engine.cleanup(cfg)
        engine.mark_run_complete(cfg, "COMPLETED")

        t_total = time.perf_counter() - t_total_start

        write_timings.disable()
        total_write_time = write_timings.total_write_seconds()

        results.append(_result("total", t_total))
        results.append(_result("table_writes", total_write_time,
                               f"{len(write_timings.records)} writes"))

        print(f"  Polars benchmark complete: {t_total:.2f}s total, {total_write_time:.2f}s writes ({bench_cfg.n_quarters} quarters, {total_rows:,} rows, {n_cols} cols, {bench_cfg.change_rate:.0%} change)")

    except Exception as e:
        results.append(BenchmarkResult(
            timestamp=timestamp, engine="polars", profile="reconciliation",
            n_quarters=bench_cfg.n_quarters, n_rows_total=0,
            n_columns=0, change_rate=bench_cfg.change_rate,
            phase="total", elapsed_seconds=0.0,
            status="error", error=str(e)[:500],
        ))
        print(f"  Polars benchmark FAILED: {e}")

    finally:
        import shutil
        shutil.rmtree(data_dir, ignore_errors=True)

    return results


# ---------------------------------------------------------------------------
# Benchmark grids
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Workload profiles based on real usage:
#   Typical: 20 quarters, 200 columns, 100,000 rows total
#   Max:     220 quarters, 5000 columns, 4,000,000 rows total
# ---------------------------------------------------------------------------

SCALING_PROFILES = [
    # (label, n_quarters, total_rows, n_cols)
    ("tiny",    2,      2_000,     50),
    ("small",   5,     10_000,    100),
    ("medium", 10,     50_000,    200),
    ("typical", 20,   100_000,    200),
    ("large",  50,    500_000,    500),
    ("xlarge", 100,  1_000_000,  1000),
    ("max",    220,  4_000_000,  5000),
]


def run_scaling_benchmarks(engines: list[str], output_dir: str, max_profile: str = "typical") -> list[BenchmarkResult]:
    """Run scaling grid up to the specified profile.

    Profiles: tiny, small, medium, typical, large, xlarge, max.
    Default stops at 'typical' to keep local runs under 10 min.
    """
    print("=" * 60)
    print("SCALING BENCHMARKS")
    print("=" * 60)

    all_results = []
    profile_labels = [p[0] for p in SCALING_PROFILES]
    max_idx = profile_labels.index(max_profile) if max_profile in profile_labels else len(SCALING_PROFILES) - 1

    for label, n_quarters, total_rows, n_cols in SCALING_PROFILES[:max_idx + 1]:
        rows_per_quarter = total_rows // n_quarters
        n_numeric = int(n_cols * 0.8)
        n_string = int(n_cols * 0.1)
        n_date = int(n_cols * 0.05)
        n_bool = max(1, n_cols - n_numeric - n_string - n_date)

        bench_cfg = BenchmarkConfig(
            n_quarters=n_quarters,
            base_rows_per_quarter=rows_per_quarter,
            rows_per_quarter_increment=0,
            n_numeric_cols=n_numeric,
            n_string_cols=n_string,
            n_date_cols=n_date,
            n_bool_cols=n_bool,
            change_rate=0.05,
            change_cols_count=min(10, n_numeric),
        )

        print(f"\n--- [{label}] {n_quarters} quarters, {n_cols} columns, {total_rows:,} rows ---")

        if "spark" in engines:
            results = _run_spark_benchmark(bench_cfg, output_dir)
            all_results.extend(results)

        if "polars" in engines:
            results = _run_polars_benchmark(bench_cfg, output_dir)
            all_results.extend(results)

    return all_results


def run_change_rate_benchmarks(engines: list[str], output_dir: str) -> list[BenchmarkResult]:
    """Run change-rate sensitivity at typical scale (20 qtrs, 200 cols, 100K rows)."""
    print("\n" + "=" * 60)
    print("CHANGE RATE BENCHMARKS (typical scale: 20 qtrs, 200 cols, 100K rows)")
    print("=" * 60)

    all_results = []

    change_rates = [0.0, 0.01, 0.05, 0.10, 0.25, 0.50]

    for cr in change_rates:
        bench_cfg = BenchmarkConfig(
            n_quarters=20,
            base_rows_per_quarter=5000,
            rows_per_quarter_increment=0,
            n_numeric_cols=160,
            n_string_cols=20,
            n_date_cols=10,
            n_bool_cols=10,
            change_rate=cr,
            change_cols_count=20,
        )

        print(f"\n--- Change rate: {cr:.0%} ---")

        if "spark" in engines:
            results = _run_spark_benchmark(bench_cfg, output_dir)
            all_results.extend(results)

        if "polars" in engines:
            results = _run_polars_benchmark(bench_cfg, output_dir)
            all_results.extend(results)

    return all_results


# ---------------------------------------------------------------------------
# Report output
# ---------------------------------------------------------------------------


def write_results(results: list[BenchmarkResult], output_dir: str) -> None:
    """Write benchmark results and write-timing detail to Delta tables.

    All output is Delta-based so it works on both local and Databricks
    (where DBFS/CSV writes are not permitted).  On local runs, CSV/JSON
    are written as an additional convenience.
    """
    from recon.helpers import _is_databricks, get_write_timings

    os.makedirs(output_dir, exist_ok=True)

    # --- Primary output: Delta tables ---

    # 1. Benchmark results Delta
    try:
        import pyarrow as pa
        from deltalake import write_deltalake

        table = pa.Table.from_pylist([asdict(r) for r in results])
        delta_path = os.path.join(output_dir, "benchmark_results_delta")
        write_deltalake(delta_path, table, mode="append")
        print(f"\nDelta written: {delta_path}")
    except ImportError:
        try:
            from recon.helpers import get_spark
            spark = get_spark()
            df = spark.createDataFrame([asdict(r) for r in results])
            df.write.format("delta").mode("append").save(os.path.join(output_dir, "benchmark_results_delta"))
            print(f"\nDelta written (Spark): {os.path.join(output_dir, 'benchmark_results_delta')}")
        except Exception:
            print("\nDelta write skipped (no deltalake or spark available)")

    # 2. Write timings detail Delta
    wt = get_write_timings()
    if wt.records:
        try:
            import pyarrow as pa
            from deltalake import write_deltalake

            detail_table = pa.Table.from_pylist(wt.summary())
            detail_path = os.path.join(output_dir, "write_timings_detail_delta")
            write_deltalake(detail_path, detail_table, mode="append")
            print(f"Write timings Delta: {detail_path} ({len(wt.records)} records)")
        except ImportError:
            try:
                from recon.helpers import get_spark
                spark = get_spark()
                df = spark.createDataFrame(wt.summary())
                df.write.format("delta").mode("append").save(
                    os.path.join(output_dir, "write_timings_detail_delta")
                )
                print(f"Write timings Delta (Spark): {os.path.join(output_dir, 'write_timings_detail_delta')}")
            except Exception:
                pass

    # --- Local-only convenience: CSV/JSON (skipped on Databricks) ---

    if not _is_databricks():
        csv_path = os.path.join(output_dir, "benchmark_results.csv")
        with open(csv_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(asdict(results[0]).keys()))
            writer.writeheader()
            for r in results:
                writer.writerow(asdict(r))
        print(f"CSV written: {csv_path}")

        json_path = os.path.join(output_dir, "benchmark_results.json")
        with open(json_path, "w") as f:
            json.dump([asdict(r) for r in results], f, indent=2)
        print(f"JSON written: {json_path}")

        if wt.records:
            detail_csv_path = os.path.join(output_dir, "write_timings_detail.csv")
            with open(detail_csv_path, "w", newline="") as f:
                writer = csv.DictWriter(f, fieldnames=["table_name", "operation", "elapsed_seconds", "row_count"])
                writer.writeheader()
                for rec in wt.summary():
                    writer.writerow(rec)
            print(f"Write timings CSV: {detail_csv_path}")

    # Print summary table
    from itertools import groupby

    print("\n" + "=" * 110)
    print("BENCHMARK SUMMARY")
    print("=" * 110)
    print(f"{'Engine':<8} {'Quarters':<9} {'Rows':<10} {'Cols':<6} {'Chg%':<6} {'Total(s)':<10} {'Writes(s)':<10} {'Compute(s)':<11} {'Status'}")
    print("-" * 110)
    key_fn = lambda r: (r.engine, r.n_quarters, r.n_rows_total, r.n_columns, r.change_rate)
    sorted_results = sorted(results, key=key_fn)
    for key, group in groupby(sorted_results, key=key_fn):
        items = {r.phase: r for r in group}
        total_r = items.get("total")
        write_r = items.get("table_writes")
        if total_r:
            total_s = total_r.elapsed_seconds
            write_s = write_r.elapsed_seconds if write_r else 0.0
            compute_s = total_s - write_s
            print(f"{total_r.engine:<8} {total_r.n_quarters:<9} {total_r.n_rows_total:<10,} {total_r.n_columns:<6} {total_r.change_rate:<6.0%} {total_s:<10.3f} {write_s:<10.3f} {compute_s:<11.3f} {total_r.status}")

    # Per-phase breakdown
    print("\n" + "=" * 110)
    print("PHASE BREAKDOWN")
    print("=" * 110)
    print(f"{'Engine':<8} {'Rows':<10} {'Cols':<6} {'Phase':<30} {'Elapsed(s)':<12} {'Notes'}")
    print("-" * 110)
    phase_order = ["setup", "phase0_quarter_screening", "phase2b_nonnull_counts",
                   "phase1_hash_extraction", "phase2_key_recon", "phase3_group_triage",
                   "phase4_targeted_comparison", "phase5_rollups", "cleanup",
                   "total", "table_writes"]
    for key, group in groupby(sorted(results, key=key_fn), key=key_fn):
        items_list = list(group)
        # Sort by phase_order
        phase_map = {r.phase: r for r in items_list}
        for ph in phase_order:
            if ph in phase_map:
                r = phase_map[ph]
                print(f"{r.engine:<8} {r.n_rows_total:<10,} {r.n_columns:<6} {r.phase:<30} {r.elapsed_seconds:<12.4f} {r.notes}")
        print()  # blank line between configs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Run reconciliation benchmarks")
    parser.add_argument("--engine", choices=["spark", "polars", "both"], default="both")
    parser.add_argument("--profile", choices=["scaling", "change_rate", "all"], default="all")
    parser.add_argument(
        "--max-scale", default="typical",
        choices=[p[0] for p in SCALING_PROFILES],
        help="Largest scaling profile to run (default: typical = 20 qtrs, 200 cols, 100K rows)",
    )
    parser.add_argument("--output", default="benchmarks/results", help="Output directory for Delta artifacts")
    args = parser.parse_args()

    engines = ["spark", "polars"] if args.engine == "both" else [args.engine]
    output_dir = args.output

    all_results = []

    if args.profile in ("scaling", "all"):
        all_results.extend(run_scaling_benchmarks(engines, output_dir, max_profile=args.max_scale))

    if args.profile in ("change_rate", "all"):
        all_results.extend(run_change_rate_benchmarks(engines, output_dir))

    if all_results:
        write_results(all_results, output_dir)
    else:
        print("No results collected.")


if __name__ == "__main__":
    main()
