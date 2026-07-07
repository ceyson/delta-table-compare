"""
Databricks production benchmark module.

Benchmarks Polars vs Spark reconciliation engines on real production Delta tables
with a configurable grid of quarter counts. Designed to run inside a Databricks
notebook and produce professional HTML tables + seaborn plots.

Usage (in a Databricks notebook):
    from benchmarks.databricks_benchmark import run_benchmark_grid, report_results

    results = run_benchmark_grid(
        source_table="catalog.schema.production_table",
        output_catalog="catalog",
        output_schema="recon_benchmarks",
        qtr_col="quarter_date",
        key_cols=["policy_id", "quarter_date"],
        quarter_grid=[4, 8, 12, 20],
        engines=["polars", "spark"],
    )
    report_results(results)
"""

from __future__ import annotations

import random
import string
import time
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Optional

import polars as pl


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkResult:
    """Single benchmark measurement."""

    timestamp: str
    engine: str
    n_quarters: int
    n_rows_total: int
    n_columns: int
    change_rate: float
    phase: str  # "total", "phase0", "phase1", etc.
    elapsed_seconds: float
    status: str = "success"
    error: Optional[str] = None
    notes: str = ""


# ---------------------------------------------------------------------------
# Data preparation
# ---------------------------------------------------------------------------


def _get_spark():
    """Get the active SparkSession (must be on Databricks or have one running)."""
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


def _inject_differences_spark(
    spark,
    source_table_fqn: str,
    target_table_fqn: str,
    change_rate: float = 0.05,
    change_cols: Optional[list[str]] = None,
    seed: int = 123,
) -> None:
    """Read a Delta table via Spark, inject constant offsets, write as new table.

    Mutates `change_rate` fraction of rows in the specified numeric columns
    by adding a fixed offset (+10 for floats, +5 for integers).  Uses a single
    ``select`` instead of chained ``withColumn`` calls to avoid quadratic
    Catalyst plan growth on wide tables.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    df = spark.table(source_table_fqn)

    if change_cols is None:
        change_cols = [
            f.name
            for f in df.schema.fields
            if isinstance(f.dataType, (T.DoubleType, T.FloatType, T.IntegerType, T.LongType))
            and f.name not in ("policy_id", "quarter_date")
        ]

    change_col_set = set(change_cols)
    schema_fields = {f.name: f.dataType for f in df.schema.fields}

    # Deterministic row-level mutation flag via hash
    rng = random.Random(seed)
    salt = rng.randint(0, 2**31)
    mutate_flag = (
        F.abs(F.hash(F.concat_ws("|", *[F.col(c).cast("string") for c in df.columns[:3]])) + F.lit(salt))
        % F.lit(int(1.0 / change_rate))
    ) == F.lit(0)

    # Build all column expressions in one pass (no chained withColumn)
    select_exprs = []
    for col_name in df.columns:
        c = F.col(f"`{col_name}`")
        if col_name in change_col_set:
            dt = schema_fields[col_name]
            if isinstance(dt, (T.DoubleType, T.FloatType)):
                expr = F.when(mutate_flag, c + F.lit(10.0)).otherwise(c)
            elif isinstance(dt, (T.IntegerType, T.LongType)):
                expr = F.when(mutate_flag, c + F.lit(5).cast(dt)).otherwise(c)
            else:
                expr = c
            select_exprs.append(expr.alias(col_name))
        else:
            select_exprs.append(c)

    df.select(*select_exprs).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(target_table_fqn)


def prepare_benchmark_data(
    source_table: str,
    output_catalog: str,
    output_schema: str,
    qtr_col: str,
    n_quarters: int,
    change_rate: float = 0.05,
    change_cols: Optional[list[str]] = None,
    change_cols_count: int = 10,
    critical_cols: Optional[list[str]] = None,
    seed: int = 42,
) -> dict:
    """Subset production data to N quarters, inject differences, write to benchmark schema.

    Args:
        source_table: Fully-qualified source table name.
        output_catalog: Catalog for benchmark tables.
        output_schema: Schema for benchmark tables.
        qtr_col: Quarter column name (numeric YYYYMMDD format).
        n_quarters: Number of distinct quarters to use.
        change_rate: Fraction of rows to mutate.
        change_cols: Explicit columns to mutate (None = auto-detect numeric).
        change_cols_count: Max number of columns to mutate if change_cols is None.
        critical_cols: Subset of columns to reconcile (None = all feature cols).
        seed: Random seed for reproducibility.

    Returns:
        Dict with keys: left_table, right_table, n_rows, n_cols, critical_cols, quarters_used.
    """
    spark = _get_spark()

    # Ensure output schema exists
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_catalog}.{output_schema}")

    # Get distinct quarters, sorted descending (most recent first)
    quarters_df = spark.table(source_table).select(qtr_col).distinct().orderBy(
        qtr_col, ascending=False
    )
    all_quarters = [row[0] for row in quarters_df.collect()]

    if n_quarters > len(all_quarters):
        print(
            f"  WARNING: Requested {n_quarters} quarters but only {len(all_quarters)} available. "
            f"Using all {len(all_quarters)}."
        )
        n_quarters = len(all_quarters)

    selected_quarters = sorted(all_quarters[:n_quarters])
    print(f"  Selected {n_quarters} quarters: {selected_quarters[0]} .. {selected_quarters[-1]}")

    # Subset source data
    from pyspark.sql import functions as F

    subset_df = spark.table(source_table).filter(F.col(qtr_col).isin(selected_quarters))

    left_fqn = f"{output_catalog}.{output_schema}.bench_left_{n_quarters}q"
    subset_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        left_fqn
    )

    n_rows = spark.table(left_fqn).count()
    all_cols = [c for c in spark.table(left_fqn).columns if c not in ("policy_id", qtr_col)]
    n_cols = len(all_cols)
    print(f"  Left table: {left_fqn} ({n_rows:,} rows, {n_cols} feature cols)")

    # Determine columns to mutate
    if change_cols is None:
        from pyspark.sql import types as T

        schema = spark.table(left_fqn).schema
        numeric_cols = [
            f.name
            for f in schema.fields
            if isinstance(f.dataType, (T.DoubleType, T.FloatType, T.IntegerType, T.LongType))
            and f.name not in ("policy_id", qtr_col)
        ]
        change_cols = numeric_cols[:change_cols_count]

    # Determine critical cols for reconciliation
    if critical_cols is None:
        critical_cols = change_cols

    # Inject differences
    right_fqn = f"{output_catalog}.{output_schema}.bench_right_{n_quarters}q"
    _inject_differences_spark(
        spark=spark,
        source_table_fqn=left_fqn,
        target_table_fqn=right_fqn,
        change_rate=change_rate,
        change_cols=change_cols,
        seed=seed,
    )
    print(f"  Right table: {right_fqn} (injected {change_rate:.0%} change in {len(change_cols)} cols)")

    return {
        "left_table": left_fqn,
        "right_table": right_fqn,
        "n_rows": n_rows,
        "n_cols": n_cols,
        "critical_cols": critical_cols,
        "all_feature_cols": all_cols,
        "quarters_used": selected_quarters,
    }


# ---------------------------------------------------------------------------
# Single benchmark run
# ---------------------------------------------------------------------------


def run_single_benchmark(
    left_table: str,
    right_table: str,
    engine_name: str,
    key_cols: list[str],
    qtr_col: str,
    critical_cols: list[str],
    all_feature_cols: list[str],
    output_catalog: str,
    output_schema: str,
    n_quarters: int,
    n_rows: int,
    n_cols: int,
    change_rate: float,
    detail_mode: str = "sample",
    compare_all_columns: bool = True,
    hash_group_size: int = 100,
    comparison_batch_size: int = 200,
) -> list[BenchmarkResult]:
    """Run a full reconciliation and return per-phase timing results."""
    from recon.config import ReconcileConfig
    from recon.engines import get_engine
    from recon.helpers import build_column_groups

    timestamp = datetime.now().isoformat()
    results: list[BenchmarkResult] = []
    run_id = f"bench_{engine_name}_{n_quarters}q_{int(time.time())}"

    def _result(phase: str, elapsed: float, notes: str = "") -> BenchmarkResult:
        return BenchmarkResult(
            timestamp=timestamp,
            engine=engine_name,
            n_quarters=n_quarters,
            n_rows_total=n_rows,
            n_columns=n_cols,
            change_rate=change_rate,
            phase=phase,
            elapsed_seconds=elapsed,
            notes=notes,
        )

    try:
        cfg = ReconcileConfig(
            left_table_name=left_table,
            right_table_name=right_table,
            output_catalog=output_catalog,
            output_schema=output_schema,
            key_cols=key_cols,
            qtr_col=qtr_col,
            critical_cols=critical_cols,
            all_feature_cols=all_feature_cols,
            compare_all_columns=compare_all_columns,
            run_id=run_id,
            source_label="BENCHMARK",
            engine=engine_name,
            detail_mode=detail_mode,
            sample_per_column=5,
            cleanup_tmp_tables=True,
            hash_group_size=hash_group_size,
            comparison_batch_size=comparison_batch_size,
        )

        engine = get_engine(engine_name)

        # Setup
        t0 = time.perf_counter()
        engine.setup(cfg)
        engine.validate_tables(cfg)
        all_compare_cols = engine.resolve_compare_cols(cfg)
        n_cols = len(all_compare_cols)
        noncritical_cols = [c for c in all_compare_cols if c not in set(cfg.critical_cols)]
        groups = build_column_groups(all_compare_cols, list(cfg.critical_cols), cfg.hash_group_size)
        engine.write_run_metadata(cfg, all_compare_cols, noncritical_cols)
        results.append(_result("setup", time.perf_counter() - t0))

        # Phase 0
        t0 = time.perf_counter()
        changed_quarters, quarter_status = engine.phase0_quarter_screening(cfg, all_compare_cols)
        results.append(_result("phase0_quarter_screening", time.perf_counter() - t0,
                               f"{len(changed_quarters)} changed"))

        # Phase 2b (nonnull counts)
        t0 = time.perf_counter()
        nonnull_counts = engine.phase2b_nonnull_counts(cfg, all_compare_cols, quarter_status)
        results.append(_result("phase2b_nonnull_counts", time.perf_counter() - t0))

        if changed_quarters:
            # Phase 1
            t0 = time.perf_counter()
            left_hashes, right_hashes = engine.phase1_hash_extraction(
                cfg, changed_quarters, all_compare_cols, groups
            )
            results.append(_result("phase1_hash_extraction", time.perf_counter() - t0,
                                   f"{len(groups)} groups"))

            # Phase 2
            t0 = time.perf_counter()
            changed_keys, total_matched_per_qtr = engine.phase2_key_recon(
                cfg, left_hashes, right_hashes, len(groups)
            )
            # Polars returns a DataFrame (has .height); Spark returns a table name string
            if hasattr(changed_keys, "height"):
                ck_count = changed_keys.height
            elif isinstance(changed_keys, str):
                ck_count = _get_spark().table(changed_keys).count()
            else:
                ck_count = "?"
            results.append(_result("phase2_key_recon", time.perf_counter() - t0,
                                   f"{ck_count} changed keys"))

            # Phase 3
            t0 = time.perf_counter()
            group_changed_keys = engine.phase3_group_triage(cfg, changed_keys, len(groups))
            results.append(_result("phase3_group_triage", time.perf_counter() - t0))

            # Phase 4
            t0 = time.perf_counter()
            engine.phase4_targeted_comparison(
                cfg, changed_quarters, groups, group_changed_keys,
                all_compare_cols, total_matched_per_qtr, nonnull_counts,
            )
            results.append(_result("phase4_targeted_comparison", time.perf_counter() - t0))

            # Phase 5
            t0 = time.perf_counter()
            engine.phase5_rollups(
                cfg, changed_quarters, quarter_status, all_compare_cols,
                groups, group_changed_keys, total_matched_per_qtr, nonnull_counts,
            )
            results.append(_result("phase5_rollups", time.perf_counter() - t0))
        else:
            # No changes — still run phase5 for zero-fill
            t0 = time.perf_counter()
            engine.phase5_rollups(
                cfg, [], quarter_status, all_compare_cols,
                groups, {}, None, nonnull_counts,
            )
            results.append(_result("phase5_rollups", time.perf_counter() - t0))

        # Cleanup
        t0 = time.perf_counter()
        if cfg.cleanup_tmp_tables:
            engine.cleanup(cfg)
        engine.mark_run_complete(cfg, "COMPLETED")
        results.append(_result("cleanup", time.perf_counter() - t0))

        # Total
        total_seconds = sum(r.elapsed_seconds for r in results)
        results.append(_result("total", total_seconds))

        print(f"    [{engine_name}] {n_quarters}q | {n_rows:,} rows | {n_cols} cols | "
              f"total={total_seconds:.2f}s")

    except Exception as e:
        results.append(_result("total", 0.0, notes=""))
        results[-1].status = "error"
        results[-1].error = str(e)[:500]
        print(f"    [{engine_name}] FAILED: {e}")

    return results


# ---------------------------------------------------------------------------
# Grid runner
# ---------------------------------------------------------------------------


def run_benchmark_grid(
    source_table: str,
    output_catalog: str,
    output_schema: str,
    qtr_col: str,
    key_cols: list[str],
    quarter_grid: list[int] = None,
    critical_cols: Optional[list[str]] = None,
    change_rate: float = 0.05,
    change_cols_count: int = 10,
    engines: list[str] = None,
    detail_mode: str = "sample",
    seed: int = 42,
    compare_all_columns: bool = True,
    hash_group_size: int = 100,
    comparison_batch_size: int = 200,
) -> pl.DataFrame:
    """Run the benchmark grid: quarter_grid x engines.

    Args:
        source_table: FQN of the production Delta table.
        output_catalog: Catalog for benchmark artifacts.
        output_schema: Schema for benchmark artifacts.
        qtr_col: Quarter column (numeric YYYYMMDD).
        key_cols: Key columns for reconciliation.
        quarter_grid: List of quarter counts to benchmark (default [4, 8, 12, 20]).
        critical_cols: Columns to reconcile (None = auto-detect).
        change_rate: Fraction of rows to mutate.
        change_cols_count: Number of columns to mutate.
        engines: Engines to benchmark (default ["polars", "spark"]).
        detail_mode: Reconciliation detail mode.
        seed: Random seed.
        compare_all_columns: When True, compare all columns. When False,
            compare only critical_cols (fast, focused mode).
        hash_group_size: Columns per hash group (larger = fewer groups = fewer
            Phase 4 jobs). Default 100.
        comparison_batch_size: Columns per Phase 4 comparison batch (larger =
            fewer Spark/Polars jobs). Default 200.

    Returns:
        Polars DataFrame with all benchmark results.
    """
    if quarter_grid is None:
        quarter_grid = [4, 8, 12, 20]
    if engines is None:
        engines = ["polars", "spark"]

    print("=" * 70)
    print("DATABRICKS PRODUCTION BENCHMARK")
    print(f"Source: {source_table}")
    print(f"Grid: {quarter_grid} quarters x {engines} engines")
    print(f"Change rate: {change_rate:.0%}, detail_mode: {detail_mode}, compare_all_columns: {compare_all_columns}")
    print("=" * 70)

    all_results: list[BenchmarkResult] = []

    for n_q in quarter_grid:
        print(f"\n--- {n_q} quarters ---")

        # Prepare data (once per quarter count, reused across engines)
        prep_info = prepare_benchmark_data(
            source_table=source_table,
            output_catalog=output_catalog,
            output_schema=output_schema,
            qtr_col=qtr_col,
            n_quarters=n_q,
            change_rate=change_rate,
            change_cols=None,
            change_cols_count=change_cols_count,
            critical_cols=critical_cols,
            seed=seed,
        )

        for eng in engines:
            results = run_single_benchmark(
                left_table=prep_info["left_table"],
                right_table=prep_info["right_table"],
                engine_name=eng,
                key_cols=key_cols,
                qtr_col=qtr_col,
                critical_cols=prep_info["critical_cols"],
                all_feature_cols=prep_info["all_feature_cols"],
                output_catalog=output_catalog,
                output_schema=output_schema,
                n_quarters=n_q,
                n_rows=prep_info["n_rows"],
                n_cols=prep_info["n_cols"],
                change_rate=change_rate,
                detail_mode=detail_mode,
                compare_all_columns=compare_all_columns,
                hash_group_size=hash_group_size,
                comparison_batch_size=comparison_batch_size,
            )
            all_results.extend(results)

    # Convert to Polars DataFrame
    results_df = pl.DataFrame([asdict(r) for r in all_results])

    # Persist to Delta
    _persist_results(results_df, output_catalog, output_schema)

    print(f"\n{'=' * 70}")
    print(f"Benchmark complete. {len(all_results)} measurements collected.")
    print(f"{'=' * 70}")

    return results_df


def _persist_results(results_df: pl.DataFrame, output_catalog: str, output_schema: str) -> None:
    """Write benchmark results to a Delta table for historical tracking."""
    try:
        spark = _get_spark()
        pandas_df = results_df.to_pandas()
        spark_df = spark.createDataFrame(pandas_df)
        table_name = f"{output_catalog}.{output_schema}.benchmark_results"
        spark_df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
            table_name
        )
        print(f"  Results persisted to: {table_name}")
    except Exception as e:
        print(f"  WARNING: Could not persist results to Delta: {e}")


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------


def report_results(results_df: pl.DataFrame, max_scale_rows: Optional[int] = None) -> None:
    """Generate professional HTML tables and seaborn plots from benchmark results.

    Args:
        results_df: DataFrame from run_benchmark_grid().
        max_scale_rows: If provided, project scaling to this row count.
    """
    _display_summary_table(results_df)
    _display_phase_breakdown(results_df)
    _display_scaling_projection(results_df, max_scale_rows)
    _display_plots(results_df, max_scale_rows)


def _display_summary_table(results_df: pl.DataFrame) -> None:
    """Render a summary comparison table via displayHTML."""
    totals = results_df.filter(pl.col("phase") == "total")

    if totals.height == 0:
        print("No total-phase results to display.")
        return

    # Build summary rows
    rows_html = []
    for row in totals.sort(["n_quarters", "engine"]).iter_rows(named=True):
        status_color = "#2d8a4e" if row["status"] == "success" else "#c0392b"
        rows_html.append(
            f"<tr>"
            f"<td style='text-align:left'>{row['engine']}</td>"
            f"<td style='text-align:right'>{row['n_quarters']}</td>"
            f"<td style='text-align:right'>{row['n_rows_total']:,}</td>"
            f"<td style='text-align:right'>{row['n_columns']}</td>"
            f"<td style='text-align:right'>{row['change_rate']:.0%}</td>"
            f"<td style='text-align:right;font-weight:bold'>{row['elapsed_seconds']:.3f}s</td>"
            f"<td style='text-align:left;color:{status_color}'>{row['status']}</td>"
            f"</tr>"
        )

    # Compute speedup ratios
    speedup_html = _compute_speedup_rows(totals)

    html = f"""
    <style>
        .bench-table {{ border-collapse: collapse; font-family: 'Segoe UI', sans-serif; font-size: 13px; width: 100%; }}
        .bench-table th {{ background: #1a1a2e; color: #eee; padding: 8px 12px; text-align: left; }}
        .bench-table td {{ padding: 6px 12px; border-bottom: 1px solid #ddd; }}
        .bench-table tr:nth-child(even) {{ background: #f8f9fa; }}
        .bench-table tr:hover {{ background: #e8f4fd; }}
        .section-title {{ font-family: 'Segoe UI', sans-serif; font-size: 16px; font-weight: 600; margin: 16px 0 8px 0; }}
    </style>
    <p class="section-title">Benchmark Summary</p>
    <table class="bench-table">
        <thead>
            <tr><th style='text-align:left'>Engine</th><th style='text-align:right'>Quarters</th><th style='text-align:right'>Rows</th><th style='text-align:right'>Cols</th><th style='text-align:right'>Change%</th><th style='text-align:right'>Total Time</th><th style='text-align:left'>Status</th></tr>
        </thead>
        <tbody>{''.join(rows_html)}</tbody>
    </table>
    {speedup_html}
    """

    try:
        # Databricks displayHTML
        from IPython.display import display, HTML
        display(HTML(html))
    except Exception:
        print(html)


def _compute_speedup_rows(totals: pl.DataFrame) -> str:
    """Compute Polars vs Spark speedup per quarter count."""
    engines = totals["engine"].unique().to_list()
    if "polars" not in engines or "spark" not in engines:
        return ""

    polars_times = totals.filter(pl.col("engine") == "polars").sort("n_quarters")
    spark_times = totals.filter(pl.col("engine") == "spark").sort("n_quarters")

    if polars_times.height == 0 or spark_times.height == 0:
        return ""

    rows = []
    for p_row in polars_times.iter_rows(named=True):
        n_q = p_row["n_quarters"]
        s_row = spark_times.filter(pl.col("n_quarters") == n_q)
        if s_row.height > 0:
            spark_secs = s_row["elapsed_seconds"][0]
            polars_secs = p_row["elapsed_seconds"]
            if polars_secs > 0:
                ratio = spark_secs / polars_secs
                faster = "Polars" if ratio > 1 else "Spark"
                color = "#2d8a4e" if ratio > 1 else "#c0392b"
                rows.append(
                    f"<tr><td style='text-align:right'>{n_q}</td>"
                    f"<td style='text-align:right'>{polars_secs:.3f}s</td>"
                    f"<td style='text-align:right'>{spark_secs:.3f}s</td>"
                    f"<td style='text-align:right;color:{color};font-weight:bold'>{ratio:.1f}x</td>"
                    f"<td style='text-align:left;color:{color}'>{faster} faster</td></tr>"
                )

    if not rows:
        return ""

    return f"""
    <p class="section-title" style="margin-top:20px">Engine Comparison (Spark / Polars ratio)</p>
    <table class="bench-table">
        <thead><tr><th style='text-align:right'>Quarters</th><th style='text-align:right'>Polars</th><th style='text-align:right'>Spark</th><th style='text-align:right'>Ratio</th><th style='text-align:left'>Winner</th></tr></thead>
        <tbody>{''.join(rows)}</tbody>
    </table>
    """


def _display_phase_breakdown(results_df: pl.DataFrame) -> None:
    """Render per-phase timing breakdown for the largest grid point."""
    phase_order = [
        "setup", "phase0_quarter_screening", "phase2b_nonnull_counts",
        "phase1_hash_extraction", "phase2_key_recon", "phase3_group_triage",
        "phase4_targeted_comparison", "phase5_rollups", "cleanup",
    ]

    # Use largest quarter count
    max_q = results_df["n_quarters"].max()
    subset = results_df.filter(
        (pl.col("n_quarters") == max_q) & pl.col("phase").is_in(phase_order)
    ).sort(["engine", "phase"])

    if subset.height == 0:
        return

    rows_html = []
    for row in subset.iter_rows(named=True):
        pct = ""
        total_row = results_df.filter(
            (pl.col("n_quarters") == max_q)
            & (pl.col("engine") == row["engine"])
            & (pl.col("phase") == "total")
        )
        if total_row.height > 0 and total_row["elapsed_seconds"][0] > 0:
            pct = f"{row['elapsed_seconds'] / total_row['elapsed_seconds'][0] * 100:.1f}%"

        rows_html.append(
            f"<tr>"
            f"<td style='text-align:left'>{row['engine']}</td>"
            f"<td style='text-align:left'><code>{row['phase']}</code></td>"
            f"<td style='text-align:right'>{row['elapsed_seconds']:.4f}s</td>"
            f"<td style='text-align:right;color:#666'>{pct}</td>"
            f"<td style='text-align:left;color:#888'>{row['notes']}</td>"
            f"</tr>"
        )

    html = f"""
    <p class="section-title" style="margin-top:20px">Phase Breakdown ({max_q} quarters)</p>
    <table class="bench-table">
        <thead><tr><th style='text-align:left'>Engine</th><th style='text-align:left'>Phase</th><th style='text-align:right'>Time</th><th style='text-align:right'>% of Total</th><th style='text-align:left'>Notes</th></tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
    </table>
    """

    try:
        from IPython.display import display, HTML
        display(HTML(html))
    except Exception:
        print(html)


def _display_scaling_projection(
    results_df: pl.DataFrame, max_scale_rows: Optional[int] = None
) -> None:
    """Fit linear model and project to max scale."""
    import numpy as np

    totals = results_df.filter(
        (pl.col("phase") == "total") & (pl.col("status") == "success")
    )

    if totals.height < 2:
        return

    engines = totals["engine"].unique().to_list()
    rows_html = []

    for eng in sorted(engines):
        eng_data = totals.filter(pl.col("engine") == eng).sort("n_rows_total")
        if eng_data.height < 2:
            continue

        x = eng_data["n_rows_total"].to_numpy().astype(float)
        y = eng_data["elapsed_seconds"].to_numpy().astype(float)

        # Linear fit: y = slope * x + intercept
        slope, intercept = np.polyfit(x, y, 1)
        y_pred = slope * x + intercept
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0.0

        projected = ""
        if max_scale_rows is not None:
            proj_time = slope * max_scale_rows + intercept
            if proj_time > 3600:
                projected = f"{proj_time / 3600:.1f} hr"
            elif proj_time > 60:
                projected = f"{proj_time / 60:.1f} min"
            else:
                projected = f"{proj_time:.1f}s"

        rows_html.append(
            f"<tr>"
            f"<td style='text-align:left'>{eng}</td>"
            f"<td style='text-align:right'>{slope * 1e6:.4f}</td>"
            f"<td style='text-align:right'>{intercept:.2f}s</td>"
            f"<td style='text-align:right'>{r_squared:.4f}</td>"
            f"<td style='text-align:right;font-weight:bold'>{projected}</td>"
            f"</tr>"
        )

    if not rows_html:
        return

    proj_header = f"<th style='text-align:right'>Projected ({max_scale_rows:,} rows)</th>" if max_scale_rows else "<th style='text-align:right'>Projected</th>"

    html = f"""
    <p class="section-title" style="margin-top:20px">Scaling Projection (linear fit: time = slope &times; rows + intercept)</p>
    <table class="bench-table">
        <thead><tr><th style='text-align:left'>Engine</th><th style='text-align:right'>Slope (s/Mrow)</th><th style='text-align:right'>Intercept</th><th style='text-align:right'>R&sup2;</th>{proj_header}</tr></thead>
        <tbody>{''.join(rows_html)}</tbody>
    </table>
    """

    try:
        from IPython.display import display, HTML
        display(HTML(html))
    except Exception:
        print(html)


def _display_plots(results_df: pl.DataFrame, max_scale_rows: Optional[int] = None) -> None:
    """Render seaborn scaling + phase breakdown plots."""
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns
        import numpy as np
    except ImportError:
        print("  Plots skipped: matplotlib/seaborn not available.")
        return

    totals = results_df.filter(
        (pl.col("phase") == "total") & (pl.col("status") == "success")
    )

    if totals.height < 3:
        # Not enough points for a meaningful plot
        return

    sns.set_theme(style="whitegrid", context="notebook", palette="deep")

    # --- Scaling line plot ---
    fig, ax = plt.subplots(1, 1, figsize=(9, 5))

    plot_df = totals.select(["engine", "n_rows_total", "elapsed_seconds"]).to_pandas()
    sns.lineplot(
        data=plot_df,
        x="n_rows_total",
        y="elapsed_seconds",
        hue="engine",
        style="engine",
        markers=True,
        markersize=8,
        linewidth=2,
        ax=ax,
    )

    # Projection extrapolation
    if max_scale_rows is not None:
        for eng in plot_df["engine"].unique():
            eng_data = plot_df[plot_df["engine"] == eng].sort_values("n_rows_total")
            if len(eng_data) >= 2:
                x = eng_data["n_rows_total"].values.astype(float)
                y = eng_data["elapsed_seconds"].values.astype(float)
                slope, intercept = np.polyfit(x, y, 1)
                x_ext = np.array([x[-1], max_scale_rows])
                y_ext = slope * x_ext + intercept
                ax.plot(x_ext, y_ext, "--", alpha=0.4, linewidth=1.5)

    ax.set_xlabel("Total Rows", fontsize=11)
    ax.set_ylabel("Elapsed Seconds", fontsize=11)
    ax.set_title("Reconciliation Runtime Scaling", fontsize=13, fontweight="bold")
    ax.legend(title="Engine", fontsize=10)
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, _: f"{x:,.0f}"))
    plt.tight_layout()
    plt.show()

    # --- Phase breakdown bar chart (largest grid point) ---
    phase_order = [
        "setup", "phase0_quarter_screening", "phase2b_nonnull_counts",
        "phase1_hash_extraction", "phase2_key_recon", "phase3_group_triage",
        "phase4_targeted_comparison", "phase5_rollups", "cleanup",
    ]

    max_q = results_df["n_quarters"].max()
    phase_data = results_df.filter(
        (pl.col("n_quarters") == max_q) & pl.col("phase").is_in(phase_order)
    )

    if phase_data.height > 0:
        fig2, ax2 = plt.subplots(1, 1, figsize=(10, 5))
        phase_pd = phase_data.select(["engine", "phase", "elapsed_seconds"]).to_pandas()

        sns.barplot(
            data=phase_pd,
            x="phase",
            y="elapsed_seconds",
            hue="engine",
            ax=ax2,
        )
        ax2.set_xlabel("")
        ax2.set_ylabel("Elapsed Seconds", fontsize=11)
        ax2.set_title(f"Phase Breakdown ({max_q} quarters)", fontsize=13, fontweight="bold")
        ax2.tick_params(axis="x", rotation=35, labelsize=9)
        for label in ax2.get_xticklabels():
            label.set_ha("right")
        ax2.legend(title="Engine", fontsize=10)
        plt.tight_layout()
        plt.show()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------


def _quote_id(name: str) -> str:
    """Backtick-quote an identifier, stripping existing quotes first."""
    stripped = name.strip()
    if len(stripped) >= 2 and stripped[0] == stripped[-1] and stripped[0] in ("`", "'", '"'):
        stripped = stripped[1:-1]
    return f"`{stripped}`"


def cleanup_benchmark_tables(output_catalog: str, output_schema: str) -> None:
    """Drop all bench_left_*, bench_right_*, and benchmark_results tables."""
    spark = _get_spark()
    db = f"{_quote_id(output_catalog)}.{_quote_id(output_schema)}"

    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    dropped = 0

    for row in tables:
        table_name = row["tableName"]
        if (
            table_name.startswith("bench_left_")
            or table_name.startswith("bench_right_")
            or table_name == "benchmark_results"
        ):
            fqn = f"{db}.`{table_name}`"
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            dropped += 1

    print(f"Dropped {dropped} benchmark tables from {db}")


def cleanup_recon_tables(output_catalog: str, output_schema: str) -> None:
    """Drop all recon_* output tables from schema. Use before re-running after schema changes."""
    spark = _get_spark()
    db = f"{_quote_id(output_catalog)}.{_quote_id(output_schema)}"

    tables = spark.sql(f"SHOW TABLES IN {db}").collect()
    dropped = 0

    for row in tables:
        table_name = row["tableName"]
        if table_name.startswith("recon_") or table_name.startswith("bench_"):
            fqn = f"{db}.`{table_name}`"
            spark.sql(f"DROP TABLE IF EXISTS {fqn}")
            dropped += 1

    print(f"Dropped {dropped} tables (recon_* + bench_*) from {db}")
