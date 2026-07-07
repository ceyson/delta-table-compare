# Databricks notebook source
# MAGIC %md
# MAGIC # Production Benchmark: Polars vs Spark Reconciliation
# MAGIC
# MAGIC Benchmarks the reconciliation framework on real production data using a grid
# MAGIC of quarter counts and both engines. Produces timing comparisons, phase breakdowns,
# MAGIC scaling projections, and seaborn visualizations.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration
# MAGIC
# MAGIC Edit the cell below with your production table details.

# COMMAND ----------

# Configuration — edit these values
SOURCE_TABLE = "your_catalog.your_schema.your_production_table"
OUTPUT_CATALOG = "your_catalog"
OUTPUT_SCHEMA = "recon_benchmarks"

# Quarter column (numeric YYYYMMDD format, e.g. 20260331)
QTR_COL = "quarter_date"
KEY_COLS = ["policy_id", "quarter_date"]

# Grid of quarter counts to benchmark
QUARTER_GRID = [4, 8, 12, 20]

# Engines to compare
ENGINES = ["polars", "spark"]

# Difference injection settings
CHANGE_RATE = 0.05           # 5% of rows mutated
CHANGE_COLS_COUNT = 10       # Number of numeric columns to perturb

# Critical columns — the must-compare subset.
# When COMPARE_ALL_COLUMNS is False, ONLY these columns are reconciled.
# When True, all columns are compared but critical_cols are prioritized.
# (None = auto-detect numeric columns)
CRITICAL_COLS = None

# When True: compare all ~4,900 columns (full profile).
# When False: compare only CRITICAL_COLS (fast, focused mode).
COMPARE_ALL_COLUMNS = True

# Optional: project scaling to this many rows (for forecasting table)
MAX_SCALE_ROWS = 4_000_000

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Install / Verify Dependencies

# COMMAND ----------

# Polars should already be installed on DBR 14+. If not:
# %pip install polars>=1.0

import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt
print(f"polars={pl.__version__}, seaborn={sns.__version__}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run Benchmark Grid

# COMMAND ----------

import sys, os
# Ensure project root is importable
repo_root = os.path.dirname(os.path.dirname(os.path.abspath(".")))
# If running from the Workspace repo, adjust:
# repo_root = "/Workspace/Repos/<user>/<repo>/delta_table_compare"
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)

from benchmarks.databricks_benchmark import run_benchmark_grid, report_results, cleanup_benchmark_tables, cleanup_recon_tables

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3a. Clean up stale tables (run once after schema changes)
# MAGIC
# MAGIC Drops all `recon_*` and `bench_*` tables. Required when column types
# MAGIC have changed between code versions.

# COMMAND ----------

# Uncomment the line below to drop all recon/bench tables before a fresh run:
# cleanup_recon_tables(OUTPUT_CATALOG, OUTPUT_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Run Benchmark Grid

# COMMAND ----------

results = run_benchmark_grid(
    source_table=SOURCE_TABLE,
    output_catalog=OUTPUT_CATALOG,
    output_schema=OUTPUT_SCHEMA,
    qtr_col=QTR_COL,
    key_cols=KEY_COLS,
    quarter_grid=QUARTER_GRID,
    critical_cols=CRITICAL_COLS,
    change_rate=CHANGE_RATE,
    change_cols_count=CHANGE_COLS_COUNT,
    engines=ENGINES,
    compare_all_columns=COMPARE_ALL_COLUMNS,
    seed=42,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Results & Visualizations

# COMMAND ----------

report_results(results, max_scale_rows=MAX_SCALE_ROWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Raw Results (optional)
# MAGIC
# MAGIC The full results DataFrame for further analysis.

# COMMAND ----------

display(results.to_pandas())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Critical-Only Benchmark (optional)
# MAGIC
# MAGIC Re-run with `compare_all_columns=False` to benchmark only the critical
# MAGIC columns. Useful for comparing full-profile vs focused-mode performance.

# COMMAND ----------

# results_critical = run_benchmark_grid(
#     source_table=SOURCE_TABLE,
#     output_catalog=OUTPUT_CATALOG,
#     output_schema=OUTPUT_SCHEMA,
#     qtr_col=QTR_COL,
#     key_cols=KEY_COLS,
#     quarter_grid=QUARTER_GRID,
#     critical_cols=CRITICAL_COLS,
#     change_rate=CHANGE_RATE,
#     change_cols_count=CHANGE_COLS_COUNT,
#     engines=ENGINES,
#     compare_all_columns=False,
#     seed=42,
# )
# report_results(results_critical, max_scale_rows=MAX_SCALE_ROWS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Cleanup Benchmark Tables (optional)
# MAGIC
# MAGIC Uncomment and run to drop the bench_left_*/bench_right_* staging tables.

# COMMAND ----------

# cleanup_benchmark_tables(OUTPUT_CATALOG, OUTPUT_SCHEMA)
