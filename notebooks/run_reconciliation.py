# Databricks notebook source
# COMMAND ----------
# Thin entry-point notebook for running a reconciliation.
# Configure your table names, key columns, and options below.
# This notebook imports from the `recon` package.

import os
import sys

src_path = os.path.abspath("../src")

sys.path.insert(0, "../src")  # Adjust path if needed based on workspace layout.

if src_path not in sys.path:
    sys.path.insert(0, src_path)

from recon.config import ReconcileConfig
from recon import run_reconciliation
from recon.descriptives import columns_comp
from recon.helpers import get_spark

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# COMMAND ----------
# --- Configuration ---
# Replace these with your actual table names, columns, and options.

left_table_name = "your_catalog.your_source_schema.left_table"
right_table_name = "your_catalog.your_source_schema.right_table"
output_catalog = "your_catalog"
output_schema = "your_recon_schema"
id_col = "policy_id"
qtr_col = "quarter_date"

# Critical columns: always compared first and grouped together.
critical_cols = ["revenue", "premium", "loss_amount"]

# Optional: specify source label for multi-warehouse filtering.
source_label = "TEST"  # e.g., "EDW_PROD", "WH_EAST"

# COMMAND ----------
# --- Build config and run ---

cfg = ReconcileConfig(
    left_table_name=left_table_name,
    right_table_name=right_table_name,
    output_catalog=output_catalog,
    output_schema=output_schema,
    key_cols=[id_col, qtr_col],
    qtr_col=qtr_col,
    critical_cols=critical_cols,
    all_feature_cols=critical_cols,
    source_label=source_label,
    hash_group_size=100,
    comparison_batch_size=200,
    detail_mode="sample",
    sample_per_column=10,
    write_row_status_detail=False,
    cleanup_tmp_tables=True,
    default_numeric_tolerance=0.0,
    tolerances={},
    noisy_column_threshold=0.95,
)

outputs = run_reconciliation(cfg)

# COMMAND ----------
# --- Review outputs ---

print("Run complete. Output tables:")
for key, table in outputs.items():
    print(f"  {key}: {table}")
