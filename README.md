# Data Reconciliation Framework (`recon`)

A multi-engine, hash-based reconciliation framework for Delta Lake tables supporting both PySpark (Databricks) and Polars (local/CI) execution.

## Overview

The `recon` package compares two versioned datasets (left vs. right) at the quarter level using a 6-phase approach that progressively narrows the scope of comparison — from full-table checksums down to individual cell-level value diffs:

1. **Phase 0** — Quarter-level checksum screening (fast skip for identical quarters)
2. **Phase 1** — Single-scan row-level + group-level hash extraction
3. **Phase 2** — Key reconciliation and row triage (matched / left_only / right_only)
4. **Phase 2b** — Accurate per-column nonnull counts across all matched rows
5. **Phase 3** — Group triage to identify which column groups changed per row
6. **Phase 4** — Targeted column comparison on changed rows × changed groups
7. **Phase 5** — Cross-quarter rollups, zero-fill, noisy-column detection

## Engines

| Engine | Environment | Best For |
|--------|-------------|----------|
| **PySpark** | Databricks (distributed) | Production runs on large clusters |
| **Polars** | Local Linux / CI | Fast iteration, testing, single-node analysis |

Both engines produce identical output schemas and Delta table artifacts.

## Quick Start

### Databricks (PySpark)

```python
import sys
sys.path.insert(0, "/Workspace/Repos/<user>/<repo>/spark_reconciliation")

from recon import ReconcileConfig, run_reconciliation

cfg = ReconcileConfig(
    left_table_name="catalog.schema.left_table",
    right_table_name="catalog.schema.right_table",
    output_catalog="catalog",
    output_schema="recon_schema",
    key_cols=["policy_id", "quarter_date"],
    qtr_col="quarter_date",
    critical_cols=["revenue", "premium"],
    source_label="EDW_PROD",
    detail_mode="sample",
)

outputs = run_reconciliation(cfg)
```

### Local (Polars)

```python
from recon.config import ReconcileConfig
from recon.engines import get_engine

cfg = ReconcileConfig(
    left_table_name="/path/to/left_delta",
    right_table_name="/path/to/right_delta",
    output_catalog="local",
    output_schema="/path/to/output",
    key_cols=["policy_id", "quarter_date"],
    qtr_col="quarter_date",
    critical_cols=["revenue", "premium"],
    engine="polars",
)

engine = get_engine("polars")
# See docs/spec.md for full phase-by-phase usage
```

## Package Structure

```
spark_reconciliation/
├── recon/
│   ├── __init__.py          Public API
│   ├── config.py            ReconcileConfig dataclass
│   ├── helpers.py           Utilities: quoting, I/O, hashing, column grouping
│   ├── phases.py            All phase 0–5 functions (Spark)
│   ├── runner.py            Orchestrator with per-phase timing
│   └── engines/
│       ├── __init__.py      Engine registry
│       └── polars_engine.py Polars implementation of all phases
├── benchmarks/
│   └── bench_runner.py      Scaling + change-rate benchmark grid
├── tests/
│   ├── conftest.py          Shared fixtures (SparkSession, paths)
│   ├── data_generator.py    Synthetic test data with injected differences
│   └── test_recon/          Unit and integration tests
├── docs/
│   ├── architecture.md      Design and data flow
│   ├── spec.md              Configuration and output schemas
│   ├── decision_log.md      Key design decisions
│   ├── interpretation_guide.md  How to read results
│   └── benchmarks.md        Performance data and projections
├── requirements-dev.txt     Development dependencies
├── setup_env.sh             Environment setup script
└── README.md                This file
```

## Output Tables

The reconciliation produces **9 persistent Delta tables** (all prefixed `recon_`):

| Table | Description |
|-------|-------------|
| `run_metadata` | One row per run with config snapshot and status |
| `quarter_checksums` | Per-quarter checksum comparison results |
| `row_status_counts` | Per-quarter matched/left_only/right_only counts |
| `row_status_detail` | Per-row status (opt-in) |
| `column_summary_by_quarter` | Per-column, per-quarter mismatch statistics |
| `column_summary_all_quarters` | Per-column totals across all quarters |
| `mismatch_sample` | Capped sample of mismatched values |
| `mismatch_detail` | Full mismatch detail (opt-in via `detail_mode="full_direct"`) |
| `noisy_columns` | Columns exceeding the noisy threshold |

All tables include `run_id` and `source_label` for multi-run/multi-source filtering.

## Benchmarks (Summary)

Measured on single-node Linux (AMD Ryzen, 32 GB RAM), 5% change rate:

| Scale | Rows | Cols | Polars | Spark (local) |
|-------|------|------|--------|---------------|
| Tiny | 2,000 | 50 | 0.15s | 29s |
| Small | 10,000 | 100 | 0.35s | 29s |
| Medium | 50,000 | 200 | 1.0s | 41s |
| **Typical** | **100,000** | **200** | **1.9s** | **45s** |
| **Max** (projected) | **4,000,000** | **5,000** | **~28 min** | **~5 hr (local)** |

See [docs/benchmarks.md](docs/benchmarks.md) for full phase breakdown, projections, and Databricks estimates.

## Development

```bash
# Setup virtual environment
bash setup_env.sh

# Activate
source .venv/bin/activate

# Run tests
pytest tests/ -v --timeout=120

# Run benchmarks (tiny → typical, both engines)
python benchmarks/bench_runner.py --engine both --max-scale typical
```

## Documentation

- [Architecture](docs/architecture.md) — Design, data flow, engine abstraction
- [Specification](docs/spec.md) — Configuration reference, output schemas, phase contracts
- [Decision Log](docs/decision_log.md) — Key design decisions and rationale
- [Interpretation Guide](docs/interpretation_guide.md) — How to read and act on results
- [Benchmarks](docs/benchmarks.md) — Measured performance, conditions, projections

## Requirements

- **Python**: >= 3.10
- **PySpark engine**: PySpark >= 3.4, delta-spark (bundled on Databricks)
- **Polars engine**: polars >= 1.0, deltalake >= 0.18
- **Testing**: pytest >= 7.0, pyspark (local SparkSession with Delta)
