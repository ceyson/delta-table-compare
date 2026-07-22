# Code Review: `delta-table-compare`

## Review Purpose

This document is intended as a formal engineering review of the `delta-table-compare` repository.

The goal is not to redesign the project or introduce new abstractions. The goal is to identify where the repository's public claims, package structure, implementation, documentation, and operational behavior may be inconsistent or insufficiently hardened.

The repository already demonstrates meaningful engineering work. This review focuses on the remaining gaps between:

- a strong internal data engineering project, and
- a polished, trustworthy, public Python package.

---

# Executive Summary

The repository has several substantial strengths:

- A legitimate progressive-narrowing reconciliation strategy
- Clear separation of major processing phases
- Auditable output tables
- Spark- and Polars-oriented implementation work
- Centralized configuration
- Tests, benchmarks, architecture documentation, and a decision log
- Consideration for Databricks, Unity Catalog, and shared-cluster constraints

The project does **not** appear to be meaningless generated code. Its core design addresses a real computational problem.

However, portions of the repository appear to have been polished beyond the maturity of the current implementation. The largest concern is a possible mismatch between the documented multi-engine architecture and the actual public execution path.

The repository currently appears strongest as an internal engineering package and somewhat less complete as a public open-source package.

## Highest-Priority Findings

1. Confirm whether the public reconciliation runner truly supports both Spark and Polars.
2. Correct stale or misleading quick-start instructions.
3. Add continuous integration and package-build verification.
4. Strengthen run identifier generation for concurrent execution.
5. Complete package metadata and licensing.
6. Reframe maximum-scale benchmark projections as rough estimates rather than validated capacity claims.
7. Replace or supplement direct `print()` calls with structured logging.
8. Tighten configuration and requested-column validation.
9. Define cleanup behavior for failed runs.
10. Return a typed result object instead of an unstructured dictionary.

---

# Instructions to the Implementing Engineer

Treat each finding below as an independent code-review item.

For every item:

1. Classify it as:
   - **Accept**
   - **Reject**
   - **Needs Discussion**
2. Explain the reasoning.
3. Do not accept a recommendation merely because it appears in this document.
4. Where rejecting a recommendation, defend the current design with concrete implementation evidence.
5. Where accepting a recommendation:
   - Make the smallest practical change.
   - Preserve existing behavior unless the finding explicitly concerns that behavior.
   - Avoid unrelated refactoring.
   - Avoid renaming files or public APIs without a demonstrated need.
   - Add or update tests for the changed behavior.
   - Update documentation where public behavior changes.

Do not introduce new architectural layers, factories, managers, handlers, registries, or plugin systems unless the existing implementation demonstrably requires them.

Do not rewrite working code solely for stylistic consistency.

---

# Findings

## CR-001: Verify and Unify the Public Multi-Engine Execution Path

**Priority:** Critical  
**Category:** Architecture / Public API

### Observation

The repository presents itself as supporting both Spark and Polars.

The documented primary Spark entry point appears to be:

```python
result = run_reconciliation(cfg)
```

The public wrapper appears to delegate to a runner whose implementation is Spark-specific.

The Polars documentation appears to instantiate an engine directly:

```python
engine = get_engine("polars")
engine.setup(cfg)
```

but does not appear to demonstrate a complete reconciliation run through the same public API.

### Concern

The repository may currently have:

- a turnkey Spark reconciliation runner, and
- a lower-level Polars engine implementation,

rather than one genuinely engine-neutral orchestration path.

If true, this creates a mismatch between the repository's architectural claims and its public interface.

### Required Review

Determine which of the following is accurate:

1. The public runner already dispatches correctly to both engines.
2. The engine abstraction is partially implemented.
3. Spark and Polars intentionally have different orchestration paths.
4. The documentation overstates the current level of engine unification.

### Recommended Resolution

Prefer one public execution interface:

```python
cfg = ReconcileConfig(
    engine="polars",
    ...
)

result = run_reconciliation(cfg)
```

and:

```python
cfg = ReconcileConfig(
    engine="spark",
    ...
)

result = run_reconciliation(cfg)
```

The orchestration should depend on an engine contract rather than import Spark phase functions directly.

A minimal shape may be:

```python
def run_reconciliation(
    cfg: ReconcileConfig,
    *,
    collect_timings: bool = False,
) -> ReconciliationResult:
    engine = get_engine(cfg.engine)
    return engine.run(
        cfg,
        collect_timings=collect_timings,
    )
```

This is illustrative only. Reuse existing structures where possible.

### Acceptance Criteria

- Both engines can complete reconciliation through the same documented public function.
- The quick-start examples for Spark and Polars differ only in configuration and installation requirements.
- Tests verify dispatch to both engines.
- The architecture document accurately describes the implemented design.

---

## CR-002: Remove Stale Repository Paths and Avoid `sys.path` Injection

**Priority:** High  
**Category:** Documentation / Packaging

### Observation

The README appears to reference an older directory or repository name such as:

```python
sys.path.insert(
    0,
    "/Workspace/Repos/<user>/<repo>/spark_reconciliation",
)
```

The current repository is named `delta-table-compare`.

### Concern

This makes the quick start look copied from an earlier project state and undermines the presence of a real `pyproject.toml`.

A packaged project should ordinarily be installed rather than manually added to `sys.path`.

### Recommended Resolution

Document editable installation:

```bash
pip install -e ".[spark]"
```

or:

```bash
pip install -e ".[polars]"
```

Then import normally:

```python
from recon import ReconcileConfig, run_reconciliation
```

For Databricks-specific installation, document the supported workspace or wheel-installation workflow explicitly.

### Acceptance Criteria

- No stale `spark_reconciliation` path remains.
- The README does not require manual `sys.path` modification.
- A fresh clone can be installed and imported using documented commands.
- A CI smoke test verifies installation and import.

---

## CR-003: Add Continuous Integration

**Priority:** High  
**Category:** Testing / Repository Trust

### Observation

The repository contains tests and documents test commands, but no visible continuous-integration workflow was identified during review.

### Concern

Tests that exist but are not automatically executed provide weaker assurance than tests run against each pull request.

A public repository should demonstrate that:

- supported Python versions install successfully,
- package metadata builds,
- pure-Python tests pass,
- Polars tests pass,
- Spark tests pass in a suitable environment,
- formatting and linting remain clean.

### Recommended Resolution

Add GitHub Actions workflows covering:

- Python 3.10, 3.11, and 3.12 where supported
- Pure-Python and Polars tests on Linux
- Polars tests on Windows if Windows support is claimed
- Spark tests on Linux
- Ruff linting
- Ruff formatting checks
- Package build
- Installation smoke test

Avoid an overly complex matrix initially.

### Acceptance Criteria

- Pull requests run automated tests.
- The package builds successfully in CI.
- The README displays a CI status badge.
- CI commands match local developer instructions.

---

## CR-004: Strengthen `run_id` Generation

**Priority:** High  
**Category:** Reliability / Concurrency

### Observation

The default `run_id` appears to use a timestamp with second-level precision:

```python
datetime.now().strftime("%Y%m%d_%H%M%S")
```

Temporary resource names and cleanup behavior rely on this identifier.

### Concern

Two runs starting within the same second may collide.

This may occur in:

- scheduled workflows,
- parallel tests,
- concurrent users,
- retried jobs,
- notebook automation.

### Recommended Resolution

Use a collision-resistant identifier that remains human-readable.

Example:

```python
from datetime import datetime, timezone
from uuid import uuid4

run_id = (
    datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    + "_"
    + uuid4().hex[:12]
)
```

### Acceptance Criteria

- Concurrent runs cannot reasonably generate the same identifier.
- Tests validate uniqueness across rapid repeated creation.
- Existing output filtering by `run_id` continues to work.
- Documentation does not imply that the ID is only a timestamp.

---

## CR-005: Complete Package Metadata and Licensing

**Priority:** High  
**Category:** Packaging / Open Source Readiness

### Observation

The package metadata appears minimal.

Potential omissions include:

- License
- README metadata
- Authors or maintainers
- Project URLs
- Package classifiers
- Clear supported Python versions

The distribution name `recon` may also be too generic.

An `all` optional-dependency group may refer recursively to the package's own extras.

### Concern

Incomplete metadata reduces installability, discoverability, and legal clarity.

Recursive or self-referential extras may behave unexpectedly and should be verified.

### Recommended Resolution

Complete the `pyproject.toml` metadata.

Illustrative example:

```toml
[project]
name = "delta-table-compare"
version = "0.2.0"
description = "Progressive reconciliation for wide Delta tables"
readme = "README.md"
requires-python = ">=3.10"
license = { text = "MIT" }
authors = [
    { name = "Project Maintainer" }
]
classifiers = [
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.10",
    "Programming Language :: Python :: 3.11",
    "Programming Language :: Python :: 3.12",
]

[project.urls]
Repository = "https://github.com/ceyson/delta-table-compare"
Issues = "https://github.com/ceyson/delta-table-compare/issues"
```

Review the package name independently. The import package may remain `recon` even if the distribution name changes.

### Acceptance Criteria

- The repository includes an explicit license.
- `python -m build` succeeds.
- Wheel and source distribution metadata are correct.
- Extras install without recursive dependency behavior.
- Package and import names are clearly documented.

---

## CR-006: Reframe Maximum-Scale Benchmark Projections

**Priority:** Medium-High  
**Category:** Performance / Documentation

### Observation

The repository includes measured benchmark points and larger-scale projections.

The largest projected scenario appears substantially beyond the largest measured workload in:

- row count,
- column count,
- quarter count,
- and total comparison work.

The projection appears based largely on a regression over a small number of benchmark points.

### Concern

The benchmark section may communicate more precision than the evidence supports.

A value such as `~28 minutes` can be interpreted as a validated capacity estimate even when it is a long-range extrapolation.

Runtime may also depend heavily on:

- change density,
- quarter distribution,
- cluster warmup,
- partitioning,
- file layout,
- executor count,
- memory pressure,
- output detail level,
- hashing batch size,
- and spill behavior.

### Recommended Resolution

Separate benchmark claims into:

1. **Measured results**
2. **Observed scaling trends**
3. **Rough scenario estimates**
4. **Unvalidated maximum-scale projections**

Use ranges rather than single precise values for large extrapolations.

Expand measurements to include intermediate scales and repeated runs.

Suggested additions:

- 250K rows × 500 columns
- 500K rows × 1,000 columns
- 1M rows × 1,000 columns
- Multiple change rates
- Cold and warm Spark sessions
- Three to five repetitions per scenario
- Median, minimum, maximum, and percentile timings

### Acceptance Criteria

- Measured and projected results are clearly separated.
- Projection language is explicitly cautious.
- Model assumptions are documented.
- The benchmark methodology records hardware or cluster configuration.
- Repeated runs are used for newly published benchmark claims.

---

## CR-007: Replace Direct `print()` Usage with Logging

**Priority:** Medium  
**Category:** Observability

### Observation

The runner appears to use direct `print()` calls for execution status and configuration information.

### Concern

Direct printing limits:

- log-level control,
- structured context,
- timestamps,
- integration with job monitoring,
- suppression in library usage,
- and redirection by callers.

Databricks notebook users may still benefit from visible output, but standard logging also appears in notebook output when configured.

### Recommended Resolution

Use a module logger:

```python
import logging

logger = logging.getLogger(__name__)
```

Log major lifecycle events with context such as:

- `run_id`
- engine
- phase
- table names
- elapsed time
- row counts
- changed key counts
- cleanup status

Do not build an elaborate logging framework.

### Acceptance Criteria

- Library code does not depend on direct printing.
- Important lifecycle information remains visible in notebooks.
- Tests can suppress or capture logs.
- Errors include the run identifier and failed phase.

---

## CR-008: Tighten Configuration Validation

**Priority:** Medium  
**Category:** Correctness / Defensive Programming

### Observation

The configuration validates several important fields, but additional invalid states may not be rejected.

Potential cases include:

- `noisy_column_threshold` outside `[0, 1]`
- Negative absolute tolerance
- Negative relative tolerance
- Duplicate key columns
- Duplicate critical columns
- Critical columns also declared as keys
- Empty catalog or schema values
- Empty requested feature lists
- Invalid batch sizes
- Unexpectedly long or unsafe custom prefixes

### Concern

Reconciliation software should fail clearly when its configuration is inconsistent.

Silent correction or delayed Spark failures make troubleshooting harder.

### Recommended Resolution

Expand `ReconcileConfig` validation with explicit and actionable error messages.

Avoid overvalidation where a legitimate use case exists.

### Acceptance Criteria

- Invalid numeric ranges are rejected.
- Duplicate and conflicting columns are handled deliberately.
- Error messages identify the exact invalid field.
- Unit tests cover every added validation rule.

---

## CR-009: Do Not Silently Omit Requested Comparison Columns

**Priority:** Medium-High  
**Category:** Correctness

### Observation

Requested feature columns that do not exist in both datasets may be silently excluded.

### Concern

A misspelled or missing column can disappear from reconciliation without being noticed.

For a comparison framework, silent omission can be more dangerous than failure because the result may appear complete.

### Recommended Resolution

Add explicit requested-column validation.

Preferred default behavior:

- Raise an error when a specifically requested comparison column is absent from either side.

Optional permissive behavior may be supported through configuration:

```python
strict_column_validation: bool = True
```

When permissive mode is used, record omitted columns in:

- logs,
- run metadata,
- and the returned result.

### Acceptance Criteria

- Explicitly requested missing columns cannot disappear silently.
- Missing columns identify whether they are absent from the base, comparison, or both.
- Strict behavior is tested.
- Permissive behavior, if retained, is documented and auditable.

---

## CR-010: Define Cleanup Behavior for Failed Runs

**Priority:** Medium  
**Category:** Operations / Resource Management

### Observation

Successful runs appear to clean up temporary resources.

On failure, temporary tables may remain for debugging.

### Concern

Retaining temporary resources after failure can be useful, but an undocumented policy may lead to:

- orphaned Delta tables,
- storage growth,
- naming clutter,
- and uncertainty about what is safe to delete.

### Recommended Resolution

Make the policy explicit.

Possible configuration:

```python
cleanup_on_success: bool = True
cleanup_on_failure: bool = False
```

Consider also documenting a retention or cleanup utility for abandoned artifacts.

Do not automatically delete failed-run resources if they are valuable for diagnosis unless users can opt out.

### Acceptance Criteria

- Cleanup behavior is documented.
- Success and failure behavior are independently testable.
- Failed-run artifacts are discoverable by `run_id`.
- A safe manual or automated cleanup process exists.

---

## CR-011: Return a Typed Result Object

**Priority:** Medium  
**Category:** Public API / Maintainability

### Observation

The public runner appears to return an unstructured dictionary such as:

```python
dict[str, str]
```

### Concern

A dictionary provides limited discoverability and weak guarantees.

Callers must know string keys in advance, and future additions may become inconsistent.

### Recommended Resolution

Return a typed immutable result object.

Illustrative example:

```python
from dataclasses import dataclass
from typing import Mapping

@dataclass(frozen=True)
class ReconciliationResult:
    run_id: str
    status: str
    output_tables: Mapping[str, str]
    engine: str
    elapsed_seconds: float | None = None
```

Do not overpopulate the result with large DataFrames.

### Acceptance Criteria

- Public return values have documented attributes.
- Static type checking can discover result fields.
- Existing dictionary-style consumers receive a migration path if necessary.
- Tests cover the result object.

---

## CR-012: Align Documentation With Implemented Behavior

**Priority:** High  
**Category:** Documentation Accuracy

### Observation

The repository contains strong documentation, but some sections may describe intended architecture rather than current behavior.

Potential areas include:

- shared orchestration across engines,
- Polars quick-start completeness,
- maximum-scale performance expectations,
- package installation,
- temporary-resource cleanup,
- and public return types.

### Concern

Documentation that outruns implementation damages trust more than incomplete documentation.

### Recommended Resolution

Review every public claim against the current code.

For each major documented capability, verify:

- it exists,
- it has a runnable example,
- it has a test,
- and limitations are stated.

### Acceptance Criteria

- Every quick-start example runs from a fresh install.
- Architecture diagrams reflect actual control flow.
- Limitations are explicit.
- Aspirational features are labeled as planned rather than implemented.

---

# Positive Findings to Preserve

The following qualities should be preserved during remediation.

## Progressive Narrowing

The staged strategy appears to avoid cell-level comparison until earlier screening identifies relevant rows and column groups.

Do not flatten this design into a naïve full outer join over every feature.

## Auditable Outputs

The output model includes run-level traceability and multiple levels of summary and detail.

Preserve:

- `run_id`
- source labels
- quarter-level results
- key-status reporting
- column summaries
- samples
- optional full detail
- noisy-column identification

## Decision Documentation

The decision log records meaningful engineering choices and measured performance improvements.

Continue using it for decisions that materially affect:

- execution strategy,
- output contracts,
- engine behavior,
- compatibility,
- or performance.

Do not use it for trivial formatting changes.

## Databricks and Unity Catalog Awareness

The implementation appears to account for real platform constraints.

Preserve platform-safe quoting, table naming, cleanup discipline, and shared-cluster considerations.

## Test Separation

The separation of pure-Python, Polars, and Spark tests is useful.

Do not force all contributors to install Java and Spark merely to run lightweight tests.

---

# Recommended Implementation Order

## Phase 1: Correctness and Claims

1. Resolve CR-001: public multi-engine runner.
2. Resolve CR-009: missing requested columns.
3. Resolve CR-012: documentation alignment.
4. Correct README installation and quick starts.

## Phase 2: Reliability

5. Resolve run-ID collision risk.
6. Define failed-run cleanup behavior.
7. Tighten configuration validation.
8. Add a typed result object.

## Phase 3: Repository Hardening

9. Add CI.
10. Complete package metadata and licensing.
11. Add package-build and install smoke tests.
12. Introduce standard logging.

## Phase 4: Evidence

13. Expand benchmark coverage.
14. Reframe extrapolations.
15. Publish reproducible benchmark environment details.

---

# Suggested Pull Request Boundaries

Avoid implementing every review item in one large change.

Recommended pull requests:

1. `fix: unify public engine dispatch`
2. `docs: correct installation and engine quick starts`
3. `fix: validate requested reconciliation columns`
4. `fix: generate collision-resistant run identifiers`
5. `feat: define cleanup policy for failed runs`
6. `feat: return typed reconciliation results`
7. `ci: add lint, test, build, and install workflows`
8. `build: complete package metadata and license`
9. `refactor: replace runner prints with standard logging`
10. `docs: revise benchmark methodology and projections`

Each pull request should include focused tests and avoid unrelated file rewrites.

---

# Required Response Format

Respond to this review with a table containing:

| ID | Decision | Reasoning | Proposed Change | Tests | Documentation |
|---|---|---|---|---|---|

After the table, provide:

## Disagreements

Defend every rejected finding with specific references to current code.

## Accepted Changes

List the exact files expected to change for each accepted finding.

## Deferred Work

Identify items that are valid but should not be implemented now.

## Risks

Describe possible regressions introduced by accepted changes.

## Implementation Plan

Provide an ordered plan using small, reviewable pull requests.

Do not produce code until the review disposition has been completed.

---

# Prompt to Give Claude

Use the following prompt together with this file:

> Treat `CODE_REVIEW.md` as a formal review from another senior engineer.
>
> Inspect the repository itself before responding. Do not blindly accept the findings.
>
> For every review item:
>
> 1. Mark it Accept, Reject, or Needs Discussion.
> 2. Cite the relevant files, functions, tests, and documentation.
> 3. Defend the current implementation where the critique is incorrect.
> 4. For accepted items, propose the smallest practical change.
> 5. Identify tests and documentation updates.
>
> Preserve the repository's progressive-narrowing design and avoid unrelated refactoring.
>
> Do not introduce new abstraction layers unless you first demonstrate why the existing design cannot support the required behavior.
>
> Do not write or modify code yet. First return the complete review disposition in the format requested by `CODE_REVIEW.md`.