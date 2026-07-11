# ADR-0001: Engine Parity Contract

**Status:** Accepted  
**Date:** 2026-07-10

---

## Context

The `delta-table-compare` package supports two reconciliation engines: PySpark (`spark`) and Polars (`polars`). Both engines implement the same multi-phase reconciliation pipeline but use entirely different internal mechanisms — distributed Spark jobs vs. single-node vectorized Polars operations.

During architecture review, it was established that the two engines currently diverge in observable behavior:

- The Polars engine uses a different hashing algorithm and ignores hash-normalization flags (`trim_strings_for_hash`, `lower_strings_for_hash`, `float_hash_round_scale`) that govern Spark triage decisions.
- The public `run_reconciliation` function ignores `cfg.engine` and always routes to the Spark implementation.
- No cross-engine parity tests exist.

Because this is a reconciliation tool, engine choice affecting reconciliation outcomes would make results uncitable and untrustworthy across environments.

---

## Decision

**Spark and Polars must produce identical observable reconciliation results for the same inputs and configuration.**

Observable results are defined as:

- Mismatch counts per column and quarter
- Null-mismatch counts
- Row status classifications (matched, left_only, right_only)
- Tolerance behavior — a difference within tolerance must be suppressed identically by both engines
- Quarter status (identical vs. changed) — this gates the narrowing optimization and must agree
- Output table schemas and column semantics

Internal implementation may differ. Spark remains the **reference implementation**. Where the two engines disagree on an output, the Spark result is authoritative and the Polars implementation must be corrected to match.

Hash normalization rules — including null sentinel representation, string trim and case behavior, float rounding, and date/timestamp serialization format — must be specified in a single engine-independent location. Each engine must implement those rules consistently during both the triage phase (hashing) and the final value comparison phase (Phase 4).

The specific hash primitive used internally by each engine (e.g., xxhash64, Polars native hash) is an implementation detail and is intentionally left to the implementing engineer to resolve during remediation. What is required is that the observable outputs converge, not that internal hash bytes are identical.

---

## Consequences

- A cross-engine parity test suite is required and must pass before Polars may be used in production.
- Normalization flags in `ReconcileConfig` must have effect in both engines.
- Changes to normalization rules require updating both engine implementations and the parity tests.
- Polars triage results may change when the normalization fix is applied, since the corrected algorithm may classify different rows as changed. This is expected and correct.
- The Spark engine's outputs are frozen as the reference; remediating Polars must not alter Spark results.

---

## Alternatives Considered

**Polars as approximate engine.** Allow Polars to diverge, documented. Rejected because a reconciliation tool whose two engines disagree on what "changed" cannot be trusted. Every Polars result would require Spark confirmation, eliminating the utility of the engine choice.

**Spark as approximate engine.** Rejected. Spark is the production-deployed, distributed engine with the longer correctness history in this codebase.

**No parity requirement, separate results per engine.** Rejected. The package presents a single reconciliation concept to callers. Engine choice should be an operational decision, not a semantic one.
