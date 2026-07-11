# ADR-0002: Polars Support Status

**Status:** Accepted  
**Date:** 2026-07-10

---

## Context

The repository documents Polars as a supported engine and includes a working implementation covering all reconciliation phases. However, as of this decision:

- `run_reconciliation` ignores `cfg.engine` and always executes the Spark path. Polars is unreachable through the public API.
- The Polars engine diverges from Spark in observable reconciliation outcomes (see ADR-0001).
- The Databricks I/O path (`_read_delta_via_spark`, `_write_via_spark`) has no off-cluster test coverage. Behavior on a live cluster with Unity Catalog permissions, shared sessions, and Arrow transport has not been validated.
- No performance envelope for Polars on Databricks has been measured. Existing benchmark projections are derived from local `pl.scan_delta` runs and do not represent Databricks conditions.

Presenting Polars as a production-ready engine under these conditions would misrepresent the state of the software to users.

---

## Decision

**Polars is experimental status until the following gates pass:**

1. **Public dispatch** — `run_reconciliation(cfg)` correctly routes to the Polars engine when `cfg.engine="polars"`.
2. **Cross-engine parity** — the full parity test suite passes, demonstrating identical observable outputs for both engines on the same inputs (see ADR-0001).
3. **Databricks transport validation** — the Spark-to-Arrow-to-Polars read path and the Polars-to-Spark write path have been exercised on a real Databricks cluster with the target Unity Catalog configuration, and type consistency is confirmed.
4. **Documented deployment gates** — runtime behavior, memory requirements, and operational constraints are documented based on measured cluster evidence rather than local projections.

Until all four gates pass, documentation must label Polars as **experimental**. The Polars engine may remain in the repository and may be used by informed developers, but it must not be presented as equivalent to the Spark engine in the README, quick-start guides, or benchmark documentation.

The Polars production performance envelope will be defined from measured Databricks results. No hard row, column, or quarter limits will be published until that evidence exists. Documentation should state that Spark is the production reference engine and that Polars performance depends on workload shape and Spark-to-Arrow transport characteristics.

---

## Consequences

- README and all public documentation must label Polars as experimental until the gates are satisfied.
- Polars quick-start examples must be clearly marked experimental and must demonstrate the public dispatch path once CR-001 is resolved.
- The `engine` field in `ReconcileConfig` remains accepted to enable testing and forward compatibility.
- Users who choose Polars in the experimental period do so with explicit awareness of the current limitations.
- Once all gates pass, this ADR should be superseded by one that promotes Polars to supported status.

---

## Alternatives Considered

**Remove Polars from the repository.** Rejected. The implementation is substantive and provides real value at narrow scale. The issue is documentation accuracy, not the engine itself.

**Promote Polars to supported immediately.** Rejected. Public dispatch is broken, parity is unproven, and Databricks transport is untested. Presenting this as production-ready would be inaccurate.

**No status label, leave it ambiguous.** Rejected. Ambiguity on a reconciliation tool is more harmful than an explicit experimental label.
