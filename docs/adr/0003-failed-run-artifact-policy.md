# ADR-0003: Failed-Run Artifact Policy

**Status:** Accepted  
**Date:** 2026-07-10

---

## Context

The reconciliation framework creates temporary Delta tables during a run (prefixed by `temp_prefix` and scoped to the `run_id`). On successful completion, `cleanup_tmp_tables=True` causes these tables to be dropped. On failure, the current Spark implementation marks the run as `FAILED` in `run_metadata` but leaves temporary tables in place — this behavior is implicit and undocumented.

The Polars engine currently has no run lifecycle wrapper at all: it has no `mark_run_complete` call in the public path, no failure handling, and no cleanup semantics.

Users encountering a failed run have no documented guidance on:

- which temporary tables belong to the failed run,
- whether it is safe to delete them,
- how to find them by `run_id`,
- or whether they will be cleaned up automatically on retry.

---

## Decision

**Temporary artifacts from failed runs are retained by default for diagnosis.**

The policy is:

- `cleanup_on_success` (default `True`) — drop temporary tables after a successful run. Configurable.
- `cleanup_on_failure` (default `False`) — retain temporary tables after a failed run. Configurable to `True` for environments where storage accumulation is a concern.
- All temporary tables are named with the `run_id` so that artifacts from a specific failed run can be identified and reviewed or dropped manually.
- The run's `run_id` and `status=FAILED` are recorded in `run_metadata` regardless of cleanup setting, enabling discovery.
- This policy applies to both the Spark and Polars engines. The Polars engine must acquire equivalent lifecycle handling as part of the public dispatch remediation (CR-001).

Documentation must describe:

- how to find orphaned artifacts by `run_id`,
- the default behavior on success and failure,
- how to configure cleanup,
- and how to safely drop retained artifacts manually.

---

## Consequences

- `ReconcileConfig` gains two explicit cleanup fields (`cleanup_on_success`, `cleanup_on_failure`) replacing or supplementing the existing `cleanup_tmp_tables` field.
- Storage may accumulate in high-failure environments unless `cleanup_on_failure=True` is configured.
- Diagnosis after failure is supported by default — the temporary tables preserve intermediate state.
- The Polars engine's failure path is currently absent; this policy cannot be enforced for Polars until CR-001 delivers a unified run wrapper.
- `cleanup_tmp_tables` compatibility must be handled at implementation time to avoid breaking existing callers.

---

## Alternatives Considered

**Auto-clean on failure.** Rejected. Intermediate state in temporary tables is often the primary diagnostic tool for a failed reconciliation run. Destroying it by default trades operator convenience for loss of debuggability.

**Always retain, no cleanup option.** Rejected. In scheduled or high-frequency environments, retaining every failed run's artifacts indefinitely causes uncontrolled storage growth.

**Retain on failure, mandatory TTL.** Considered but deferred. A time-based retention policy or cleanup utility is a reasonable follow-on but adds scope beyond the current remediation. Document the manual cleanup path for now.
