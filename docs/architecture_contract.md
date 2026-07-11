# Architecture Contract

This document defines the behavioral and structural invariants of the `delta-table-compare` package. These invariants must hold regardless of which engine executes a reconciliation run.

Any implementation change that would violate a contract clause requires an ADR before the change may land.

---

## 1. Engine Parity at the Observable-Output Level

The package exposes a single reconciliation concept. Engine choice is an operational parameter, not a semantic one.

**Contract:** For the same source tables, configuration, and inputs, both the `spark` and `polars` engines must produce identical values for:

- mismatch count per column per quarter
- null-mismatch count per column per quarter
- row status classifications (`matched`, `left_only`, `right_only`) per key per quarter
- tolerance suppression behavior — a value difference within the configured tolerance must be treated as no difference by both engines
- quarter status (`identical` vs. `changed`) — this classification gates the progressive-narrowing optimization and must agree across engines

Internal data structures, intermediate representations, hash bytes, and execution plans are not part of the observable contract and may differ between engines.

---

## 2. Spark as the Reference Implementation

The Spark engine is the production reference. Where a discrepancy exists between Spark and Polars observable outputs, the Spark result is authoritative.

**Contract:** Remediating cross-engine divergence must correct the non-Spark engine. Changes must not alter Spark output values to match Polars. The Spark functional test suite is the regression baseline; it must remain green across all engine-parity work.

---

## 3. Polars Engine Status

Polars is **experimental** until the gates defined in ADR-0002 are satisfied.

**Contract:** Documentation and public interfaces must not represent Polars as a production-equivalent engine until public dispatch, cross-engine parity, Databricks transport validation, and documented deployment gates have all passed. The `engine` configuration field remains accepted to enable testing and phased adoption.

---

## 4. Shared Engine-Independent Normalization Semantics

Hash-based triage and value comparison both depend on how raw column values are serialized and normalized. These rules must not be defined independently per engine.

**Contract:** The normalization specification — including null sentinel representation, string trimming and case folding behavior, float rounding scale, and date/timestamp serialization format — must be defined in a single authoritative location. Each engine must implement and consume that specification. The following configuration flags must have identical effect in both engines:

- `trim_strings_for_hash`
- `lower_strings_for_hash`
- `float_hash_round_scale`

Divergence in normalization behavior between engines is a correctness defect, not a performance trade-off.

---

## 5. Hashing as a Narrowing Optimization

Row-level and group-level hashes are used to identify *candidates* for comparison, not to determine final mismatch status.

**Contract:** A hash match means a row or column group is skipped from detailed comparison. A hash mismatch means the row or group proceeds to Phase 4 value comparison. **The hash result alone does not determine whether a mismatch is reported in the output.** Phase 4 is the authoritative source of mismatch decisions.

This means:

- A false hash collision (two distinct values hash to the same value) causes a missed mismatch — this is a known, acceptable approximation inherent in hash-based triage.
- A hash divergence between engines on the same data causes triage to route different rows to Phase 4, which can produce different output counts — this is a correctness defect and must be eliminated.

---

## 6. Normalization Applies Consistently in Triage and Final Comparison

Normalization rules must be applied at both stages of the pipeline where value semantics matter.

**Contract:** The same normalization rules applied during Phase 1 hashing (triage) must also be applied during Phase 4 value comparison. An engine must not apply `trim_strings_for_hash=True` during hashing but compare raw untrimmed values in Phase 4. Consistent application is required to produce correct mismatch counts.

---

## 7. Public Runner Dispatch Based on Engine Configuration

The single public entry point, `run_reconciliation`, must respect the `engine` field of `ReconcileConfig`.

**Contract:** Calling `run_reconciliation(cfg)` with `cfg.engine="spark"` must execute the Spark engine. Calling it with `cfg.engine="polars"` must execute the Polars engine. The caller must not be required to import engine classes, manually orchestrate phases, or know which engine is active. Quick-start examples for both engines must differ only in configuration and installation requirements.

---

## 8. Stable Output Meaning Across Engines

The output table schemas, column names, and value semantics are part of the public contract.

**Contract:** Both engines must write to the same nine output tables with identical schemas. A `mismatch_count` of 47 in `column_summary_all_quarters` must mean the same thing regardless of which engine produced it. `run_id`, `source_label`, and `status` fields must be present and consistent across all output tables for multi-run filtering and auditing.

---

## 9. No Silent Omission of Explicitly Requested Columns

When a caller explicitly names columns to compare via `all_feature_cols`, every named column must either be included in the reconciliation or produce an error.

**Contract:** A column named in `all_feature_cols` that is absent from one or both source tables must not be silently dropped from the comparison scope. With default configuration (`strict_column_validation=True`), the run must raise an error that names the missing column and identifies which source table it is absent from. A permissive mode (`strict_column_validation=False`) may be supported, but omitted columns must be recorded in the run metadata and logs.

This constraint exists because silent omission can cause a reconciliation result to appear complete when it is not, which is more dangerous than a clear failure.

---

## 10. Auditable Run Status and Cleanup Behavior

Every reconciliation run must be traceable and its artifact lifecycle must be deterministic and documented.

**Contract:**

- Every run must write an initial `RUNNING` row to `run_metadata` at start and update it to `COMPLETED` or `FAILED` at end, regardless of engine or whether an exception occurred.
- Cleanup behavior must be controlled by explicit configuration fields, not implicit assumptions. The default behavior is: clean up temporary tables on success; retain them on failure (see ADR-0003).
- Temporary tables created by a run must include the `run_id` in their name so that artifacts from any specific run — including failed runs — can be identified, reviewed, and dropped manually.
- Both the Spark and Polars engines must implement this lifecycle. An engine that lacks failure handling or cleanup semantics does not satisfy this contract.
