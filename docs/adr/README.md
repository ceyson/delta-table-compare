# Architecture Decision Records

This directory contains Architecture Decision Records (ADRs) for the `delta-table-compare` project.

An ADR captures a significant engineering or product decision, the context in which it was made, the options considered, and the consequences of the chosen direction.

## Index

| ID | Title | Status |
|----|-------|--------|
| [0001](0001-engine-parity.md) | Engine Parity Contract | Accepted |
| [0002](0002-polars-support-status.md) | Polars Support Status | Accepted |
| [0003](0003-failed-run-artifact-policy.md) | Failed-Run Artifact Policy | Accepted |

## Format

Each ADR contains:

- **Title** — short imperative statement of the decision
- **Status** — Accepted, Superseded, or Deprecated
- **Context** — the problem, constraints, and background
- **Decision** — the chosen direction, stated precisely
- **Consequences** — effects of the decision, including trade-offs
- **Alternatives considered** — options that were evaluated and rejected

## Process

ADRs are created when a decision materially affects engine behavior, output contracts, performance characteristics, deployment constraints, or public API semantics.

New ADRs are numbered sequentially. An ADR is superseded, not deleted, when a later decision reverses or replaces it.
