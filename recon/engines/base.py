"""
Abstract base protocol for reconciliation engines.

Each engine must implement these methods to provide a complete
reconciliation pipeline. The orchestrator (runner) dispatches to
the active engine.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from ..config import ReconcileConfig


class ReconEngine(ABC):
    """Abstract interface for a reconciliation engine."""

    @abstractmethod
    def setup(self, cfg: ReconcileConfig) -> None:
        """Initialize engine-specific resources (sessions, connections)."""
        ...

    @abstractmethod
    def validate_tables(self, cfg: ReconcileConfig) -> None:
        """Verify source tables exist and required columns are present."""
        ...

    @abstractmethod
    def resolve_compare_cols(self, cfg: ReconcileConfig) -> list[str]:
        """Determine the full set of columns to compare."""
        ...

    @abstractmethod
    def phase0_quarter_screening(
        self, cfg: ReconcileConfig, all_compare_cols: list[str]
    ) -> tuple[list[Any], Any]:
        """Quarter-level checksum screening.

        Returns:
            (changed_quarters, quarter_status_data) — engine-specific status object.
        """
        ...

    @abstractmethod
    def phase1_hash_extraction(
        self,
        cfg: ReconcileConfig,
        changed_quarters: list[Any],
        all_compare_cols: list[str],
        groups: list[list[str]],
    ) -> tuple[Any, Any]:
        """Row-level + group-level hash extraction.

        Returns:
            (left_hashes, right_hashes) — engine-specific hash data references.
        """
        ...

    @abstractmethod
    def phase2_key_recon(
        self,
        cfg: ReconcileConfig,
        left_hashes: Any,
        right_hashes: Any,
        num_groups: int,
    ) -> tuple[Any, Any]:
        """Key reconciliation and row triage.

        Returns:
            (changed_keys, total_matched_per_qtr) — engine-specific references.
        """
        ...

    @abstractmethod
    def phase2b_nonnull_counts(
        self,
        cfg: ReconcileConfig,
        all_compare_cols: list[str],
        quarter_status: Any,
    ) -> Any:
        """Compute per-column nonnull counts across matched rows.

        Returns:
            nonnull_counts reference (engine-specific).
        """
        ...

    @abstractmethod
    def phase3_group_triage(
        self,
        cfg: ReconcileConfig,
        changed_keys: Any,
        num_groups: int,
    ) -> dict[int, Any]:
        """Identify which column groups changed per row.

        Returns:
            Dict mapping group_index -> changed key data for that group.
        """
        ...

    @abstractmethod
    def phase4_targeted_comparison(
        self,
        cfg: ReconcileConfig,
        changed_quarters: list[Any],
        groups: list[list[str]],
        group_changed_keys: dict[int, Any],
        all_compare_cols: list[str],
        total_matched_per_qtr: Any,
        nonnull_counts: Any,
    ) -> None:
        """Targeted column comparison on changed rows × changed groups."""
        ...

    @abstractmethod
    def phase5_rollups(
        self,
        cfg: ReconcileConfig,
        changed_quarters: list[Any],
        quarter_status: Any,
        all_compare_cols: list[str],
        groups: list[list[str]],
        group_changed_keys: dict[int, Any],
        total_matched_per_qtr: Any,
        nonnull_counts: Any,
    ) -> None:
        """Zero-fill, rollups, and noisy-column detection."""
        ...

    @abstractmethod
    def cleanup(self, cfg: ReconcileConfig) -> None:
        """Clean up temporary resources."""
        ...

    @abstractmethod
    def write_run_metadata(
        self, cfg: ReconcileConfig, all_compare_cols: list[str], noncritical_cols: list[str]
    ) -> None:
        """Write initial run metadata."""
        ...

    @abstractmethod
    def mark_run_complete(self, cfg: ReconcileConfig, status: str) -> None:
        """Update run metadata with final status."""
        ...
