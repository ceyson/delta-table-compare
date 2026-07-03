from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Mapping, Optional, Sequence


@dataclass
class ReconcileConfig:
    left_table_name: str
    right_table_name: str
    output_catalog: str
    output_schema: str
    key_cols: Sequence[str]
    qtr_col: str
    critical_cols: Sequence[str]
    all_feature_cols: Optional[Sequence[str]] = None
    noncritical_cols: Optional[Sequence[str]] = None
    run_id: Optional[str] = None

    # Optional label for filtering output tables (e.g. warehouse name, environment).
    source_label: Optional[str] = None

    left_label: str = "old_value"
    right_label: str = "new_value"
    tolerances: Mapping[str, float] = field(default_factory=dict)
    default_numeric_tolerance: float = 0.0

    hash_group_size: int = 100
    sample_per_column: int = 10

    # summary     -> summaries only; no mismatch sample/detail
    # sample      -> summaries + capped mismatch samples
    # full_direct -> summaries + samples + full direct mismatch detail
    detail_mode: str = "sample"

    # Write one row per key/quarter to recon_row_status_detail.
    write_row_status_detail: bool = False

    # Drop temp tables created by this run after successful completion.
    cleanup_tmp_tables: bool = True

    # Column batch size for Phase 4 targeted comparison.
    comparison_batch_size: int = 200

    # Threshold (0.0–1.0) above which a column is flagged as suspected systematic/noisy.
    noisy_column_threshold: float = 0.95

    temp_prefix: str = "recon_tmp"
    final_prefix: str = "recon"

    # Engine selection: "spark" or "polars"
    engine: str = "spark"

    # Hash normalization options.
    trim_strings_for_hash: bool = False
    lower_strings_for_hash: bool = False
    float_hash_round_scale: Optional[int] = None

    def __post_init__(self) -> None:
        if not self.left_table_name:
            raise ValueError("left_table_name is required.")
        if not self.right_table_name:
            raise ValueError("right_table_name is required.")
        if not self.output_catalog:
            raise ValueError("output_catalog is required.")
        if not self.output_schema:
            raise ValueError("output_schema is required.")
        if not self.key_cols:
            raise ValueError("key_cols must contain at least one column.")
        if self.qtr_col not in self.key_cols:
            raise ValueError("qtr_col should be part of key_cols.")
        if not self.critical_cols:
            raise ValueError("critical_cols must contain at least one column.")
        if self.left_label == self.right_label:
            raise ValueError("left_label and right_label must be different.")
        if self.hash_group_size <= 0:
            raise ValueError("hash_group_size must be positive.")
        if self.sample_per_column <= 0:
            raise ValueError("sample_per_column must be positive.")
        if self.detail_mode not in {"summary", "sample", "full_direct"}:
            raise ValueError("detail_mode must be one of: summary, sample, full_direct.")
        if self.engine not in {"spark", "polars"}:
            raise ValueError("engine must be one of: spark, polars.")
        if self.comparison_batch_size <= 0:
            raise ValueError("comparison_batch_size must be positive.")
        if self.run_id is None or not str(self.run_id).strip():
            self.run_id = datetime.now().strftime("%Y%m%d_%H%M%S")
