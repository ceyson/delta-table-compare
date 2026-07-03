"""
Unit tests for recon.config — ReconcileConfig validation logic.
"""

import pytest

from recon.config import ReconcileConfig


class TestReconcileConfigValidation:
    """Test __post_init__ validation rules."""

    def _minimal_cfg(self, **overrides):
        defaults = dict(
            left_table_name="cat.schema.left",
            right_table_name="cat.schema.right",
            output_catalog="cat",
            output_schema="schema",
            key_cols=["id", "quarter_date"],
            qtr_col="quarter_date",
            critical_cols=["revenue"],
            run_id="test_run_001",
        )
        defaults.update(overrides)
        return ReconcileConfig(**defaults)

    def test_valid_config(self):
        cfg = self._minimal_cfg()
        assert cfg.run_id == "test_run_001"

    def test_empty_left_table_raises(self):
        with pytest.raises(ValueError, match="left_table_name"):
            self._minimal_cfg(left_table_name="")

    def test_empty_key_cols_raises(self):
        with pytest.raises(ValueError, match="key_cols"):
            self._minimal_cfg(key_cols=[])

    def test_qtr_col_not_in_key_cols_raises(self):
        with pytest.raises(ValueError, match="qtr_col"):
            self._minimal_cfg(qtr_col="other_col")

    def test_same_labels_raises(self):
        with pytest.raises(ValueError, match="left_label and right_label"):
            self._minimal_cfg(left_label="val", right_label="val")

    def test_invalid_detail_mode_raises(self):
        with pytest.raises(ValueError, match="detail_mode"):
            self._minimal_cfg(detail_mode="invalid")

    def test_auto_generated_run_id(self):
        cfg = self._minimal_cfg(run_id=None)
        assert cfg.run_id is not None
        assert len(cfg.run_id) > 0

    def test_source_label_default_none(self):
        cfg = self._minimal_cfg()
        assert cfg.source_label is None

    def test_source_label_set(self):
        cfg = self._minimal_cfg(source_label="EDW_PROD")
        assert cfg.source_label == "EDW_PROD"
