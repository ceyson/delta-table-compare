"""
Unit tests for recon.helpers — pure-logic utilities that don't require Spark.

Note: helpers.py imports pyspark at module level, so these tests require
pyspark to be installed.  They are marked ``@pytest.mark.spark`` and will
be auto-skipped in environments without pyspark.
"""

import pytest

pytestmark = pytest.mark.spark

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")


class TestNormalizeIdentifierPart:
    def test_strips_backticks(self):
        from recon.helpers import normalize_identifier_part
        assert normalize_identifier_part("`my_col`") == "my_col"

    def test_strips_double_quotes(self):
        from recon.helpers import normalize_identifier_part
        assert normalize_identifier_part('"my_col"') == "my_col"

    def test_strips_single_quotes(self):
        from recon.helpers import normalize_identifier_part
        assert normalize_identifier_part("'my_col'") == "my_col"

    def test_plain_string_unchanged(self):
        from recon.helpers import normalize_identifier_part
        assert normalize_identifier_part("my_col") == "my_col"

    def test_strips_whitespace(self):
        from recon.helpers import normalize_identifier_part
        assert normalize_identifier_part("  my_col  ") == "my_col"


class TestQuoteName:
    def test_basic(self):
        from recon.helpers import quote_name
        assert quote_name("revenue") == "`revenue`"

    def test_with_backtick_in_name(self):
        from recon.helpers import quote_name
        assert quote_name("col`name") == "`col``name`"


class TestTableFqn:
    def test_three_part_name(self):
        from recon.helpers import table_fqn
        result = table_fqn("my_catalog", "my_schema", "my_table")
        assert result == "`my_catalog`.`my_schema`.`my_table`"


class TestSafeSuffix:
    def test_alphanumeric_unchanged(self):
        from recon.helpers import safe_suffix
        assert safe_suffix("abc_123") == "abc_123"

    def test_special_chars_replaced(self):
        from recon.helpers import safe_suffix
        assert safe_suffix("2025-01-01 12:00") == "2025_01_01_12_00"


class TestChunkList:
    def test_even_split(self):
        from recon.helpers import chunk_list
        assert chunk_list([1, 2, 3, 4], 2) == [[1, 2], [3, 4]]

    def test_uneven_split(self):
        from recon.helpers import chunk_list
        assert chunk_list([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]

    def test_empty_list(self):
        from recon.helpers import chunk_list
        assert chunk_list([], 5) == []


class TestResolveAllCompareColsCriticalOnly:
    """Test that compare_all_columns=False returns only critical_cols."""

    def test_returns_only_critical_cols(self):
        from recon.config import ReconcileConfig
        from recon.helpers import resolve_all_compare_cols

        cfg = ReconcileConfig(
            left_table_name="default.dummy_left",
            right_table_name="default.dummy_right",
            output_catalog="spark_catalog",
            output_schema="default",
            key_cols=["id", "quarter_date"],
            qtr_col="quarter_date",
            critical_cols=["col_b", "col_a", "col_c"],
            compare_all_columns=False,
            run_id="test_critical_only",
        )
        result = resolve_all_compare_cols(cfg)
        assert result == sorted(["col_a", "col_b", "col_c"])

    def test_does_not_read_tables(self):
        """When compare_all_columns=False, no Spark table access is needed."""
        from recon.config import ReconcileConfig
        from recon.helpers import resolve_all_compare_cols

        cfg = ReconcileConfig(
            left_table_name="nonexistent.table.left",
            right_table_name="nonexistent.table.right",
            output_catalog="spark_catalog",
            output_schema="default",
            key_cols=["id", "quarter_date"],
            qtr_col="quarter_date",
            critical_cols=["x", "y"],
            compare_all_columns=False,
            run_id="test_no_table_access",
        )
        # Should NOT raise even though tables don't exist
        result = resolve_all_compare_cols(cfg)
        assert result == ["x", "y"]


class TestBuildColumnGroups:
    def test_critical_first(self):
        from recon.helpers import build_column_groups
        groups = build_column_groups(
            all_compare_cols=["a", "b", "c", "d"],
            critical_cols=["c", "a"],
            hash_group_size=2,
        )
        # Critical cols first in order, then remaining
        assert groups[0] == ["c", "a"]
        assert groups[1] == ["b", "d"]

    def test_group_size(self):
        from recon.helpers import build_column_groups
        cols = [f"col_{i}" for i in range(10)]
        groups = build_column_groups(cols, critical_cols=["col_0"], hash_group_size=3)
        assert all(len(g) <= 3 for g in groups)
        # All columns accounted for
        flat = [c for g in groups for c in g]
        assert sorted(flat) == sorted(cols)
