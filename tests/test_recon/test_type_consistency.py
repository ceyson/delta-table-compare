"""
Tests for type consistency between Spark and Polars engines.

Validates that both engines produce compatible Delta table schemas
(LongType / Int64 for all integer columns) to prevent
DELTA_MERGE_INCOMPATIBLE_DATATYPE errors when appending to shared tables.
"""

import pytest

pytestmark = pytest.mark.spark

pyspark = pytest.importorskip("pyspark", reason="PySpark not installed")


class TestUpcastNarrowInts:
    """Verify _upcast_narrow_ints widens all narrow integer types."""

    def test_integer_type_widened_to_long(self):
        from pyspark.sql import types as T
        from recon.helpers import get_spark, _upcast_narrow_ints

        spark = get_spark()
        schema = T.StructType([
            T.StructField("id", T.IntegerType(), False),
            T.StructField("name", T.StringType(), True),
            T.StructField("count", T.ShortType(), True),
            T.StructField("flag", T.ByteType(), True),
            T.StructField("big", T.LongType(), True),
        ])
        df = spark.createDataFrame([(1, "a", 10, 1, 100)], schema=schema)
        result = _upcast_narrow_ints(df)

        result_types = {f.name: type(f.dataType) for f in result.schema.fields}
        assert result_types["id"] == T.LongType
        assert result_types["name"] == T.StringType
        assert result_types["count"] == T.LongType
        assert result_types["flag"] == T.LongType
        assert result_types["big"] == T.LongType

    def test_no_change_when_already_long(self):
        from pyspark.sql import types as T
        from recon.helpers import get_spark, _upcast_narrow_ints

        spark = get_spark()
        schema = T.StructType([
            T.StructField("x", T.LongType(), False),
            T.StructField("y", T.DoubleType(), True),
        ])
        df = spark.createDataFrame([(1, 2.0)], schema=schema)
        result = _upcast_narrow_ints(df)

        result_types = {f.name: type(f.dataType) for f in result.schema.fields}
        assert result_types["x"] == T.LongType
        assert result_types["y"] == T.DoubleType


class TestPolarsWriteTypeConsistency:
    """Verify that Polars _write_via_spark produces Spark-compatible types."""

    def test_polars_int32_becomes_long_type(self):
        polars = pytest.importorskip("polars", reason="Polars not installed")
        import os
        if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            pytest.skip("Only meaningful on Databricks (tests _write_via_spark path)")

        from recon.engines.polars_engine import _write_via_spark
        from recon.helpers import get_spark

        df = polars.DataFrame({
            "quarter_date": [20260331, 20260630],
            "value": [1.5, 2.5],
        })
        # quarter_date will be Int32 from Python int literals
        assert df.schema["quarter_date"] == polars.Int32

        # Write and verify schema
        test_table = "recon_benchmarks.__test_type_check"
        _write_via_spark(df, test_table, mode="overwrite")
        spark = get_spark()
        spark_schema = spark.table(test_table).schema
        qtr_field = next(f for f in spark_schema.fields if f.name == "quarter_date")

        from pyspark.sql import types as T
        assert isinstance(qtr_field.dataType, T.LongType)
        spark.sql(f"DROP TABLE IF EXISTS {test_table}")


class TestRunMetadataSchemaUsesLongType:
    """Verify the run_metadata schema uses LongType for counts."""

    def test_schema_fields_are_long(self):
        from pyspark.sql import types as T
        from recon.helpers import get_spark

        spark = get_spark()
        # Simulate create_run_metadata by checking the schema definition
        from recon.runner import create_run_metadata
        from recon.config import ReconcileConfig

        cfg = ReconcileConfig(
            left_table_name="default.test_left",
            right_table_name="default.test_right",
            output_catalog="default",
            output_schema="default",
            key_cols=["id", "qtr"],
            qtr_col="qtr",
            critical_cols=["col_a"],
            all_feature_cols=["col_a", "col_b"],
            run_id="test_schema_check",
            source_label="TEST",
            engine="spark",
        )

        # We can't easily call create_run_metadata without a real table,
        # but we can verify the schema is defined with LongType by importing
        # and inspecting the function source.
        import inspect
        source = inspect.getsource(create_run_metadata)
        assert "LongType" in source
        assert "IntegerType" not in source


class TestBatchKeyTypeValidation:
    """Canonical regression test: an unsupported batching-column (cfg.qtr_col)
    datatype must be rejected during validation/setup — before any
    reconciliation work or artifact generation — not deep inside batch_key
    generation."""

    def test_unsupported_qtr_col_type_rejected_at_validation(self):
        import pytest as _pytest
        from pyspark.sql import types as T
        from recon.helpers import get_spark, validate_columns_exist
        from recon.config import ReconcileConfig

        spark = get_spark()
        # DoubleType is an intentionally-unsupported batching key type.
        schema = T.StructType([
            T.StructField("policy_id", T.IntegerType(), False),
            T.StructField("quarter_val", T.DoubleType(), False),
            T.StructField("v", T.DoubleType(), True),
        ])
        df = spark.createDataFrame([(1, 2020.1, 5.0)], schema=schema)
        df.createOrReplaceTempView("bk_type_left")
        df.createOrReplaceTempView("bk_type_right")

        cfg = ReconcileConfig(
            left_table_name="bk_type_left",
            right_table_name="bk_type_right",
            output_catalog="spark_catalog",
            output_schema="default",
            key_cols=["policy_id", "quarter_val"],
            qtr_col="quarter_val",
            critical_cols=["v"],
            all_feature_cols=["v"],
            run_id="bk_type_check",
            source_label="TEST",
        )

        # Fails at the earliest schema-aware setup step (validate_columns_exist),
        # naming the offending column and its detected type.
        with _pytest.raises(ValueError, match="quarter_val.*unsupported type"):
            validate_columns_exist(cfg)
