from __future__ import annotations

import time as _time
from dataclasses import dataclass, field
from typing import Any, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

from .config import ReconcileConfig


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------

_spark_session: SparkSession | None = None


def get_spark() -> SparkSession:
    """Get the active SparkSession, or create one if needed.

    When running locally with Delta, configures the session with the
    delta-spark extensions automatically.
    """
    from pyspark.sql import SparkSession

    global _spark_session

    if _spark_session is not None:
        return _spark_session

    session = SparkSession.getActiveSession()
    if session is not None:
        _spark_session = session
        return session

    # Build a local session with Delta support.
    try:
        from delta import configure_spark_with_delta_pip

        builder = (
            SparkSession.builder
            .appName("recon")
            .master("local[*]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            .config("spark.sql.warehouse.dir", "/tmp/recon_spark_warehouse")
            .config("spark.driver.memory", "4g")
        )
        _spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    except ImportError:
        _spark_session = (
            SparkSession.builder
            .appName("recon")
            .master("local[*]")
            .config("spark.driver.memory", "4g")
            .getOrCreate()
        )

    return _spark_session


def set_spark(session: SparkSession) -> None:
    """Override the module-level SparkSession (used by tests)."""
    global _spark_session
    _spark_session = session


# ---------------------------------------------------------------------------
# Shared-cluster compatibility helper
# ---------------------------------------------------------------------------


def safe_unpersist(df: DataFrame) -> None:
    """Safely unpersist a cached DataFrame.

    On shared Databricks clusters, calling ``.unpersist()`` can raise
    errors due to restricted JVM access.  This helper silently catches
    any such exception.

    Args:
        df: The DataFrame to unpersist.
    """
    try:
        df.unpersist()
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Identifier and table-name helpers
# ---------------------------------------------------------------------------


def normalize_identifier_part(name: str) -> str:
    """Strip surrounding quotes and whitespace from an identifier.

    Handles backticks, single quotes, and double quotes.

    Args:
        name: Raw identifier string (e.g. '`my_col`' or '"schema"').

    Returns:
        Cleaned identifier without surrounding quotes or whitespace.
    """
    value = str(name).strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"`", "'", '"'}:
        value = value[1:-1].strip()
    return value


def quote_name(name: str) -> str:
    """Backtick-quote a column or identifier name for Spark SQL.

    Internal backticks are escaped by doubling them.

    Args:
        name: The identifier to quote.

    Returns:
        Backtick-quoted string safe for use in Spark SQL expressions.
    """
    name = normalize_identifier_part(name)
    return f"`{name.replace('`', '``')}`"


def table_fqn(catalog: str, schema: str, table: str) -> str:
    """Build a fully-qualified three-part table name.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema (database) name.
        table: Table name.

    Returns:
        Backtick-quoted string in the form `catalog`.`schema`.`table`.
    """
    return f"{quote_name(catalog)}.{quote_name(schema)}.{quote_name(table)}"


def schema_fqn(catalog: str, schema: str) -> str:
    """Build a fully-qualified two-part schema reference.

    Args:
        catalog: Unity Catalog catalog name.
        schema: Schema (database) name.

    Returns:
        Backtick-quoted string in the form `catalog`.`schema`.
    """
    return f"{quote_name(catalog)}.{quote_name(schema)}"


def safe_suffix(value: str) -> str:
    """Convert a string into a safe suffix for table names.

    Non-alphanumeric characters (except underscore) are replaced with '_'.

    Args:
        value: Arbitrary string (e.g. a run_id or timestamp).

    Returns:
        Sanitized string containing only [a-zA-Z0-9_].
    """
    out = []
    for ch in str(value):
        if ch.isalnum() or ch == "_":
            out.append(ch)
        else:
            out.append("_")
    return "".join(out)


def colq(name: str) -> F.Column:
    """Return a Spark Column reference with backtick-quoted name.

    Args (lazy import: pyspark.sql.functions):
        name: Column name to reference.

    Returns:
        ``pyspark.sql.Column`` referencing the quoted column name.
    """
    from pyspark.sql import functions as F
    return F.col(quote_name(name))


def chunk_list(values: Sequence[Any], size: int) -> list[list[Any]]:
    """Split a sequence into fixed-size sublists.

    Args:
        values: Sequence to partition.
        size: Maximum number of elements per chunk.

    Returns:
        List of sublists, each containing up to *size* elements.
    """
    return [list(values[i : i + size]) for i in range(0, len(values), size)]


def final_table(cfg: ReconcileConfig, logical_name: str) -> str:
    """Build the fully-qualified name for a persistent output table.

    Args:
        cfg: Active reconciliation configuration.
        logical_name: Short logical name (e.g. 'run_metadata', 'mismatch_sample').

    Returns:
        Three-part table name: `catalog`.`schema`.`recon_<logical_name>`.
    """
    return table_fqn(
        cfg.output_catalog, cfg.output_schema, f"{cfg.final_prefix}_{logical_name}"
    )


def tmp_table(cfg: ReconcileConfig, logical_name: str) -> str:
    """Build the fully-qualified name for a temporary intermediate table.

    The table name includes the sanitized run_id so that concurrent runs
    do not collide.

    Args:
        cfg: Active reconciliation configuration.
        logical_name: Short logical name (e.g. 'left_hashes', 'changed_keys').

    Returns:
        Three-part table name: `catalog`.`schema`.`recon_tmp_<logical_name>_<run_id>`.
    """
    return table_fqn(
        cfg.output_catalog,
        cfg.output_schema,
        f"{cfg.temp_prefix}_{logical_name}_{safe_suffix(cfg.run_id)}",
    )


# ---------------------------------------------------------------------------
# Generic Spark/Delta helpers
# ---------------------------------------------------------------------------


def ensure_schema_exists(cfg: ReconcileConfig) -> None:
    """Verify that the output schema is accessible.

    Raises:
        RuntimeError: If the schema cannot be accessed (e.g. does not exist
            or the caller lacks permissions).

    Args:
        cfg: Active reconciliation configuration.
    """
    try:
        get_spark().sql(
            f"SHOW TABLES IN {schema_fqn(cfg.output_catalog, cfg.output_schema)}"
        ).take(1)
    except Exception as exc:
        raise RuntimeError(
            f"Could not access output schema {cfg.output_catalog}.{cfg.output_schema}. "
            f"Original error: {str(exc)[:1000]}"
        ) from exc


# ---------------------------------------------------------------------------
# Write timing instrumentation
# ---------------------------------------------------------------------------

@dataclass
class WriteTimingRecord:
    """A single table write timing measurement."""
    table_name: str
    operation: str  # "append" or "overwrite"
    elapsed_seconds: float
    row_count: int = 0


class WriteTimingCollector:
    """Collects timing data for Delta table writes."""

    def __init__(self):
        self.records: list[WriteTimingRecord] = []
        self.enabled: bool = False

    def enable(self) -> None:
        self.enabled = True
        self.records.clear()

    def disable(self) -> None:
        self.enabled = False

    def record(self, table_name: str, operation: str, elapsed: float, row_count: int = 0) -> None:
        if self.enabled:
            self.records.append(WriteTimingRecord(table_name, operation, elapsed, row_count))

    def summary(self) -> list[dict]:
        """Return timing records as list of dicts."""
        return [
            {"table_name": r.table_name, "operation": r.operation,
             "elapsed_seconds": r.elapsed_seconds, "row_count": r.row_count}
            for r in self.records
        ]

    def total_write_seconds(self) -> float:
        return sum(r.elapsed_seconds for r in self.records)


_write_timings = WriteTimingCollector()


def get_write_timings() -> WriteTimingCollector:
    """Access the global write timing collector."""
    return _write_timings


# ---------------------------------------------------------------------------
# Delta I/O helpers
# ---------------------------------------------------------------------------


def _is_databricks() -> bool:
    """Detect if running on Databricks (vs local)."""
    import os
    return "DATABRICKS_RUNTIME_VERSION" in os.environ


def _get_table_location(full_table_name: str) -> str:
    """Resolve a managed table path from the warehouse directory.

    For local mode, maps catalog.schema.table -> warehouse_dir/schema.db/table.
    """
    spark = get_spark()
    warehouse = spark.conf.get("spark.sql.warehouse.dir", "/tmp/recon_spark_warehouse")
    parts = full_table_name.replace("`", "").split(".")
    if len(parts) == 3:
        # catalog.schema.table
        _, schema, table = parts
    elif len(parts) == 2:
        schema, table = parts
    else:
        schema = "default"
        table = parts[0]
    return f"{warehouse}/{schema}.db/{table}"


def _upcast_narrow_ints(df: "DataFrame") -> "DataFrame":
    """Widen IntegerType/ShortType/ByteType columns to LongType.

    Prevents Delta schema merge conflicts when different engines (Spark vs
    Polars) write to the same output table — Polars always produces Int64.
    """
    from pyspark.sql import types as _T
    from pyspark.sql import functions as _F

    _NARROW = (_T.ByteType, _T.ShortType, _T.IntegerType)
    for field in df.schema.fields:
        if isinstance(field.dataType, _NARROW):
            df = df.withColumn(field.name, _F.col(field.name).cast(_T.LongType()))
    return df


def write_delta_append(df: DataFrame, full_table_name: str) -> None:  # noqa: F811
    """Append rows to a Delta table, creating it if needed.

    Schema evolution (``mergeSchema``) is enabled so new columns are
    added automatically.

    Args:
        df: DataFrame to write.
        full_table_name: Fully-qualified table name (catalog.schema.table).
    """
    df = _upcast_narrow_ints(df)

    t0 = _time.perf_counter()
    if _is_databricks():
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .saveAsTable(full_table_name)
        )
    else:
        # Local mode: write to path, then register as table.
        path = _get_table_location(full_table_name)
        (
            df.write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(path)
        )
        spark = get_spark()
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception:
            pass
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {full_table_name} "
            f"USING DELTA LOCATION '{path}'"
        )
    elapsed = _time.perf_counter() - t0
    _write_timings.record(full_table_name, "append", elapsed)


def overwrite_delta_table(df: DataFrame, full_table_name: str) -> None:  # noqa: F811
    """Overwrite a Delta table with new data and schema.

    Replaces both data and schema entirely.  Used for temporary
    intermediate tables that are rebuilt each run.

    Args:
        df: DataFrame to write.
        full_table_name: Fully-qualified table name (catalog.schema.table).
    """
    df = _upcast_narrow_ints(df)
    t0 = _time.perf_counter()
    if _is_databricks():
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(full_table_name)
        )
    else:
        # Local mode: write to path, re-register.
        path = _get_table_location(full_table_name)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .save(path)
        )
        spark = get_spark()
        try:
            spark.sql(f"DROP TABLE IF EXISTS {full_table_name}")
        except Exception:
            pass
        spark.sql(
            f"CREATE TABLE IF NOT EXISTS {full_table_name} "
            f"USING DELTA LOCATION '{path}'"
        )
    elapsed = _time.perf_counter() - t0
    _write_timings.record(full_table_name, "overwrite", elapsed)


def is_numeric_type(dt: T.DataType) -> bool:
    """Check whether a Spark DataType is numeric.

    Args (lazy import: pyspark.sql.types):
        dt: A ``pyspark.sql.types.DataType`` instance.

    Returns:
        True if *dt* is one of Byte, Short, Int, Long, Float, Double, or Decimal.
    """
    from pyspark.sql import types as T
    return isinstance(
        dt,
        (
            T.ByteType,
            T.ShortType,
            T.IntegerType,
            T.LongType,
            T.FloatType,
            T.DoubleType,
            T.DecimalType,
        ),
    )


def get_schema_map(table_name: str) -> dict[str, T.DataType]:  # noqa: F811
    """Return a mapping of column name to DataType for a table.

    Args:
        table_name: Fully-qualified or resolvable Spark table name.

    Returns:
        Dict mapping each column name (str) to its ``DataType``.
    """
    return {
        field.name: field.dataType for field in get_spark().table(table_name).schema.fields
    }


def validate_columns_exist(cfg: ReconcileConfig) -> None:
    """Assert that all key and critical columns exist in both source tables.

    Raises:
        ValueError: If any required column is missing from either table.

    Args:
        cfg: Active reconciliation configuration.
    """
    left_schema = get_spark().table(cfg.left_table_name).schema
    right_cols = set(get_spark().table(cfg.right_table_name).columns)
    left_cols = {f.name for f in left_schema.fields}
    strict_required_cols = set(cfg.key_cols) | set(cfg.critical_cols)
    missing_left = sorted(strict_required_cols - left_cols)
    missing_right = sorted(strict_required_cols - right_cols)
    if missing_left:
        raise ValueError(
            f"Missing required key/critical columns from left table: {missing_left[:50]}"
        )
    if missing_right:
        raise ValueError(
            f"Missing required key/critical columns from right table: {missing_right[:50]}"
        )

    # Reject unsupported batching-column types before any reconciliation work.
    assert_supported_batch_key_type(
        left_schema[cfg.qtr_col].dataType, cfg.qtr_col
    )


def resolve_all_compare_cols(cfg: ReconcileConfig) -> list[str]:
    """Determine the full set of columns to compare.

    Resolution order:

    1. If ``cfg.compare_all_columns`` is ``False``, only
       ``cfg.critical_cols`` are compared (fast, focused mode).
    2. Else if ``cfg.all_feature_cols`` is specified, only those columns
       (present in both tables and not in key_cols) are used.
    3. Otherwise, all columns common to both tables (excluding keys) are
       compared.

    Args:
        cfg: Active reconciliation configuration.

    Returns:
        Sorted list of column names to be compared.
    """
    if not cfg.compare_all_columns:
        return sorted(cfg.critical_cols)

    left_cols = set(get_spark().table(cfg.left_table_name).columns)
    right_cols = set(get_spark().table(cfg.right_table_name).columns)
    common = left_cols & right_cols
    excluded = set(cfg.key_cols)

    if cfg.all_feature_cols is not None:
        return sorted(
            [c for c in cfg.all_feature_cols if c in common and c not in excluded]
        )

    return sorted(common - excluded)


def resolve_noncritical_cols(
    cfg: ReconcileConfig, all_compare_cols: list[str]
) -> list[str]:
    """Return the subset of compare columns that are NOT critical.

    Args:
        cfg: Active reconciliation configuration.
        all_compare_cols: Full list of columns being compared.

    Returns:
        List of column names that are in *all_compare_cols* but not in
        ``cfg.critical_cols``.
    """
    critical_set = set(cfg.critical_cols)
    return [c for c in all_compare_cols if c not in critical_set]


# ---------------------------------------------------------------------------
# Hash normalization
# ---------------------------------------------------------------------------


def normalize_for_hash(
    col_expr: F.Column, data_type: T.DataType, cfg: ReconcileConfig
) -> F.Column:
    """Normalize a column value to a deterministic string for hashing.

    Requires PySpark at runtime.

    Applies type-specific transformations (trimming, lowering, rounding,
    date formatting) and replaces NULLs with a sentinel so that
    xxhash64 never receives a null input.

    Args:
        col_expr: Spark Column expression to normalize.
        data_type: The column's ``DataType``.
        cfg: Active reconciliation configuration (controls trim/lower/round).

    Returns:
        A Spark Column expression producing a non-null string.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    sentinel = F.lit("__NULL__")

    if isinstance(data_type, T.StringType):
        x = col_expr
        if cfg.trim_strings_for_hash:
            x = F.trim(x)
        if cfg.lower_strings_for_hash:
            x = F.lower(x)
        return F.coalesce(x.cast("string"), sentinel)

    if (
        isinstance(data_type, (T.FloatType, T.DoubleType))
        and cfg.float_hash_round_scale is not None
    ):
        return F.coalesce(
            F.round(col_expr.cast("double"), cfg.float_hash_round_scale).cast("string"),
            sentinel,
        )

    if isinstance(data_type, T.TimestampType):
        return F.coalesce(
            F.date_format(col_expr, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"), sentinel
        )

    if isinstance(data_type, T.DateType):
        return F.coalesce(F.date_format(col_expr, "yyyy-MM-dd"), sentinel)

    return F.coalesce(col_expr.cast("string"), sentinel)


# ---------------------------------------------------------------------------
# Canonical batch key (invariant persistent-artifact schema)
# ---------------------------------------------------------------------------
#
# Shared artifact tables (quarter_checksums, row_status_counts,
# column_summary_by_quarter) are append-mode and shared across runs.  Writing
# the batching dimension under its dataset-specific name/type (``cfg.qtr_col``)
# makes their schema vary per run and breaks Delta schema merge.  We persist the
# batching value under a single invariant column ``batch_key`` of type STRING.
#
# CANONICAL CONTRACT (must be produced identically by the Spark expression
# ``batch_key_col``, the driver-side ``batch_key_value``, and the Polars engine):
#
#   NULL       -> NULL
#   string     -> value unchanged
#   integral   -> base-10 string, no separators   (byte/short/int/long)
#   date       -> "yyyy-MM-dd"                     (ISO-8601 date)
#   timestamp  -> "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"   (ISO-8601, microseconds)
#
# SUPPORTED batching-column types are exactly those five.  Timestamps are
# formatted by their wall-clock components (Spark session timezone / naive
# Polars datetime); a pure ``date`` column is recommended for period batching.
#
# INTENTIONALLY UNSUPPORTED: float, double, decimal, boolean, and any other
# type.  These are not meaningful period/partition keys and their string
# representations are not portable across Spark, Python, and Polars (float
# repr, boolean casing).
#
# ENFORCEMENT lives upstream in ``validate_columns_exist`` (Spark) and
# ``PolarsEngine.validate_tables`` (Polars), which reject unsupported
# ``cfg.qtr_col`` types at setup — before any reconciliation work — with a
# clear, column- and type-specific message.  The ``batch_key_*`` helpers below
# assume validated input and only produce the canonical representation; their
# terse fallthrough guard exists solely to surface a programming error should
# validation ever be bypassed (it is NOT the user-facing validation policy).

BATCH_KEY_COL = "batch_key"

_BATCH_KEY_TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"
_BATCH_KEY_SUPPORTED_DESC = (
    "string, integral (byte/short/int/long), date, or timestamp"
)


def batch_key_unsupported_error(col_name: str, detected: str) -> ValueError:
    """Build the canonical user-facing error for an unsupported batch key type.

    Used by the upstream validators (Spark ``validate_columns_exist`` and
    ``PolarsEngine.validate_tables``) so both engines emit identical wording.
    """
    return ValueError(
        f"Batching column '{col_name}' has unsupported type '{detected}' for "
        f"batch_key. cfg.qtr_col must be one of: {_BATCH_KEY_SUPPORTED_DESC}. "
        f"float/double/decimal/boolean are not supported as batch keys."
    )


def assert_supported_batch_key_type(data_type: "T.DataType", col_name: str) -> None:
    """Validate that a Spark ``cfg.qtr_col`` DataType is a supported batch key.

    Raises :func:`batch_key_unsupported_error` if not.  Call this at setup
    (see :func:`validate_columns_exist`) so unsupported configurations fail
    before any expensive reconciliation begins.
    """
    from pyspark.sql import types as T

    supported = (
        T.StringType,
        T.ByteType,
        T.ShortType,
        T.IntegerType,
        T.LongType,
        T.DateType,
        T.TimestampType,
    )
    if not isinstance(data_type, supported):
        raise batch_key_unsupported_error(col_name, data_type.simpleString())


def batch_key_col(col_expr: "F.Column", data_type: "T.DataType") -> "F.Column":
    """Format a validated batching-dimension Spark Column as the canonical string.

    Pure formatter: assumes ``data_type`` was already accepted by
    :func:`assert_supported_batch_key_type` upstream.  NULLs are preserved.
    """
    from pyspark.sql import functions as F
    from pyspark.sql import types as T

    if isinstance(data_type, T.TimestampType):
        return F.date_format(col_expr, _BATCH_KEY_TS_FORMAT)
    if isinstance(data_type, T.DateType):
        return F.date_format(col_expr, "yyyy-MM-dd")
    if isinstance(
        data_type, (T.StringType, T.ByteType, T.ShortType, T.IntegerType, T.LongType)
    ):
        return col_expr.cast("string")
    # Internal invariant — should be prevented by upstream validation.
    raise ValueError(f"batch_key_col received unvalidated type: {data_type}")


def batch_key_value(value: Any) -> "str | None":
    """Driver-side equivalent of :func:`batch_key_col` for building row tuples.

    Pure formatter producing output identical to :func:`batch_key_col` for the
    same underlying value.  Assumes the batching column type was validated
    upstream; NULLs are preserved.
    """
    import datetime

    if value is None:
        return None
    # datetime is a subclass of date — check it first.
    if isinstance(value, datetime.datetime):
        return value.strftime("%Y-%m-%dT%H:%M:%S.%f")
    if isinstance(value, datetime.date):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, str):
        return value
    # bool is a subclass of int — exclude it from the integral branch.
    if isinstance(value, int) and not isinstance(value, bool):
        return str(value)
    # Internal invariant — should be prevented by upstream validation.
    raise ValueError(
        f"batch_key_value received unvalidated type: {type(value).__name__}"
    )


# ---------------------------------------------------------------------------
# Column grouping helpers
# ---------------------------------------------------------------------------


def build_column_groups(
    all_compare_cols: list[str],
    critical_cols: Sequence[str],
    hash_group_size: int,
) -> list[list[str]]:
    """Partition all compare columns into fixed-size groups.

    Critical columns are placed first (in their own groups), followed by
    noncritical columns.  Every column appears in exactly one group.
    Returns a list of groups (each a list of column names).
    """
    critical_set = set(critical_cols)
    ordered = [c for c in critical_cols if c in set(all_compare_cols)]
    ordered += [c for c in all_compare_cols if c not in critical_set]
    return chunk_list(ordered, hash_group_size)


def build_group_index(groups: list[list[str]]) -> dict[str, int]:
    """Map each column name to its group index.

    Args:
        groups: List of column groups as returned by :func:`build_column_groups`.

    Returns:
        Dict mapping column name (str) to its zero-based group index (int).
    """
    idx: dict[str, int] = {}
    for gi, grp in enumerate(groups):
        for c in grp:
            idx[c] = gi
    return idx


# ---------------------------------------------------------------------------
# Feature metadata builder
# ---------------------------------------------------------------------------


def build_feature_meta(
    cfg: ReconcileConfig, cols: Sequence[str]
) -> dict[str, dict[str, Any]]:
    """Build per-column metadata used during comparison.

    For each column, determines left/right DataTypes, whether both sides
    are numeric, and the applicable tolerance.

    Args:
        cfg: Active reconciliation configuration.
        cols: Column names to produce metadata for.

    Returns:
        Dict mapping column name to a dict with keys:
        ``left_type``, ``right_type``, ``is_numeric``, ``tolerance``.
    """
    left_schema = get_schema_map(cfg.left_table_name)
    right_schema = get_schema_map(cfg.right_table_name)
    meta: dict[str, dict[str, Any]] = {}
    for c in cols:
        ldt = left_schema[c]
        rdt = right_schema[c]
        numeric = is_numeric_type(ldt) and is_numeric_type(rdt)
        meta[c] = {
            "left_type": ldt,
            "right_type": rdt,
            "is_numeric": numeric,
            "tolerance": float(
                cfg.tolerances.get(c, cfg.default_numeric_tolerance if numeric else 0.0)
            ),
        }
    return meta
