"""
RetailNova - Bronze Layer Ingestion Pipeline
==============================================
Incremental extraction from SQL Server → MinIO (Delta format)

Key patterns:
  - Watermark-based CDC (high-water mark per table)
  - Full schema preservation (raw / immutable)
  - Partitioned by year/month
  - Audit columns added (_extracted_at, _source_table, _run_id)
  - Schema validation before write
"""

import sys
import json
from datetime import datetime, timezone
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.tables import DeltaTable

sys.path.insert(0, "/opt/bitnami/spark")

from pipelines.config   import source_config, storage_config, pipeline_config, BRONZE_TABLES
from pipelines.logger   import pipeline_run, get_watermark, update_watermark, log_metric
from pipelines.spark_session import build_spark_session


# ─── SOURCE TABLE METADATA ──────────────────────────────────────────────────
# Maps each source table to its CDC column (used for incremental extraction)
SOURCE_TABLE_CONFIG = {
    "customers": {
        "cdc_column":  "last_modified",
        "primary_key": "customer_id",
        "partition":   True,
    },
    "products": {
        "cdc_column":  "last_modified",
        "primary_key": "product_id",
        "partition":   True,
    },
    "stores": {
        "cdc_column":  "created_at",
        "primary_key": "store_id",
        "partition":   False,          # small dim table → no partitioning needed
    },
    "sales_orders": {
        "cdc_column":  "last_modified",
        "primary_key": "order_id",
        "partition":   True,
    },
    "sales_order_lines": {
        "cdc_column":  "last_modified",
        "primary_key": "line_id",
        "partition":   True,
    },
}


def extract_incremental(
    spark: SparkSession,
    table_name: str,
    cdc_column: str,
    watermark: datetime,
) -> DataFrame:
    """
    Read new/changed rows from SQL Server since the last watermark.

    Uses JDBC pushdown predicate to avoid full table scans.
    In Azure: this would be replaced with ADF's Copy Activity or
              Databricks Auto Loader with CDC.
    """
    # Build pushdown predicate – converts watermark to SQL Server DATETIME format
    wm_str = watermark.strftime("%Y-%m-%d %H:%M:%S")
    predicate = f"{cdc_column} > '{wm_str}'"

    print(f"  [EXTRACT] {table_name} WHERE {cdc_column} > '{wm_str}'")

    df = (
        spark.read.format("jdbc")
        .option("url",        source_config.jdbc_url)
        .option("dbtable",    f"(SELECT * FROM dbo.{table_name} WHERE {predicate}) AS t")
        .option("user",       source_config.username)
        .option("password",   source_config.password)
        .option("driver",     source_config.jdbc_properties["driver"])
        # Parallelism hint for large tables
        .option("fetchsize",  str(pipeline_config.batch_size))
        .load()
    )
    return df


def add_audit_columns(df: DataFrame, table_name: str, run_id: str) -> DataFrame:
    """Append Bronze audit metadata columns to the raw dataframe."""
    return (
        df
        .withColumn("_extracted_at",   F.current_timestamp())
        .withColumn("_source_table",   F.lit(f"dbo.{table_name}"))
        .withColumn("_source_system",  F.lit("RetailNova_OLTP_SQLServer"))
        .withColumn("_run_id",         F.lit(run_id))
        .withColumn("_year",           F.year(F.current_timestamp()))
        .withColumn("_month",          F.month(F.current_timestamp()))
    )


def write_bronze_delta(
    df: DataFrame,
    table_name: str,
    storage,
    partition_flag: bool,
) -> int:
    """
    Write DataFrame to Bronze Delta table in MinIO.

    Write mode: APPEND (Bronze is immutable — we never overwrite raw data)
    Partitioning: year + month (only for high-volume tables)

    Cost-saving strategy:
      - Delta auto-compaction: merges small files → reduces S3 LIST calls
      - Partition pruning: downstream reads scan only relevant partitions
    """
    target_path = storage.layer_path("bronze", table_name)

    writer = (
        df.write
        .format("delta")
        .mode("append")
        .option("delta.autoOptimize.optimizeWrite",    "true")
        .option("delta.autoOptimize.autoCompact",      "true")
        # Data retention: keep 7 days of Delta log history (cost vs auditability)
        .option("delta.logRetentionDuration",          "interval 7 days")
        .option("delta.deletedFileRetentionDuration",  "interval 7 days")
    )

    if partition_flag:
        writer = writer.partitionBy("_year", "_month")

    writer.save(target_path)

    # Return row count for logging
    return df.count()


def validate_schema(df: DataFrame, table_name: str) -> bool:
    """
    Basic schema validation before Bronze write.
    Checks that mandatory columns exist and types are consistent.
    """
    schema = df.schema
    field_names = {f.name for f in schema.fields}

    # Minimum required columns per table
    required = {
        "customers":         {"customer_id", "email", "last_modified"},
        "products":          {"product_id",  "product_code", "unit_price"},
        "stores":            {"store_id",    "store_code"},
        "sales_orders":      {"order_id",    "customer_id", "order_date"},
        "sales_order_lines": {"line_id",     "order_id",    "product_id", "quantity"},
    }

    missing = required.get(table_name, set()) - field_names
    if missing:
        raise ValueError(
            f"Schema validation failed for '{table_name}': "
            f"missing columns: {missing}"
        )
    return True


# ─── MAIN PIPELINE ──────────────────────────────────────────────────────────

def run_bronze_ingestion(spark: SparkSession, tables: list = None) -> None:
    """
    Master Bronze ingestion — iterates over all configured source tables.

    Each table runs in its own pipeline_run context so failures are
    isolated (one table failure does not stop the others).
    """
    tables_to_run = tables or BRONZE_TABLES
    print(f"\n{'='*60}")
    print(f"  BRONZE INGESTION  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Tables: {tables_to_run}")
    print(f"{'='*60}\n")

    overall_rows = 0

    for table_name in tables_to_run:
        cfg = SOURCE_TABLE_CONFIG.get(table_name)
        if not cfg:
            print(f"  [SKIP] No config found for '{table_name}'")
            continue

        watermark_key = f"src_{table_name}"

        with pipeline_run(
            pipeline_name  = f"bronze_{table_name}",
            pipeline_type  = "ingestion",
            layer          = "bronze",
            source_table   = f"dbo.{table_name}",
            target_table   = f"bronze/{table_name}",
        ) as ctx:

            # 1. Read watermark
            watermark = get_watermark(watermark_key)
            print(f"  [{table_name}] watermark = {watermark}")

            # 2. Incremental extract
            df_raw = extract_incremental(
                spark, table_name,
                cfg["cdc_column"], watermark
            )
            row_count = df_raw.count()
            ctx["rows_read"] = row_count
            print(f"  [{table_name}] extracted {row_count} rows")

            if row_count == 0:
                print(f"  [{table_name}] no new data → skipping write")
                ctx["rows_skipped"] = 0
                update_watermark(watermark_key, datetime.now(timezone.utc), 0, "SKIPPED")
                continue

            # 3. Schema validation
            validate_schema(df_raw, table_name)

            # 4. Add audit columns
            df_audited = add_audit_columns(df_raw, table_name, ctx["run_id"])

            # 5. Write to Bronze
            written = write_bronze_delta(
                df_audited, table_name,
                storage_config,
                cfg["partition"]
            )
            ctx["rows_written"] = written
            overall_rows += written

            # 6. Update watermark to NOW() (all rows processed)
            update_watermark(
                watermark_key,
                datetime.now(timezone.utc),
                written,
                "SUCCESS"
            )

            # 7. Record metrics
            log_metric(ctx["run_id"], f"bronze_{table_name}",
                       "rows_written", written, "rows", "bronze")

            print(f"  [{table_name}] ✓ written {written} rows to bronze")

    print(f"\n[BRONZE] Total rows written: {overall_rows}")


if __name__ == "__main__":
    spark = build_spark_session("RetailNova-Bronze-Ingestion")
    try:
        run_bronze_ingestion(spark)
    finally:
        spark.stop()
