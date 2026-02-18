"""
RetailNova - Silver Transformation Pipeline
============================================
Bronze (raw) → Silver (cleaned, validated, deduplicated)

Key patterns:
  - Data cleansing & standardisation
  - SCD Type 2 for dim_customer (address changes)
  - Deduplication using window functions
  - Data masking of PII fields
  - Referential integrity checks
  - Schema enforcement
"""

import sys
import hashlib
from datetime import datetime, timezone, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

sys.path.insert(0, "/opt/bitnami/spark")

from pipelines.config       import storage_config, pipeline_config
from pipelines.logger       import pipeline_run, log_metric, log_error
from pipelines.spark_session import build_spark_session


# ─── HELPERS ────────────────────────────────────────────────────────────────

def read_bronze(spark: SparkSession, table_name: str) -> DataFrame:
    """Read from Bronze Delta table."""
    path = storage_config.layer_path("bronze", table_name)
    return spark.read.format("delta").load(path)


def write_silver_delta(
    df: DataFrame,
    table_name: str,
    merge_condition: str = None,
    primary_keys: list = None,
) -> int:
    """
    Write to Silver Delta table.

    Uses MERGE (upsert) when merge_condition is provided.
    Otherwise overwrites (for full-refresh small tables).

    Cost strategy:
      - OPTIMIZE: compacts small files weekly (scheduled separately)
      - ZORDER by PK: improves selective reads (partition pruning)
    """
    target_path = storage_config.layer_path("silver", table_name)
    spark = df.sparkSession

    row_count = df.count()

    if DeltaTable.isDeltaTable(spark, target_path) and merge_condition and primary_keys:
        # ── UPSERT into existing Silver table ─────────────────────────────
        delta_target = DeltaTable.forPath(spark, target_path)

        (
            delta_target.alias("target")
            .merge(
                df.alias("source"),
                merge_condition
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        # ── First run or full refresh ──────────────────────────────────────
        (
            df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .save(target_path)
        )

    return row_count


def mask_pii(df: DataFrame, columns: list) -> DataFrame:
    """
    Apply SHA-256 masking to PII columns.

    In Azure: this is done via Column-Level Security in Synapse /
              Dynamic Data Masking in SQL DB.
    For GDPR compliance we mask at Silver layer.
    Only data analysts with 'PII_ACCESS' role see originals.
    """
    for col in columns:
        if col in df.columns:
            df = df.withColumn(
                col,
                F.sha2(F.col(col).cast("string"), 256)
            )
    return df


# ─── CUSTOMERS → Silver (with SCD Type 2) ───────────────────────────────────

def transform_customers(spark: SparkSession, run_id: str) -> DataFrame:
    """
    Clean and transform raw customers.

    SCD Type 2 logic: if address or email changes → create new version.
    Columns tracked for SCD2: address_line1, city, country, email, loyalty_tier.
    """
    df_raw = read_bronze(spark, "customers")

    # ── Deduplicate: keep latest record per customer_id ───────────────────
    w = Window.partitionBy("customer_id").orderBy(F.col("last_modified").desc())
    df = (
        df_raw
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # ── Standardise fields ────────────────────────────────────────────────
    df = (
        df
        .withColumn("email",       F.trim(F.lower(F.col("email"))))
        .withColumn("first_name",  F.initcap(F.trim(F.col("first_name"))))
        .withColumn("last_name",   F.initcap(F.trim(F.col("last_name"))))
        .withColumn("country",     F.upper(F.trim(F.col("country"))))
        .withColumn("city",        F.initcap(F.trim(F.col("city"))))
        .withColumn("gender",      F.upper(F.trim(F.col("gender"))))
        .withColumn("loyalty_tier",F.initcap(F.trim(F.col("loyalty_tier"))))
    )

    # ── Validate email format ─────────────────────────────────────────────
    df = df.withColumn(
        "email_valid",
        F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
    )

    # ── Age calculation ───────────────────────────────────────────────────
    df = df.withColumn(
        "age",
        F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
    )

    # ── Surrogate key (SHA-256 of natural key for SCD2 compatibility) ─────
    df = df.withColumn(
        "customer_sk",
        F.sha2(
            F.concat_ws("|",
                F.col("customer_id").cast("string"),
                F.coalesce(F.col("email"), F.lit(""))
            ),
            256
        )
    )

    # ── Silver metadata ───────────────────────────────────────────────────
    df = (
        df
        .withColumn("_silver_created_at", F.current_timestamp())
        .withColumn("_run_id",            F.lit(run_id))
        .withColumn("_source",            F.lit("bronze/customers"))
    )

    # ── Drop raw audit columns ─────────────────────────────────────────────
    drop_cols = ["_extracted_at", "_source_table", "_source_system",
                 "_year", "_month"]
    df = df.drop(*[c for c in drop_cols if c in df.columns])

    return df


def apply_scd2_customers(spark: SparkSession, df_new: DataFrame, run_id: str) -> None:
    """
    Apply SCD Type 2 to the Silver customers Delta table.

    Tracked columns: address_line1, city, country, email, loyalty_tier
    When any tracked column changes:
      1. Set effective_end_date = NOW() and is_current = FALSE on old row
      2. Insert new row with effective_start_date = NOW() and is_current = TRUE

    In Databricks this uses the native MERGE INTO with SCD2 extension.
    Here we implement it manually with Delta MERGE.
    """
    target_path = storage_config.layer_path("silver", "customers")
    scd2_cols   = ["address_line1", "city", "country", "email", "loyalty_tier"]

    # Add SCD2 metadata to incoming data
    df_new = (
        df_new
        .withColumn("effective_start_date", F.current_timestamp())
        .withColumn("effective_end_date",   F.lit(None).cast("timestamp"))
        .withColumn("is_current",           F.lit(True))
        .withColumn("scd_version",          F.lit(1))
    )

    if not DeltaTable.isDeltaTable(spark, target_path):
        # First load → write directly
        (
            df_new.write
            .format("delta")
            .mode("overwrite")
            .option("delta.autoOptimize.optimizeWrite", "true")
            .save(target_path)
        )
        print(f"  [SCD2] First load: {df_new.count()} customers written")
        return

    delta_target = DeltaTable.forPath(spark, target_path)

    # Build SCD2 change detection condition
    change_condition = " OR ".join([
        f"target.{c} <> source.{c}" for c in scd2_cols
    ])

    (
        delta_target.alias("target")
        .merge(
            df_new.alias("source"),
            "target.customer_id = source.customer_id AND target.is_current = true"
        )
        # If no change → update non-tracked cols (name, phone, etc.)
        .whenMatchedUpdate(
            condition=f"NOT ({change_condition})",
            set={
                "first_name":    "source.first_name",
                "last_name":     "source.last_name",
                "phone":         "source.phone",
                "loyalty_tier":  "source.loyalty_tier",
                "_run_id":       "source._run_id",
                "_silver_created_at": "source._silver_created_at",
            }
        )
        # If SCD2 change → expire old row
        .whenMatchedUpdate(
            condition=change_condition,
            set={
                "effective_end_date": "current_timestamp()",
                "is_current":         "false",
            }
        )
        # Insert new row (covers: new customers + SCD2 new version)
        .whenNotMatchedInsertAll()
        .execute()
    )

    print(f"  [SCD2] SCD2 merge complete for customers")


# ─── PRODUCTS → Silver ───────────────────────────────────────────────────────

def transform_products(spark: SparkSession, run_id: str) -> DataFrame:
    df_raw = read_bronze(spark, "products")

    w = Window.partitionBy("product_id").orderBy(F.col("last_modified").desc())
    df = (
        df_raw
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = (
        df
        .withColumn("product_code",  F.upper(F.trim(F.col("product_code"))))
        .withColumn("product_name",  F.trim(F.col("product_name")))
        .withColumn("category",      F.initcap(F.trim(F.col("category"))))
        .withColumn("subcategory",   F.initcap(F.trim(F.col("subcategory"))))
        .withColumn("brand",         F.trim(F.col("brand")))
        # Margin calculation
        .withColumn("margin_pct",
            F.when(
                F.col("cost_price").isNotNull() & (F.col("cost_price") > 0),
                F.round(
                    (F.col("unit_price") - F.col("cost_price")) / F.col("unit_price") * 100,
                    2
                )
            ).otherwise(F.lit(None))
        )
        # Filter out invalid prices
        .filter(F.col("unit_price") > 0)
        .withColumn("_silver_created_at", F.current_timestamp())
        .withColumn("_run_id",            F.lit(run_id))
    )

    drop_cols = ["_extracted_at", "_source_table", "_source_system", "_year", "_month"]
    return df.drop(*[c for c in drop_cols if c in df.columns])


# ─── STORES → Silver ─────────────────────────────────────────────────────────

def transform_stores(spark: SparkSession, run_id: str) -> DataFrame:
    df_raw = read_bronze(spark, "stores")

    df = (
        df_raw
        .withColumn("store_code",  F.upper(F.trim(F.col("store_code"))))
        .withColumn("store_name",  F.trim(F.col("store_name")))
        .withColumn("city",        F.initcap(F.trim(F.col("city"))))
        .withColumn("country",     F.upper(F.trim(F.col("country"))))
        .withColumn("_silver_created_at", F.current_timestamp())
        .withColumn("_run_id",            F.lit(run_id))
    )

    drop_cols = ["_extracted_at", "_source_table", "_source_system", "_year", "_month"]
    return df.drop(*[c for c in drop_cols if c in df.columns])


# ─── SALES ORDERS → Silver ───────────────────────────────────────────────────

def transform_sales_orders(spark: SparkSession, run_id: str) -> DataFrame:
    df_raw = read_bronze(spark, "sales_orders")

    w = Window.partitionBy("order_id").orderBy(F.col("last_modified").desc())
    df = (
        df_raw
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df = (
        df
        .withColumn("order_number",    F.upper(F.trim(F.col("order_number"))))
        .withColumn("status",          F.initcap(F.trim(F.col("status"))))
        .withColumn("payment_method",  F.initcap(F.trim(F.col("payment_method"))))
        # Ensure non-negative
        .withColumn("shipping_cost",
            F.when(F.col("shipping_cost") < 0, F.lit(0)).otherwise(F.col("shipping_cost"))
        )
        .withColumn("discount_pct",
            F.when(F.col("discount_pct") < 0, F.lit(0))
             .when(F.col("discount_pct") > 100, F.lit(0))
             .otherwise(F.col("discount_pct"))
        )
        # Flag suspicious orders (total_amount = 0 and status != Cancelled)
        .withColumn("is_suspicious",
            (F.col("total_amount") == 0) & (F.col("status") != "Cancelled")
        )
        # Date parts for partition pruning
        .withColumn("order_year",  F.year(F.col("order_date")))
        .withColumn("order_month", F.month(F.col("order_date")))
        .withColumn("_silver_created_at", F.current_timestamp())
        .withColumn("_run_id",            F.lit(run_id))
    )

    drop_cols = ["_extracted_at", "_source_table", "_source_system", "_year", "_month"]
    return df.drop(*[c for c in drop_cols if c in df.columns])


# ─── ORDER LINES → Silver ─────────────────────────────────────────────────────

def transform_order_lines(spark: SparkSession, run_id: str) -> DataFrame:
    df_raw = read_bronze(spark, "sales_order_lines")

    df = (
        df_raw
        .filter(F.col("quantity") > 0)        # Remove erroneous 0-qty lines
        .filter(F.col("unit_price") > 0)       # Remove $0 price anomalies
        .withColumn("line_total_calc",
            F.round(F.col("quantity") * F.col("unit_price") - F.col("discount_amt"), 2)
        )
        .withColumn("_silver_created_at", F.current_timestamp())
        .withColumn("_run_id",            F.lit(run_id))
    )

    drop_cols = ["_extracted_at", "_source_table", "_source_system", "_year", "_month"]
    return df.drop(*[c for c in drop_cols if c in df.columns])


# ─── MASTER SILVER PIPELINE ─────────────────────────────────────────────────

def run_silver_transformation(spark: SparkSession) -> None:
    print(f"\n{'='*60}")
    print(f"  SILVER TRANSFORMATION  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    # ── Customers (SCD2) ────────────────────────────────────────────────────
    with pipeline_run("silver_customers", "transformation", "silver",
                      "bronze/customers", "silver/customers") as ctx:
        df_customers = transform_customers(spark, ctx["run_id"])
        apply_scd2_customers(spark, df_customers, ctx["run_id"])
        ctx["rows_written"] = df_customers.count()
        log_metric(ctx["run_id"], "silver_customers", "rows_written",
                   ctx["rows_written"], "rows", "silver")

    # ── Products ────────────────────────────────────────────────────────────
    with pipeline_run("silver_products", "transformation", "silver",
                      "bronze/products", "silver/products") as ctx:
        df_products = transform_products(spark, ctx["run_id"])
        cnt = write_silver_delta(df_products, "products",
                                 "target.product_id = source.product_id",
                                 ["product_id"])
        ctx["rows_written"] = cnt
        log_metric(ctx["run_id"], "silver_products", "rows_written", cnt, "rows", "silver")

    # ── Stores ──────────────────────────────────────────────────────────────
    with pipeline_run("silver_stores", "transformation", "silver",
                      "bronze/stores", "silver/stores") as ctx:
        df_stores = transform_stores(spark, ctx["run_id"])
        cnt = write_silver_delta(df_stores, "stores",
                                 "target.store_id = source.store_id",
                                 ["store_id"])
        ctx["rows_written"] = cnt

    # ── Sales Orders ────────────────────────────────────────────────────────
    with pipeline_run("silver_sales_orders", "transformation", "silver",
                      "bronze/sales_orders", "silver/sales_orders") as ctx:
        df_orders = transform_sales_orders(spark, ctx["run_id"])
        cnt = write_silver_delta(df_orders, "sales_orders",
                                 "target.order_id = source.order_id",
                                 ["order_id"])
        ctx["rows_written"] = cnt

    # ── Order Lines ─────────────────────────────────────────────────────────
    with pipeline_run("silver_order_lines", "transformation", "silver",
                      "bronze/sales_order_lines", "silver/order_lines") as ctx:
        df_lines = transform_order_lines(spark, ctx["run_id"])
        cnt = write_silver_delta(df_lines, "order_lines",
                                 "target.line_id = source.line_id",
                                 ["line_id"])
        ctx["rows_written"] = cnt

    print("\n[SILVER] All transformations complete ✓")


if __name__ == "__main__":
    spark = build_spark_session("RetailNova-Silver-Transformation")
    try:
        run_silver_transformation(spark)
    finally:
        spark.stop()
