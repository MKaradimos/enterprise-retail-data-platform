"""
RetailNova - Gold Layer Pipeline
==================================
Silver → Gold (Star Schema, KPIs, Cohorts, Segments)

Produces:
  dim_date          - Date dimension with all calendar attributes
  dim_customer      - SCD2-aware customer dimension with surrogate key
  dim_product       - Product dimension with margin categories
  dim_store         - Store dimension
  fact_sales        - Fact table with degenerate dimension (order_number)
  agg_monthly_kpis  - Pre-aggregated monthly KPIs for dashboards
  cohort_analysis   - Monthly retention cohort table
  customer_segments - RFM-based segmentation
"""

import sys
from datetime import datetime, date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType,
    DateType, BooleanType
)
from delta.tables import DeltaTable

sys.path.insert(0, "/opt/bitnami/spark")

from pipelines.config       import storage_config
from pipelines.logger       import pipeline_run, log_metric
from pipelines.spark_session import build_spark_session


# ─── HELPERS ────────────────────────────────────────────────────────────────

def read_silver(spark: SparkSession, table_name: str) -> DataFrame:
    path = storage_config.layer_path("silver", table_name)
    return spark.read.format("delta").load(path)


def write_gold(df: DataFrame, table_name: str, mode: str = "overwrite") -> int:
    path = storage_config.layer_path("gold", table_name)
    (
        df.write
        .format("delta")
        .mode(mode)
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .save(path)
    )
    return df.count()


# ─── DIM_DATE ───────────────────────────────────────────────────────────────

def build_dim_date(spark: SparkSession) -> DataFrame:
    """
    Generate a complete date dimension from 2020-01-01 to 2030-12-31.

    This is computed once and stored in Gold.
    Includes: fiscal week, quarter, holiday flags, Greek locale attributes.
    """
    from pyspark.sql.types import LongType

    # Generate sequence of dates
    df_dates = spark.sql("""
        SELECT
            explode(sequence(
                to_date('2020-01-01'),
                to_date('2030-12-31'),
                interval 1 day
            )) AS calendar_date
    """)

    df_dim = (
        df_dates
        .withColumn("date_key",         F.date_format("calendar_date", "yyyyMMdd").cast("int"))
        .withColumn("year",             F.year("calendar_date"))
        .withColumn("quarter",          F.quarter("calendar_date"))
        .withColumn("month",            F.month("calendar_date"))
        .withColumn("month_name",       F.date_format("calendar_date", "MMMM"))
        .withColumn("month_name_short", F.date_format("calendar_date", "MMM"))
        .withColumn("week_of_year",     F.weekofyear("calendar_date"))
        .withColumn("day_of_month",     F.dayofmonth("calendar_date"))
        .withColumn("day_of_week",      F.dayofweek("calendar_date"))      # 1=Sun
        .withColumn("day_name",         F.date_format("calendar_date", "EEEE"))
        .withColumn("day_name_short",   F.date_format("calendar_date", "EEE"))
        .withColumn("is_weekend",
            F.col("day_of_week").isin([1, 7])
        )
        .withColumn("is_weekday",
            ~F.col("day_of_week").isin([1, 7])
        )
        # Fiscal year (assume fiscal = calendar for RetailNova)
        .withColumn("fiscal_year",   F.year("calendar_date"))
        .withColumn("fiscal_quarter",F.quarter("calendar_date"))
        .withColumn("fiscal_period",
            F.concat(F.lit("FY"), F.year("calendar_date"),
                     F.lit("Q"), F.quarter("calendar_date"))
        )
        # Year-Month label for charts
        .withColumn("year_month",    F.date_format("calendar_date", "yyyy-MM"))
        .withColumn("year_quarter",  F.concat(F.year("calendar_date"), F.lit("-Q"),
                                              F.quarter("calendar_date")))
        # Is last day of month
        .withColumn("is_month_end",
            F.col("calendar_date") == F.last_day("calendar_date")
        )
        # Greek public holidays (static list for demo)
        .withColumn("is_holiday",
            F.col("calendar_date").isin([
                date(2024, 1, 1),   # New Year
                date(2024, 3, 25),  # Independence Day
                date(2024, 5, 1),   # Labour Day
                date(2024, 8, 15),  # Assumption
                date(2024, 10, 28), # OXI Day
                date(2024, 12, 25), # Christmas
                date(2024, 12, 26), # Boxing Day
                date(2025, 1, 1),
                date(2025, 3, 25),
                date(2025, 5, 1),
            ])
        )
        # Relative period labels
        .withColumn("is_current_month",
            (F.year("calendar_date")  == F.year(F.current_date())) &
            (F.month("calendar_date") == F.month(F.current_date()))
        )
        .withColumn("is_current_year",
            F.year("calendar_date") == F.year(F.current_date())
        )
    )

    return df_dim.orderBy("date_key")


# ─── DIM_CUSTOMER ───────────────────────────────────────────────────────────

def build_dim_customer(spark: SparkSession) -> DataFrame:
    """
    Customer dimension from Silver (current records only).
    Adds: customer lifetime value bucket, age group.
    """
    df = read_silver(spark, "customers")

    # Take only current SCD2 records (or all if SCD2 not yet applied)
    if "is_current" in df.columns:
        df = df.filter(F.col("is_current") == True)

    df_dim = (
        df
        .withColumn("full_name",
            F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
        )
        .withColumn("age_group",
            F.when(F.col("age") < 25, "18-24")
             .when(F.col("age") < 35, "25-34")
             .when(F.col("age") < 45, "35-44")
             .when(F.col("age") < 55, "45-54")
             .when(F.col("age") < 65, "55-64")
             .otherwise("65+")
        )
        .select(
            F.col("customer_id"),
            F.col("customer_sk"),
            "full_name", "first_name", "last_name",
            "email",     "phone",      "gender",
            "city",      "country",    "postal_code",
            "age",       "age_group",  "date_of_birth",
            "loyalty_tier", "is_active",
            "created_at",
            F.col("_silver_created_at").alias("dim_created_at"),
        )
    )
    return df_dim


# ─── DIM_PRODUCT ────────────────────────────────────────────────────────────

def build_dim_product(spark: SparkSession) -> DataFrame:
    df = read_silver(spark, "products")

    return (
        df
        .withColumn("margin_category",
            F.when(F.col("margin_pct") >= 50, "High Margin")
             .when(F.col("margin_pct") >= 30, "Mid Margin")
             .when(F.col("margin_pct") >= 10, "Low Margin")
             .otherwise("Unknown")
        )
        .select(
            "product_id", "product_code", "product_name",
            "category",   "subcategory",  "brand",
            "unit_price", "cost_price",   "margin_pct",
            "margin_category", "weight_kg", "is_active",
            "created_at",
        )
    )


# ─── DIM_STORE ──────────────────────────────────────────────────────────────

def build_dim_store(spark: SparkSession) -> DataFrame:
    df = read_silver(spark, "stores")

    return df.select(
        "store_id", "store_code", "store_name",
        "store_type", "city", "region", "country",
        "manager_name", "open_date", "is_active",
    )


# ─── FACT_SALES ─────────────────────────────────────────────────────────────

def build_fact_sales(spark: SparkSession) -> DataFrame:
    """
    Star schema fact table.
    Degenerate dimension: order_number (kept on fact for drill-down).
    Foreign keys to all dimensions.
    Grain: one row per order line.
    """
    df_orders = read_silver(spark, "sales_orders")
    df_lines  = read_silver(spark, "order_lines")
    df_cust   = read_silver(spark, "customers")
    df_prod   = read_silver(spark, "products")

    # Get date keys
    df_orders = df_orders.withColumn(
        "order_date_key",
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int")
    )

    # Customer surrogate key lookup
    if "customer_sk" in df_cust.columns:
        df_cust_sk = df_cust.select("customer_id", "customer_sk")
        if "is_current" in df_cust.columns:
            df_cust_sk = df_cust_sk.filter(F.col("is_current") == True)
    else:
        df_cust_sk = df_cust.select(
            "customer_id",
            F.sha2(F.col("customer_id").cast("string"), 256).alias("customer_sk")
        )

    # Product cost lookup (broadcast for small dim table)
    df_prod_cost = df_prod.select(
        F.col("product_id").alias("_prod_id"),
        F.col("cost_price"),
    )

    # Join fact: orders + lines + SK lookups + product cost
    df_fact = (
        df_lines
        .join(df_orders.select(
            "order_id", "order_number", "customer_id", "store_id",
            "order_date", "order_date_key", "status",
            "shipping_cost", "discount_pct", "payment_method",
            "order_year", "order_month"
        ), on="order_id", how="inner")

        .join(df_cust_sk, on="customer_id", how="left")

        .join(F.broadcast(df_prod_cost),
              F.col("product_id") == F.col("_prod_id"), how="left")
        .drop("_prod_id")

        # KPI calculations at line level
        .withColumn("revenue",
            F.round(F.col("line_total_calc"), 2)
        )
        .withColumn("gross_profit",
            F.round(
                F.col("line_total_calc") -
                (F.col("quantity") * F.coalesce(F.col("cost_price"), F.lit(0))),
                2
            )
        )
        .select(
            # Surrogate / natural keys
            "line_id",              # grain key
            "order_id",             # FK to orders
            "order_number",         # degenerate dimension
            "customer_id",
            F.coalesce("customer_sk", F.sha2(F.col("customer_id").cast("string"), 256))
             .alias("customer_sk"),
            "store_id",
            "product_id",
            "order_date_key",
            # Order attributes
            "order_date", "order_year", "order_month",
            "status", "payment_method",
            # Line measures
            "quantity",
            "unit_price",
            "discount_amt",
            F.col("line_total_calc").alias("line_amount"),
            "shipping_cost",
        )
    )
    return df_fact


# ─── AGG_MONTHLY_KPIS ───────────────────────────────────────────────────────

def build_monthly_kpis(spark: SparkSession) -> DataFrame:
    """
    Pre-aggregated monthly KPIs for the executive dashboard.

    KPIs:
      - Total Revenue
      - Total Orders
      - Unique Customers
      - Average Order Value (AOV)
      - Basket Size (avg items per order)
      - Repeat Purchase Rate
      - Customer Lifetime Value (CLV proxy)
    """
    df_fact   = spark.read.format("delta").load(
        storage_config.layer_path("gold", "fact_sales"))
    df_orders = read_silver(spark, "sales_orders")

    # Monthly order-level aggregation
    df_order_agg = (
        df_orders
        .filter(F.col("status").isin(["Delivered", "Shipped", "Processing"]))
        .groupBy("order_year", "order_month")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("total_amount").alias("total_revenue"),
            F.countDistinct("customer_id").alias("unique_customers"),
            F.avg("total_amount").alias("avg_order_value"),
        )
    )

    # Basket size from fact
    df_basket = (
        df_fact
        .groupBy("order_id", "order_year", "order_month")
        .agg(
            F.sum("quantity").alias("total_items"),
            F.count("line_id").alias("total_lines"),
        )
        .groupBy("order_year", "order_month")
        .agg(
            F.avg("total_items").alias("avg_basket_size"),
            F.avg("total_lines").alias("avg_basket_lines"),
        )
    )

    # Repeat purchase rate: % of customers who bought more than once (all time)
    df_repeat = (
        df_orders
        .filter(F.col("status").isin(["Delivered", "Shipped"]))
        .groupBy("customer_id")
        .agg(F.count("order_id").alias("order_count"))
        .withColumn("is_repeat", F.col("order_count") > 1)
        .agg(
            F.count("customer_id").alias("total_customers"),
            F.sum(F.col("is_repeat").cast("int")).alias("repeat_customers")
        )
        .withColumn("repeat_purchase_rate",
            F.round(F.col("repeat_customers") / F.col("total_customers") * 100, 2)
        )
        .select("total_customers", "repeat_customers", "repeat_purchase_rate")
    )

    repeat_rate = df_repeat.collect()[0]["repeat_purchase_rate"] if df_repeat.count() > 0 else 0.0

    # CLV proxy: avg revenue per customer (lifetime)
    df_clv = (
        df_orders
        .filter(F.col("status").isin(["Delivered", "Shipped"]))
        .groupBy("customer_id")
        .agg(F.sum("total_amount").alias("customer_lifetime_value"))
        .agg(
            F.avg("customer_lifetime_value").alias("avg_clv"),
            F.percentile_approx("customer_lifetime_value", 0.5).alias("median_clv"),
        )
    )

    avg_clv    = df_clv.collect()[0]["avg_clv"]    or 0.0
    median_clv = df_clv.collect()[0]["median_clv"] or 0.0

    # Combine monthly aggregations
    df_monthly = (
        df_order_agg
        .join(df_basket, on=["order_year", "order_month"], how="left")
        .withColumn("repeat_purchase_rate", F.lit(repeat_rate))
        .withColumn("avg_customer_ltv",     F.lit(float(avg_clv)))
        .withColumn("median_customer_ltv",  F.lit(float(median_clv)))
        .withColumn("year_month",
            F.concat_ws("-",
                F.lpad(F.col("order_year").cast("string"),  4, "0"),
                F.lpad(F.col("order_month").cast("string"), 2, "0")
            )
        )
        .withColumn("_gold_created_at", F.current_timestamp())
        .orderBy("order_year", "order_month")
    )

    return df_monthly


# ─── COHORT ANALYSIS ────────────────────────────────────────────────────────

def build_cohort_analysis(spark: SparkSession) -> DataFrame:
    """
    Monthly acquisition cohort → retention analysis.

    Cohort month = first purchase month.
    Calculates retained customers at each month offset (0, 1, 2, ...).
    """
    df_orders = read_silver(spark, "sales_orders")
    df_orders = df_orders.filter(
        F.col("status").isin(["Delivered", "Shipped", "Processing"])
    )

    # First purchase month per customer
    df_first = (
        df_orders
        .groupBy("customer_id")
        .agg(F.min("order_date").alias("first_order_date"))
        .withColumn("cohort_year",  F.year("first_order_date"))
        .withColumn("cohort_month", F.month("first_order_date"))
    )

    # Join back to get all orders with cohort info
    df_cohort = (
        df_orders
        .join(df_first, on="customer_id", how="inner")
        .withColumn("order_period_months",
            (F.year("order_date")  - F.col("cohort_year"))  * 12 +
            (F.month("order_date") - F.col("cohort_month"))
        )
    )

    # Aggregate by cohort + period offset
    df_result = (
        df_cohort
        .groupBy("cohort_year", "cohort_month", "order_period_months")
        .agg(F.countDistinct("customer_id").alias("active_customers"))
        .withColumn("cohort_label",
            F.concat_ws("-",
                F.lpad(F.col("cohort_year").cast("string"),  4, "0"),
                F.lpad(F.col("cohort_month").cast("string"), 2, "0")
            )
        )
        .withColumn("_gold_created_at", F.current_timestamp())
        .orderBy("cohort_year", "cohort_month", "order_period_months")
    )

    # Cohort size (period 0 = acquisition month)
    df_cohort_size = (
        df_result
        .filter(F.col("order_period_months") == 0)
        .select(
            "cohort_year", "cohort_month",
            F.col("active_customers").alias("cohort_size")
        )
    )

    df_final = (
        df_result
        .join(df_cohort_size, on=["cohort_year", "cohort_month"], how="left")
        .withColumn("retention_rate",
            F.round(F.col("active_customers") / F.col("cohort_size") * 100, 2)
        )
    )
    return df_final


# ─── CUSTOMER SEGMENTS (RFM) ────────────────────────────────────────────────

def build_customer_segments(spark: SparkSession) -> DataFrame:
    """
    RFM (Recency, Frequency, Monetary) segmentation.

    Scoring: 1–4 per dimension → segment label.
    Segments: Champions, Loyal, At-Risk, Lost, New Customers, etc.
    """
    df_orders = read_silver(spark, "sales_orders")
    df_orders = df_orders.filter(
        F.col("status").isin(["Delivered", "Shipped"])
    )

    # Calculate RFM metrics
    today = date.today()
    df_rfm = (
        df_orders
        .groupBy("customer_id")
        .agg(
            F.datediff(F.lit(today), F.max("order_date")).alias("recency_days"),
            F.count("order_id").alias("frequency"),
            F.sum("total_amount").alias("monetary"),
        )
    )

    # Score each dimension 1-4 using percentile buckets
    df_scored = (
        df_rfm
        .withColumn("r_score",
            F.when(F.col("recency_days") <= 30, 4)
             .when(F.col("recency_days") <= 60, 3)
             .when(F.col("recency_days") <= 90, 2)
             .otherwise(1)
        )
        .withColumn("f_score",
            F.when(F.col("frequency") >= 5, 4)
             .when(F.col("frequency") >= 3, 3)
             .when(F.col("frequency") >= 2, 2)
             .otherwise(1)
        )
        .withColumn("m_score",
            F.when(F.col("monetary") >= 2000, 4)
             .when(F.col("monetary") >= 1000, 3)
             .when(F.col("monetary") >= 300, 2)
             .otherwise(1)
        )
    )

    # RFM segment labels
    df_segments = (
        df_scored
        .withColumn("rfm_score",
            F.col("r_score") * 100 + F.col("f_score") * 10 + F.col("m_score")
        )
        .withColumn("segment",
            F.when(
                (F.col("r_score") >= 3) & (F.col("f_score") >= 3) & (F.col("m_score") >= 3),
                "Champions"
            ).when(
                (F.col("r_score") >= 3) & (F.col("f_score") >= 2),
                "Loyal Customers"
            ).when(
                (F.col("r_score") >= 3) & (F.col("f_score") == 1),
                "Promising"
            ).when(
                (F.col("r_score") == 2) & (F.col("f_score") >= 2),
                "At Risk"
            ).when(
                F.col("r_score") <= 2,
                "Need Attention"
            ).otherwise("New Customers")
        )
        .withColumn("_gold_created_at", F.current_timestamp())
    )

    return df_segments


# ─── MASTER GOLD PIPELINE ───────────────────────────────────────────────────

def run_gold_pipeline(spark: SparkSession) -> None:
    print(f"\n{'='*60}")
    print(f"  GOLD LAYER BUILD  |  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*60}\n")

    with pipeline_run("gold_dim_date", "transformation", "gold") as ctx:
        df = build_dim_date(spark)
        cnt = write_gold(df, "dim_date")
        ctx["rows_written"] = cnt
        print(f"  [dim_date]     {cnt} rows written ✓")

    with pipeline_run("gold_dim_customer", "transformation", "gold") as ctx:
        df = build_dim_customer(spark)
        cnt = write_gold(df, "dim_customer")
        ctx["rows_written"] = cnt
        print(f"  [dim_customer] {cnt} rows written ✓")

    with pipeline_run("gold_dim_product", "transformation", "gold") as ctx:
        df = build_dim_product(spark)
        cnt = write_gold(df, "dim_product")
        ctx["rows_written"] = cnt
        print(f"  [dim_product]  {cnt} rows written ✓")

    with pipeline_run("gold_dim_store", "transformation", "gold") as ctx:
        df = build_dim_store(spark)
        cnt = write_gold(df, "dim_store")
        ctx["rows_written"] = cnt
        print(f"  [dim_store]    {cnt} rows written ✓")

    with pipeline_run("gold_fact_sales", "transformation", "gold") as ctx:
        df = build_fact_sales(spark)
        cnt = write_gold(df, "fact_sales")
        ctx["rows_written"] = cnt
        log_metric(ctx["run_id"], "gold_fact_sales", "rows_written", cnt, "rows", "gold")
        print(f"  [fact_sales]   {cnt} rows written ✓")

    with pipeline_run("gold_monthly_kpis", "transformation", "gold") as ctx:
        df = build_monthly_kpis(spark)
        cnt = write_gold(df, "agg_monthly_kpis")
        ctx["rows_written"] = cnt
        print(f"  [monthly_kpis] {cnt} rows written ✓")

    with pipeline_run("gold_cohort_analysis", "transformation", "gold") as ctx:
        df = build_cohort_analysis(spark)
        cnt = write_gold(df, "cohort_analysis")
        ctx["rows_written"] = cnt
        print(f"  [cohort]       {cnt} rows written ✓")

    with pipeline_run("gold_customer_segments", "transformation", "gold") as ctx:
        df = build_customer_segments(spark)
        cnt = write_gold(df, "customer_segments")
        ctx["rows_written"] = cnt
        print(f"  [segments]     {cnt} rows written ✓")

    print("\n[GOLD] All Gold tables built ✓")


if __name__ == "__main__":
    spark = build_spark_session("RetailNova-Gold-Pipeline")
    try:
        run_gold_pipeline(spark)
    finally:
        spark.stop()
