"""
RetailNova - Unit Test Suite
==============================
Tests: PySpark transformation logic, DQ rules, schema validation,
       row count reconciliation, negative scenarios (corrupt data).

Run with:
    cd /home/jovyan && python -m pytest tests/test_pipeline.py -v
"""

import sys
import pytest
from decimal import Decimal
from datetime import datetime, date, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    DecimalType, TimestampType, DateType, BooleanType, DoubleType
)

sys.path.insert(0, "/opt/bitnami/spark")


# ─── SHARED SPARK FIXTURE ───────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """
    Create a local SparkSession for unit testing.
    Uses local[*] master — no Docker Spark cluster needed.
    Delta Lake extension included.
    """
    session = (
        SparkSession.builder
        .master("local[2]")
        .appName("RetailNova-UnitTests")
        .config("spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.jars.packages",
                "io.delta:delta-spark_2.12:3.1.0")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ─── SAMPLE DATA FACTORIES ──────────────────────────────────────────────────

def make_customers_df(spark, rows=None):
    schema = StructType([
        StructField("customer_id",  IntegerType(),   False),
        StructField("first_name",   StringType(),    False),
        StructField("last_name",    StringType(),    False),
        StructField("email",        StringType(),    True),
        StructField("phone",        StringType(),    True),
        StructField("city",         StringType(),    True),
        StructField("country",      StringType(),    False),
        StructField("loyalty_tier", StringType(),    True),
        StructField("date_of_birth",DateType(),      True),
        StructField("gender",       StringType(),    True),
        StructField("is_active",    BooleanType(),   False),
        StructField("last_modified",TimestampType(), False),
    ])
    default_rows = [
        (1, "  alexandros  ", "PAPADIMITRIOU", "alex@example.gr",
         "+30-210-123", "athens", "greece", "gold",
         date(1985, 3, 15), "m", True, datetime(2024, 1, 1)),
        (2, "SOFIA", "georgiou", "SOFIA.G@EXAMPLE.GR",
         "+30-210-456", "ATHENS", "GREECE", "platinum",
         date(1990, 7, 22), "F", True, datetime(2024, 2, 1)),
    ]
    return spark.createDataFrame(rows or default_rows, schema)


def make_products_df(spark, rows=None):
    schema = StructType([
        StructField("product_id",   IntegerType(),               False),
        StructField("product_code", StringType(),                False),
        StructField("product_name", StringType(),                False),
        StructField("category",     StringType(),                False),
        StructField("subcategory",  StringType(),                True),
        StructField("unit_price",   DecimalType(10, 2),          False),
        StructField("cost_price",   DecimalType(10, 2),          True),
        StructField("is_active",    BooleanType(),               False),
        StructField("last_modified",TimestampType(),             False),
    ])
    default_rows = [
        (1, "elec-001", "Smartphone X",   "electronics", "smartphones",
         Decimal("899.99"), Decimal("550.00"), True, datetime(2024, 1, 1)),
        (2, "HOME-002", "  Air Purifier", "home",        "air quality",
         Decimal("399.99"), Decimal("200.00"), True, datetime(2024, 1, 1)),
    ]
    return spark.createDataFrame(rows or default_rows, schema)


def make_orders_df(spark, rows=None):
    schema = StructType([
        StructField("order_id",      IntegerType(),   False),
        StructField("order_number",  StringType(),    False),
        StructField("customer_id",   IntegerType(),   False),
        StructField("store_id",      IntegerType(),   False),
        StructField("order_date",    TimestampType(), False),
        StructField("status",        StringType(),    False),
        StructField("shipping_cost", DoubleType(),    True),
        StructField("discount_pct",  DoubleType(),    True),
        StructField("total_amount",  DoubleType(),    True),
        StructField("payment_method",StringType(),    True),
        StructField("last_modified", TimestampType(), False),
    ])
    default_rows = [
        (1, "ord-001", 1, 1, datetime(2024, 1, 15), "delivered",
         5.99, 0.0, 929.99, "credit card", datetime(2024, 1, 15)),
        (2, "ord-002", 2, 5, datetime(2024, 2, 1), "shipped",
         0.0, 10.0, 349.99, "paypal", datetime(2024, 2, 1)),
    ]
    return spark.createDataFrame(rows or default_rows, schema)


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 1: Silver Customer Transformations
# ══════════════════════════════════════════════════════════════════════════════

class TestSilverCustomerTransformation:

    def test_email_lowercase_trim(self, spark):
        """Email must be lowercased and trimmed."""
        df = make_customers_df(spark, rows=[
            (1, "Test", "User", "  USER@EXAMPLE.COM  ",
             None, "Athens", "Greece", "Bronze",
             date(1990, 1, 1), "M", True, datetime(2024, 1, 1)),
        ])
        result = df.withColumn(
            "email", F.trim(F.lower(F.col("email")))
        )
        email = result.collect()[0]["email"]
        assert email == "user@example.com", f"Expected 'user@example.com', got '{email}'"

    def test_first_name_initcap(self, spark):
        """First name must be title-cased."""
        df = make_customers_df(spark, rows=[
            (1, "  alexandros  ", "test", "a@b.gr",
             None, "Athens", "Greece", "Bronze",
             date(1985, 1, 1), "M", True, datetime(2024, 1, 1)),
        ])
        result = df.withColumn(
            "first_name", F.initcap(F.trim(F.col("first_name")))
        )
        name = result.collect()[0]["first_name"]
        assert name == "Alexandros", f"Expected 'Alexandros', got '{name}'"

    def test_country_uppercase(self, spark):
        """Country must be uppercased."""
        df = make_customers_df(spark)
        result = df.withColumn("country", F.upper(F.trim(F.col("country"))))
        countries = {r["country"] for r in result.collect()}
        assert all(c == c.upper() for c in countries), "Country not uppercased"

    def test_age_calculation(self, spark):
        """Age should be calculated correctly from date_of_birth."""
        dob = date(1985, 3, 15)
        df  = make_customers_df(spark, rows=[
            (1, "Alex", "P", "a@b.gr", None, "Athens", "Greece",
             "Gold", dob, "M", True, datetime(2024, 1, 1)),
        ])
        result = df.withColumn(
            "age", F.floor(F.datediff(F.current_date(), F.col("date_of_birth")) / 365.25)
        )
        age = result.collect()[0]["age"]
        assert age >= 38, f"Age should be >= 38 for DOB {dob}, got {age}"

    def test_email_regex_valid(self, spark):
        """Valid emails should pass regex."""
        valid_emails = ["user@example.gr", "name.surname@company.com", "test+tag@domain.io"]
        df = spark.createDataFrame(
            [(e,) for e in valid_emails], ["email"]
        )
        result = df.withColumn(
            "valid", F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        )
        invalids = result.filter(F.col("valid") == False).count()
        assert invalids == 0, f"{invalids} valid emails failed regex check"

    def test_email_regex_invalid(self, spark):
        """Invalid emails should fail regex."""
        invalid_emails = ["not-an-email", "@domain.com", "user@", "plaintext"]
        df = spark.createDataFrame(
            [(e,) for e in invalid_emails], ["email"]
        )
        result = df.withColumn(
            "valid", F.col("email").rlike(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
        )
        invalids = result.filter(F.col("valid") == False).count()
        assert invalids == len(invalid_emails), (
            f"Expected {len(invalid_emails)} invalid, got {invalids}"
        )

    def test_deduplication_keeps_latest(self, spark):
        """When same customer_id appears twice, keep latest last_modified."""
        from pyspark.sql.window import Window
        rows = [
            (1, "Alex",  "Old",  "a@b.gr", None, "Athens", "Greece",
             "Bronze", date(1990, 1, 1), "M", True, datetime(2024, 1, 1)),
            (1, "Alex",  "New",  "a@b.gr", None, "Patras", "Greece",
             "Gold",   date(1990, 1, 1), "M", True, datetime(2024, 6, 1)),
        ]
        df     = make_customers_df(spark, rows)
        w      = Window.partitionBy("customer_id").orderBy(F.col("last_modified").desc())
        result = (
            df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
        )
        assert result.count() == 1, "Deduplication should keep only 1 row"
        city = result.collect()[0]["city"]
        assert city == "Patras", f"Should keep latest (Patras), got '{city}'"


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 2: Silver Product Transformations
# ══════════════════════════════════════════════════════════════════════════════

class TestSilverProductTransformation:

    def test_product_code_uppercase(self, spark):
        """Product codes must be uppercased."""
        df     = make_products_df(spark)
        result = df.withColumn("product_code", F.upper(F.trim(F.col("product_code"))))
        codes  = {r["product_code"] for r in result.collect()}
        assert all(c == c.upper() for c in codes)

    def test_product_name_trim(self, spark):
        """Product names must be trimmed."""
        df     = make_products_df(spark)
        result = df.withColumn("product_name", F.trim(F.col("product_name")))
        names  = [r["product_name"] for r in result.collect()]
        assert all(n == n.strip() for n in names)

    def test_zero_price_filtered(self, spark):
        """Products with unit_price <= 0 must be filtered out."""
        rows = [
            (1, "P001", "Valid",   "Elec", "Sub", Decimal("99.99"), Decimal("50.00"), True, datetime(2024,1,1)),
            (2, "P002", "Zero$",   "Elec", "Sub", Decimal("0.00"),  Decimal("50.00"), True, datetime(2024,1,1)),
            (3, "P003", "Neg$",    "Elec", "Sub", Decimal("-1.00"), Decimal("50.00"), True, datetime(2024,1,1)),
        ]
        df     = make_products_df(spark, rows)
        result = df.filter(F.col("unit_price") > 0)
        assert result.count() == 1, f"Expected 1 valid product, got {result.count()}"


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 3: Sales Order Transformations
# ══════════════════════════════════════════════════════════════════════════════

class TestSilverOrderTransformation:

    def test_negative_shipping_cost_zeroed(self, spark):
        """Negative shipping cost should be set to 0."""
        rows = [(1, "ORD-001", 1, 1, datetime(2024, 1, 1),
                 "delivered", -9.99, 0.0, 100.0, "card", datetime(2024, 1, 1))]
        df     = make_orders_df(spark, rows)
        result = df.withColumn(
            "shipping_cost",
            F.when(F.col("shipping_cost") < 0, F.lit(0.0)).otherwise(F.col("shipping_cost"))
        )
        sc = result.collect()[0]["shipping_cost"]
        assert sc == 0.0, f"Expected 0.0, got {sc}"

    def test_discount_pct_over_100_zeroed(self, spark):
        """Discount > 100% is invalid and should be set to 0."""
        rows = [(1, "ORD-001", 1, 1, datetime(2024, 1, 1),
                 "delivered", 5.99, 150.0, 100.0, "card", datetime(2024, 1, 1))]
        df     = make_orders_df(spark, rows)
        result = df.withColumn(
            "discount_pct",
            F.when(F.col("discount_pct") > 100, F.lit(0.0)).otherwise(F.col("discount_pct"))
        )
        dp = result.collect()[0]["discount_pct"]
        assert dp == 0.0, f"Expected 0.0 for >100% discount, got {dp}"

    def test_suspicious_flag(self, spark):
        """Order with total_amount=0 and status!='Cancelled' is suspicious."""
        rows = [
            (1, "ORD-001", 1, 1, datetime(2024, 1, 1),
             "delivered",  5.99, 0.0, 0.0, "card", datetime(2024, 1, 1)),  # suspicious
            (2, "ORD-002", 2, 1, datetime(2024, 1, 1),
             "cancelled",  0.0,  0.0, 0.0, "card", datetime(2024, 1, 1)),  # not suspicious
            (3, "ORD-003", 3, 1, datetime(2024, 1, 1),
             "delivered",  5.99, 0.0, 100.0, "card", datetime(2024, 1, 1)), # normal
        ]
        df = make_orders_df(spark, rows)
        result = df.withColumn(
            "is_suspicious",
            (F.col("total_amount") == 0) & (F.col("status") != "cancelled")
        )
        suspicious_count = result.filter(F.col("is_suspicious") == True).count()
        assert suspicious_count == 1, f"Expected 1 suspicious order, got {suspicious_count}"


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 4: Data Quality Rules (Unit Tests)
# ══════════════════════════════════════════════════════════════════════════════

class TestDataQualityRules:

    def test_dq_not_null_pass(self, spark):
        """not_null rule: all values present → PASS."""
        df = spark.createDataFrame(
            [("a@b.gr",), ("c@d.gr",)], ["email"]
        )
        null_count = df.filter(F.col("email").isNull()).count()
        total      = df.count()
        pass_rate  = (total - null_count) / total * 100
        assert pass_rate == 100.0

    def test_dq_not_null_fail(self, spark):
        """not_null rule: NULLs present → fail rate detected."""
        df = spark.createDataFrame(
            [("a@b.gr",), (None,), (None,)], ["email"]
        )
        null_count = df.filter(F.col("email").isNull()).count()
        assert null_count == 2

    def test_dq_uniqueness_pass(self, spark):
        """unique rule: all distinct → PASS."""
        df = spark.createDataFrame(
            [("a@b.gr",), ("c@d.gr",), ("e@f.gr",)], ["email"]
        )
        total    = df.count()
        distinct = df.select("email").distinct().count()
        assert total == distinct

    def test_dq_uniqueness_fail(self, spark):
        """unique rule: duplicates exist → fail rate detected."""
        df = spark.createDataFrame(
            [("dup@b.gr",), ("dup@b.gr",), ("other@d.gr",)], ["email"]
        )
        total    = df.count()
        distinct = df.select("email").distinct().count()
        assert distinct < total, "Expected duplicates to be detected"

    def test_dq_range_pass(self, spark):
        """range rule: all values in [0.01, 99999] → all pass."""
        df = spark.createDataFrame(
            [(1.0,), (99999.0,), (100.50,)], ["price"]
        )
        out_of_range = df.filter(
            (F.col("price") < 0.01) | (F.col("price") > 99999.0)
        ).count()
        assert out_of_range == 0

    def test_dq_range_fail(self, spark):
        """range rule: values outside [0.01, 99999] → caught."""
        df = spark.createDataFrame(
            [(0.0,), (-5.0,), (100000.0,), (50.0,)], ["price"]
        )
        out_of_range = df.filter(
            (F.col("price") < 0.01) | (F.col("price") > 99999.0)
        ).count()
        assert out_of_range == 3

    def test_dq_completeness_threshold(self, spark):
        """completeness: 50% non-null should fail 80% threshold."""
        df = spark.createDataFrame(
            [(None,), ("val",), (None,), ("val",)], ["phone"]
        )
        total     = df.count()
        non_null  = df.filter(F.col("phone").isNotNull()).count()
        pass_rate = non_null / total * 100
        threshold = 80.0
        assert pass_rate < threshold, f"Should fail threshold {threshold}%, got {pass_rate}%"


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 5: Schema Validation
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaValidation:

    def test_required_columns_present(self, spark):
        """All required columns must exist in customers DataFrame."""
        required = {"customer_id", "email", "last_modified"}
        df       = make_customers_df(spark)
        actual   = set(df.columns)
        missing  = required - actual
        assert not missing, f"Missing required columns: {missing}"

    def test_missing_column_detected(self, spark):
        """Schema validation should catch a missing required column."""
        df_without_email = spark.createDataFrame(
            [(1, "Alex", "P")], ["customer_id", "first_name", "last_name"]
        )
        required = {"customer_id", "email", "last_modified"}
        actual   = set(df_without_email.columns)
        missing  = required - actual
        assert "email" in missing, "Should detect missing 'email' column"
        assert "last_modified" in missing, "Should detect missing 'last_modified'"

    def test_extra_columns_allowed(self, spark):
        """Extra columns beyond required should not fail schema check."""
        df = make_customers_df(spark)
        df_extra = df.withColumn("extra_col", F.lit("extra"))
        required = {"customer_id", "email", "last_modified"}
        actual   = set(df_extra.columns)
        missing  = required - actual
        assert not missing, "Extra columns should not cause schema failure"


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 6: Row Count Reconciliation
# ══════════════════════════════════════════════════════════════════════════════

class TestRowCountReconciliation:

    def test_bronze_to_silver_row_count(self, spark):
        """
        After Silver transform (no filter), row count should not increase.
        (SCD2 may reduce distinct business entities but lines stay same.)
        """
        bronze_count = 10
        df_bronze = spark.range(bronze_count).toDF("id")

        # Simulate deduplication keeping all (no dupes)
        from pyspark.sql.window import Window
        df_silver = df_bronze.withColumn("_rn", F.row_number().over(
            Window.partitionBy("id").orderBy(F.lit(1))
        )).filter(F.col("_rn") == 1).drop("_rn")

        silver_count = df_silver.count()
        assert silver_count <= bronze_count, (
            f"Silver count ({silver_count}) should not exceed Bronze ({bronze_count})"
        )

    def test_deduplication_reduces_count(self, spark):
        """Deduplication should reduce count when duplicates exist."""
        rows = [
            (1, datetime(2024, 1, 1)),
            (1, datetime(2024, 6, 1)),  # duplicate customer_id
            (2, datetime(2024, 1, 1)),
        ]
        df = spark.createDataFrame(rows, ["customer_id", "last_modified"])

        from pyspark.sql.window import Window
        w  = Window.partitionBy("customer_id").orderBy(F.col("last_modified").desc())
        result = (
            df
            .withColumn("_rn", F.row_number().over(w))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        assert result.count() == 2, f"Expected 2 unique customers, got {result.count()}"


# ══════════════════════════════════════════════════════════════════════════════
# TEST CLASS 7: Negative Tests (Corrupt Data Scenarios)
# ══════════════════════════════════════════════════════════════════════════════

class TestNegativeScenarios:

    def test_corrupt_null_email_caught_by_dq(self, spark):
        """Null email in Silver should be caught by not_null DQ rule."""
        df = spark.createDataFrame(
            [(1, None), (2, "valid@email.gr"), (3, None)],
            ["customer_id", "email"]
        )
        null_count = df.filter(F.col("email").isNull()).count()
        total      = df.count()
        pass_rate  = (total - null_count) / total * 100

        # Rule threshold is 100% for CRITICAL — this should FAIL
        assert pass_rate < 100.0, "Corrupt null emails not detected"
        assert null_count == 2, f"Should catch 2 null emails, got {null_count}"

    def test_negative_quantity_in_order_lines(self, spark):
        """Quantity <= 0 must be filtered from Silver."""
        rows = [
            (1, 1, 1, 5,   99.99, 0),   # valid
            (2, 2, 1, 0,   99.99, 0),   # invalid - zero quantity
            (3, 3, 1, -3,  99.99, 0),   # invalid - negative quantity
            (4, 4, 1, 10,  99.99, 0),   # valid
        ]
        df = spark.createDataFrame(
            rows, ["line_id", "order_id", "product_id", "quantity", "unit_price", "discount_amt"]
        )
        valid = df.filter(F.col("quantity") > 0)
        assert valid.count() == 2, f"Expected 2 valid lines, got {valid.count()}"

    def test_corrupt_file_injection_schema_mismatch(self, spark):
        """Schema mismatch in incoming data should be caught."""
        # Simulate a corrupt CSV that was merged with wrong column order
        corrupt_df = spark.createDataFrame(
            [(1, "wrong_field", 99.99)],
            ["customer_id", "unit_price", "email"]   # wrong column types/names
        )
        required = {"customer_id", "email", "last_modified"}
        actual   = set(corrupt_df.columns)
        missing  = required - actual
        assert "last_modified" in missing, "Schema mismatch should be detected"
        assert "email" not in missing or corrupt_df.schema["email"].dataType.typeName() != "string"

    def test_order_total_mismatch_detection(self, spark):
        """Flag orders where total_amount doesn't match sum of line amounts."""
        # order total = 100, but sum of lines = 150 → mismatch
        orders = spark.createDataFrame(
            [(1, 100.0)], ["order_id", "total_amount"]
        )
        lines = spark.createDataFrame(
            [(1, 1, 100.0), (1, 2, 50.0)], ["order_id", "line_id", "line_total"]
        )
        line_sums = lines.groupBy("order_id").agg(
            F.sum("line_total").alias("lines_total")
        )
        joined = orders.join(line_sums, "order_id")
        mismatches = joined.filter(
            F.abs(F.col("total_amount") - F.col("lines_total")) > 1.0
        ).count()
        assert mismatches == 1, f"Expected 1 order total mismatch, got {mismatches}"

    def test_scd2_address_change_detection(self, spark):
        """SCD2: changing address should be detected as a change."""
        old_record = spark.createDataFrame(
            [(1, "Ermou 12", "Athens", "GREECE")],
            ["customer_id", "address_line1", "city", "country"]
        )
        new_record = spark.createDataFrame(
            [(1, "Syntagma 5", "Athens", "GREECE")],   # address changed
            ["customer_id", "address_line1", "city", "country"]
        )

        joined = old_record.alias("old").join(
            new_record.alias("new"), on="customer_id"
        )
        changed = joined.filter(
            F.col("old.address_line1") != F.col("new.address_line1")
        )
        assert changed.count() == 1, "SCD2 address change should be detected"


# ─── RUN TESTS ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import subprocess
    subprocess.run([
        "python", "-m", "pytest",
        __file__, "-v", "--tb=short", "-q"
    ])
