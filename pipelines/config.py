"""
RetailNova Enterprise Data Platform
=====================================
Central configuration module.

Linux notes:
  - Host scripts:    reads .env → uses localhost for all services
  - Inside Jupyter:  Docker injects env vars with container service names
  - Tests:           conftest.py sets SPARK_MASTER=local[*] before import

Priority: env var > .env file > hardcoded default
"""

import os
from dataclasses import dataclass, field

# Load .env file if present (host-side scripts)
try:
    from dotenv import load_dotenv
    _env_path = os.path.join(os.path.dirname(__file__), "..", ".env")
    if os.path.exists(_env_path):
        load_dotenv(_env_path, override=False)  # Don't override already-set env vars
except ImportError:
    pass  # dotenv optional — Docker sets vars directly

# ─── ENVIRONMENT ─────────────────────────────────────────────────────────────
ENV = os.getenv("RETAILNOVA_ENV", "dev")   # dev | test | prod


@dataclass
class SourceConfig:
    """
    On-prem SQL Server (source system).
    Host-side: localhost:1433 (Docker port-forward)
    Container: sqlserver:1433 (service name)
    """
    host:     str = os.getenv("SQLSERVER_HOST", "localhost")
    port:     int = int(os.getenv("SQLSERVER_PORT", "1433"))
    database: str = os.getenv("SQLSERVER_DB",   "RetailNova_OLTP")
    username: str = os.getenv("SQLSERVER_USER",  "sa")
    password: str = os.getenv("SQLSERVER_PASS",  "RetailNova@2024")

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:sqlserver://{self.host}:{self.port};"
            f"databaseName={self.database};"
            "encrypt=false;trustServerCertificate=true"
        )

    @property
    def jdbc_properties(self) -> dict:
        return {
            "user":     self.username,
            "password": self.password,
            "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        }


@dataclass
class StorageConfig:
    """
    MinIO (simulates Azure Data Lake Storage Gen2).
    Host-side: http://localhost:9000
    Container: http://minio:9000
    """
    endpoint:   str = os.getenv("MINIO_ENDPOINT",    "http://localhost:9000")
    access_key: str = os.getenv("MINIO_ACCESS_KEY",  "retailnova_admin")
    secret_key: str = os.getenv("MINIO_SECRET_KEY",  "RetailNova@2024")
    bucket:     str = os.getenv("MINIO_BUCKET",      f"retailnova-{ENV}")

    bronze_prefix: str = "bronze"
    silver_prefix: str = "silver"
    gold_prefix:   str = "gold"

    def layer_path(self, layer: str, table: str) -> str:
        """s3a://bucket/layer/table/"""
        prefix = getattr(self, f"{layer}_prefix")
        return f"s3a://{self.bucket}/{prefix}/{table}"

    def partitioned_path(self, layer: str, table: str, year: int, month: int) -> str:
        """s3a://bucket/layer/table/year=YYYY/month=MM/"""
        base = self.layer_path(layer, table)
        return f"{base}/year={year:04d}/month={month:02d}"

    @property
    def spark_config(self) -> dict:
        """Spark S3A config for MinIO."""
        return {
            "spark.hadoop.fs.s3a.endpoint":               self.endpoint,
            "spark.hadoop.fs.s3a.access.key":             self.access_key,
            "spark.hadoop.fs.s3a.secret.key":             self.secret_key,
            "spark.hadoop.fs.s3a.path.style.access":      "true",
            "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            # Required for Delta Lake on S3-compatible storage
            "spark.delta.logStore.class":
                "org.apache.spark.sql.delta.storage.S3SingleDriverLogStore",
        }


@dataclass
class MetadataDBConfig:
    """
    PostgreSQL metadata store.
    Host-side: localhost:5432
    Container: metadata_db:5432
    """
    host:     str = os.getenv("POSTGRES_HOST",  "localhost")
    port:     int = int(os.getenv("POSTGRES_PORT", "5432"))
    database: str = os.getenv("POSTGRES_DB",    "retailnova_metadata")
    username: str = os.getenv("POSTGRES_USER",  "retailnova")
    password: str = os.getenv("POSTGRES_PASS",  "RetailNova@2024")

    @property
    def connection_string(self) -> str:
        return (
            f"postgresql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    @property
    def jdbc_properties(self) -> dict:
        return {
            "user":     self.username,
            "password": self.password,
            "driver":   "org.postgresql.Driver",
        }


@dataclass
class SparkConfig:
    """
    Spark cluster settings.
    Tests use:  local[2]  (set by conftest.py)
    Jupyter:    spark://spark:7077
    Host:       local[*]  (from .env)
    """
    master:             str = os.getenv("SPARK_MASTER", "local[*]")
    app_name:           str = f"RetailNova-{ENV}"
    executor_mem:       str = "1g"
    driver_mem:         str = "1g"
    # Low for dev/local — set to 200 for production cluster
    shuffle_partitions: int = int(os.getenv("SPARK_SHUFFLE_PARTITIONS", "4"))

    # JAR packages — downloaded once by Spark on first run
    # Note: on first run this takes ~2-3 minutes (downloading from Maven)
    packages: str = (
        "io.delta:delta-spark_2.12:3.1.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.microsoft.sqlserver:mssql-jdbc:12.4.2.jre11,"
        "org.postgresql:postgresql:42.7.3"
    )


@dataclass
class PipelineConfig:
    """Pipeline behaviour flags."""
    batch_size:         int   = 10_000
    max_retries:        int   = 3
    retry_delay_secs:   int   = 30
    alert_email:        str   = os.getenv("ALERT_EMAIL", "dataeng@retailnova.gr")
    sla_hours:          float = 6.0
    enable_quality:     bool  = True
    enable_audit:       bool  = True
    enable_masking:     bool  = True


# ─── TABLE REGISTRIES ────────────────────────────────────────────────────────
BRONZE_TABLES = [
    "customers", "products", "stores",
    "sales_orders", "sales_order_lines",
]

SILVER_TABLES = [
    "silver_customers", "silver_products", "silver_stores",
    "silver_sales_orders", "silver_order_lines",
]

GOLD_TABLES = [
    "dim_customer", "dim_product", "dim_store", "dim_date",
    "fact_sales", "agg_monthly_kpis", "cohort_analysis",
    "customer_segments",
]

# ─── SINGLETON INSTANCES ─────────────────────────────────────────────────────
source_config   = SourceConfig()
storage_config  = StorageConfig()
metadata_config = MetadataDBConfig()
spark_config    = SparkConfig()
pipeline_config = PipelineConfig()
