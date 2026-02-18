"""
RetailNova - Spark Session Builder
====================================
Creates a configured SparkSession.

Modes:
  local[*]         → for host-side scripts and unit tests
  spark://spark:7077 → for Jupyter notebooks inside Docker
  
Linux notes:
  - First run downloads Maven JARs (~200MB). Cached in ~/.ivy2/ after that.
  - Set SPARK_LOCAL_DIRS to a fast disk if /tmp is slow.
  - Delta Lake requires specific Hadoop AWS version for S3A support.
"""

import os
from pyspark.sql import SparkSession
from pyspark import SparkConf

from pipelines.config import spark_config, storage_config


def build_spark_session(app_name: str = None) -> SparkSession:
    """
    Build and return a configured SparkSession for RetailNova.

    Automatically adapts to:
      - local[*]         when SPARK_MASTER=local[*]  (host / tests)
      - cluster mode     when SPARK_MASTER=spark://... (Jupyter in Docker)
    """
    name   = app_name or spark_config.app_name
    master = spark_config.master

    print(f"[Spark] Building session: app={name}, master={master}")

    conf = SparkConf()

    # ── Core ──────────────────────────────────────────────────────────────
    conf.set("spark.app.name", name)
    conf.set("spark.master",   master)
    conf.set("spark.executor.memory", spark_config.executor_mem)
    conf.set("spark.driver.memory",   spark_config.driver_mem)

    # ── Delta Lake ─────────────────────────────────────────────────────────
    conf.set("spark.sql.extensions",
             "io.delta.sql.DeltaSparkSessionExtension")
    conf.set("spark.sql.catalog.spark_catalog",
             "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
    conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

    # ── S3A / MinIO ────────────────────────────────────────────────────────
    for k, v in storage_config.spark_config.items():
        conf.set(k, v)

    # ── Performance ────────────────────────────────────────────────────────
    conf.set("spark.sql.shuffle.partitions",
             str(spark_config.shuffle_partitions))
    conf.set("spark.sql.adaptive.enabled",                        "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled",     "true")
    # Auto-broadcast tables < 10MB (avoids shuffle for small dims)
    conf.set("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))

    # ── Linux-specific optimisations ────────────────────────────────────────
    # Use /tmp for local Spark storage (writable on all Linux systems)
    conf.set("spark.local.dir", "/tmp/spark-retailnova")
    # Reduce log noise in local mode
    conf.set("spark.ui.enabled", "false" if master.startswith("local") else "true")

    # ── Packages ──────────────────────────────────────────────────────────
    conf.set("spark.jars.packages", spark_config.packages)

    spark = (
        SparkSession.builder
        .config(conf=conf)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[Spark] Session ready — Spark {spark.version}")
    return spark


def stop_session(spark: SparkSession) -> None:
    """Gracefully stop SparkSession."""
    if spark:
        spark.stop()
        print("[Spark] Session stopped")
