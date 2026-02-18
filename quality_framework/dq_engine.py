"""
RetailNova - Data Quality Framework
=====================================
Generic, rule-driven validation engine.

Architecture:
  1. Load rules from PostgreSQL data_quality_rules table
  2. For each rule, run dynamic PySpark validation against target table
  3. Write results to data_quality_log table
  4. Raise exception if any CRITICAL rule fails
  5. Return quality report summary

Rule Types Supported:
  - not_null       : Column must have no NULLs
  - unique         : Column must have no duplicates
  - range          : Column value within [min, max]
  - regex          : Column matches pattern
  - row_count      : Table must have >= min_rows
  - freshness      : Max date column cannot be older than N days
  - completeness   : % non-null must exceed threshold
"""

import sys
import json
import re
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional, Tuple
from dataclasses import dataclass

import psycopg2
from psycopg2.extras import RealDictCursor
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

sys.path.insert(0, "/opt/bitnami/spark")

from pipelines.config       import metadata_config, storage_config
from pipelines.logger       import generate_run_id, log_error
from pipelines.spark_session import build_spark_session


# ─── DATA CLASSES ───────────────────────────────────────────────────────────

@dataclass
class QualityRule:
    rule_id:       int
    rule_name:     str
    target_table:  str
    target_column: Optional[str]
    rule_type:     str
    rule_params:   Dict[str, Any]
    threshold_pct: float
    severity:      str


@dataclass
class QualityResult:
    rule_id:        int
    rule_name:      str
    target_table:   str
    target_column:  Optional[str]
    rule_type:      str
    total_records:  int
    passed_records: int
    failed_records: int
    pass_rate_pct:  float
    threshold_pct:  float
    status:         str      # PASS | FAIL | WARNING
    severity:       str
    details:        Dict[str, Any]


# ─── RULE LOADER ────────────────────────────────────────────────────────────

def load_rules(table_filter: str = None) -> List[QualityRule]:
    """Load active DQ rules from metadata PostgreSQL."""
    sql = """
        SELECT rule_id, rule_name, target_table, target_column,
               rule_type, rule_params, threshold_pct, severity
        FROM data_quality_rules
        WHERE is_active = TRUE
    """
    params = []
    if table_filter:
        sql += " AND target_table = %s"
        params.append(table_filter)

    sql += " ORDER BY severity DESC, rule_id"

    rules = []
    try:
        with psycopg2.connect(
            host=metadata_config.host, port=metadata_config.port,
            dbname=metadata_config.database,
            user=metadata_config.username, password=metadata_config.password,
        ) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params or None)
                for row in cur.fetchall():
                    rules.append(QualityRule(
                        rule_id      = row["rule_id"],
                        rule_name    = row["rule_name"],
                        target_table = row["target_table"],
                        target_column= row["target_column"],
                        rule_type    = row["rule_type"],
                        rule_params  = row["rule_params"] or {},
                        threshold_pct= float(row["threshold_pct"]),
                        severity     = row["severity"],
                    ))
    except Exception as e:
        print(f"[DQ] Could not load rules: {e}")
    return rules


# ─── RULE EXECUTORS ─────────────────────────────────────────────────────────

def run_not_null(df: DataFrame, rule: QualityRule) -> Tuple[int, int]:
    total  = df.count()
    failed = df.filter(F.col(rule.target_column).isNull()).count()
    return total, total - failed


def run_unique(df: DataFrame, rule: QualityRule) -> Tuple[int, int]:
    total    = df.count()
    distinct = df.select(rule.target_column).distinct().count()
    failed   = total - distinct
    return total, distinct


def run_range(df: DataFrame, rule: QualityRule) -> Tuple[int, int]:
    params  = rule.rule_params
    min_val = params.get("min")
    max_val = params.get("max")
    total   = df.filter(F.col(rule.target_column).isNotNull()).count()

    cond = F.lit(True)
    if min_val is not None:
        cond = cond & (F.col(rule.target_column) >= min_val)
    if max_val is not None:
        cond = cond & (F.col(rule.target_column) <= max_val)

    passed = df.filter(F.col(rule.target_column).isNotNull()).filter(cond).count()
    return total, passed


def run_regex(df: DataFrame, rule: QualityRule) -> Tuple[int, int]:
    pattern = rule.rule_params.get("pattern", ".*")
    total   = df.filter(F.col(rule.target_column).isNotNull()).count()
    passed  = df.filter(
        F.col(rule.target_column).isNotNull() &
        F.col(rule.target_column).rlike(pattern)
    ).count()
    return total, passed


def run_row_count(df: DataFrame, rule: QualityRule) -> Tuple[int, int]:
    min_rows = rule.rule_params.get("min_rows", 1)
    total    = df.count()
    passed   = total if total >= min_rows else 0
    return total, passed


def run_freshness(df: DataFrame, rule: QualityRule) -> Tuple[int, int]:
    max_age_days = rule.rule_params.get("max_age_days", 1)
    total        = df.count()

    max_date_row = df.agg(F.max(F.col(rule.target_column))).collect()
    if not max_date_row or max_date_row[0][0] is None:
        return total, 0

    from datetime import date
    max_date = max_date_row[0][0]
    if hasattr(max_date, "date"):
        max_date = max_date.date()

    age_days = (date.today() - max_date).days
    passed   = total if age_days <= max_age_days else 0
    return total, passed


def run_completeness(df: DataFrame, rule: QualityRule) -> Tuple[int, int]:
    threshold = rule.rule_params.get("threshold_pct", 80)
    total     = df.count()
    non_null  = df.filter(F.col(rule.target_column).isNotNull()).count()
    return total, non_null


# ─── RULE DISPATCHER ────────────────────────────────────────────────────────

RULE_EXECUTORS = {
    "not_null":    run_not_null,
    "unique":      run_unique,
    "range":       run_range,
    "regex":       run_regex,
    "row_count":   run_row_count,
    "freshness":   run_freshness,
    "completeness":run_completeness,
}


def execute_rule(df: DataFrame, rule: QualityRule) -> QualityResult:
    """Dispatch rule to the appropriate executor and build result."""
    executor = RULE_EXECUTORS.get(rule.rule_type)
    if not executor:
        raise ValueError(f"Unknown rule type: {rule.rule_type}")

    total, passed = executor(df, rule)
    failed        = total - passed
    pass_rate     = round(passed / total * 100, 4) if total > 0 else 0.0

    # Determine status
    if pass_rate >= rule.threshold_pct:
        status = "PASS"
    elif rule.severity == "CRITICAL":
        status = "FAIL"
    else:
        status = "WARNING"

    return QualityResult(
        rule_id        = rule.rule_id,
        rule_name      = rule.rule_name,
        target_table   = rule.target_table,
        target_column  = rule.target_column,
        rule_type      = rule.rule_type,
        total_records  = total,
        passed_records = passed,
        failed_records = failed,
        pass_rate_pct  = pass_rate,
        threshold_pct  = rule.threshold_pct,
        status         = status,
        severity       = rule.severity,
        details        = {
            "rule_params":     rule.rule_params,
            "threshold_pct":   rule.threshold_pct,
        },
    )


# ─── RESULT WRITER ──────────────────────────────────────────────────────────

def write_results(
    results: List[QualityResult],
    run_id: str,
    pipeline_name: str,
    layer: str,
) -> None:
    """Bulk-insert quality results into data_quality_log."""
    sql = """
        INSERT INTO data_quality_log (
            rule_id, run_id, pipeline_name, layer,
            target_table, target_column, rule_type,
            total_records, passed_records, failed_records,
            pass_rate_pct, threshold_pct, status, severity, details
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s
        )
    """
    from psycopg2.extras import Json
    try:
        with psycopg2.connect(
            host=metadata_config.host, port=metadata_config.port,
            dbname=metadata_config.database,
            user=metadata_config.username, password=metadata_config.password,
        ) as conn:
            with conn.cursor() as cur:
                for r in results:
                    cur.execute(sql, (
                        r.rule_id, run_id, pipeline_name, layer,
                        r.target_table, r.target_column, r.rule_type,
                        r.total_records, r.passed_records, r.failed_records,
                        r.pass_rate_pct, r.threshold_pct,
                        r.status, r.severity,
                        Json(r.details),
                    ))
            conn.commit()
    except Exception as e:
        print(f"[DQ] Could not write results: {e}")


# ─── QUALITY REPORT ─────────────────────────────────────────────────────────

def print_quality_report(results: List[QualityResult]) -> None:
    passed   = [r for r in results if r.status == "PASS"]
    warnings = [r for r in results if r.status == "WARNING"]
    failures = [r for r in results if r.status == "FAIL"]

    print(f"\n{'─'*65}")
    print(f"  DATA QUALITY REPORT  |  {len(results)} rules checked")
    print(f"  ✓ PASS: {len(passed)}   ⚠ WARNING: {len(warnings)}   ✗ FAIL: {len(failures)}")
    print(f"{'─'*65}")

    for r in sorted(results, key=lambda x: (x.status != "FAIL", x.status, x.rule_name)):
        icon = "✓" if r.status == "PASS" else ("⚠" if r.status == "WARNING" else "✗")
        col  = f".{r.target_column}" if r.target_column else ""
        print(
            f"  {icon} [{r.severity:8}] {r.rule_name:45} "
            f"pass={r.pass_rate_pct:.1f}% "
            f"({r.passed_records}/{r.total_records})"
        )

    print(f"{'─'*65}\n")


# ─── MAIN FRAMEWORK ENTRY POINT ─────────────────────────────────────────────

def run_quality_checks(
    spark: SparkSession,
    layer: str,
    pipeline_name: str = "data_quality",
    raise_on_critical: bool = True,
) -> List[QualityResult]:
    """
    Load all active rules, execute them, persist results.
    Returns list of QualityResult objects.
    Raises RuntimeError if any CRITICAL rule fails and raise_on_critical=True.
    """
    run_id = generate_run_id(pipeline_name)
    print(f"\n[DQ] Starting quality checks for layer={layer} | run_id={run_id}")

    rules   = load_rules()
    results = []

    # Group rules by target table to avoid re-reading
    tables_needed = {r.target_table for r in rules}

    # Cache loaded DataFrames
    df_cache: Dict[str, DataFrame] = {}

    for table in tables_needed:
        try:
            # Map silver table names to paths
            # Convention: silver_customers → layer=silver, table=customers
            if table.startswith("silver_"):
                actual_table = table[len("silver_"):]
                path = storage_config.layer_path("silver", actual_table)
            elif table.startswith("gold_"):
                actual_table = table[len("gold_"):]
                path = storage_config.layer_path("gold", actual_table)
            else:
                path = storage_config.layer_path(layer, table)

            df = spark.read.format("delta").load(path)
            df.cache()
            df_cache[table] = df
            print(f"  [DQ] Loaded {table}: {df.count()} rows")
        except Exception as e:
            print(f"  [DQ] Could not load table '{table}': {e}")

    # Execute rules
    for rule in rules:
        df = df_cache.get(rule.target_table)
        if df is None:
            print(f"  [DQ] SKIP rule '{rule.rule_name}': table not loaded")
            continue

        try:
            result = execute_rule(df, rule)
            results.append(result)

            icon = "✓" if result.status == "PASS" else ("⚠" if result.status == "WARNING" else "✗")
            print(f"  {icon} {rule.rule_name}: {result.pass_rate_pct:.1f}% "
                  f"[{result.status}]")

        except Exception as e:
            print(f"  [DQ] ERROR in rule '{rule.rule_name}': {e}")
            log_error(run_id, pipeline_name, "DataQualityRule",
                      str(e), "ERROR", source_table=rule.target_table)

    # Write all results to metadata DB
    write_results(results, run_id, pipeline_name, layer)

    # Print report
    print_quality_report(results)

    # Check for critical failures
    critical_failures = [r for r in results if r.status == "FAIL" and r.severity == "CRITICAL"]
    if critical_failures and raise_on_critical:
        failed_rules = [r.rule_name for r in critical_failures]
        raise RuntimeError(
            f"Data Quality CRITICAL failures: {failed_rules}. "
            f"Pipeline halted. Check data_quality_log."
        )

    return results


# ─── NEGATIVE TEST: corrupt file injection ──────────────────────────────────

def inject_corrupt_data(spark: SparkSession, table_name: str, run_id: str) -> None:
    """
    Testing utility: inject known-bad records to verify DQ rules fire correctly.
    Only callable in dev/test environments.
    """
    from pipelines.config import ENV
    assert ENV in ("dev", "test"), "Corrupt injection only allowed in dev/test!"

    path = storage_config.layer_path("silver", table_name)

    if table_name == "customers":
        corrupt_df = spark.createDataFrame([
            # Missing email (not_null should catch)
            (9999, None, "Test", "Corrupt", None, "Athens", "GREECE", None, True),
            # Invalid email format (regex should catch)
            (9998, "not-an-email", "Bad", "Email", "+30-123", "Patras", "GREECE", None, True),
            # Duplicate email (unique should catch)
            (9997, "alex.papadimitriou@email.gr", "Dup", "User", None, "Athens", "GREECE", None, True),
        ], ["customer_id", "email", "first_name", "last_name", "phone", "city", "country", "age", "is_active"])

        corrupt_df.write.format("delta").mode("append").save(path)
        print(f"[DQ TEST] Injected 3 corrupt records into silver/{table_name}")
    else:
        print(f"[DQ TEST] No corruption template for '{table_name}'")


if __name__ == "__main__":
    spark = build_spark_session("RetailNova-DataQuality")
    try:
        results = run_quality_checks(spark, layer="silver", raise_on_critical=False)
        print(f"[DQ] Completed {len(results)} checks")
    finally:
        spark.stop()
