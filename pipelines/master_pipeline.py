"""
RetailNova - Master Pipeline Orchestrator
==========================================
Simulates Azure Data Factory Master Pipeline with child pipelines.

Pattern: Master → Child Pipelines with:
  - Parameterisation
  - Retry with exponential backoff
  - Conditional branching (skip gold if silver fails)
  - Alert simulation (email/teams notification)
  - SLA monitoring
  - Metadata-driven execution

This is the single entry point to run the full ETL stack.
"""

import sys
import time
import argparse
from datetime import datetime, timezone
from typing import List, Optional, Dict, Any, Callable
from enum import Enum

sys.path.insert(0, "/opt/bitnami/spark")

from pipelines.config            import pipeline_config, BRONZE_TABLES
from pipelines.logger            import pipeline_run, log_error, log_metric, generate_run_id
from pipelines.spark_session     import build_spark_session, stop_session
from pipelines.bronze_ingestion  import run_bronze_ingestion
from pipelines.silver_transformation import run_silver_transformation
from pipelines.gold_pipeline     import run_gold_pipeline
from quality_framework.dq_engine import run_quality_checks


# ─── PIPELINE STATUS ENUM ───────────────────────────────────────────────────

class PipelineStatus(str, Enum):
    PENDING   = "PENDING"
    RUNNING   = "RUNNING"
    SUCCESS   = "SUCCESS"
    FAILED    = "FAILED"
    SKIPPED   = "SKIPPED"
    RETRYING  = "RETRYING"


# ─── ALERT SIMULATION ───────────────────────────────────────────────────────

def send_alert(
    subject: str,
    message: str,
    severity: str = "WARNING",
    recipients: List[str] = None,
) -> None:
    """
    Simulate Azure Monitor alert / Teams webhook notification.

    In production: this calls Azure Logic Apps → sends email/Teams message.
    For local dev: just prints the alert with formatting.
    """
    border = "=" * 65
    print(f"\n{'🔴' if severity == 'CRITICAL' else '⚠️ '} ALERT [{severity}]")
    print(border)
    print(f"  Subject  : {subject}")
    print(f"  Message  : {message}")
    print(f"  Time     : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Recipients: {recipients or [pipeline_config.alert_email]}")
    print(border + "\n")


def check_sla(start_time: datetime, pipeline_name: str) -> None:
    """Warn if pipeline is approaching SLA threshold."""
    elapsed_hours = (datetime.now(timezone.utc) - start_time).total_seconds() / 3600
    if elapsed_hours > pipeline_config.sla_hours:
        send_alert(
            subject=f"SLA BREACH: {pipeline_name}",
            message=(
                f"Pipeline '{pipeline_name}' has been running for "
                f"{elapsed_hours:.1f}h, exceeding SLA of {pipeline_config.sla_hours}h."
            ),
            severity="CRITICAL",
        )


# ─── RETRY WRAPPER ──────────────────────────────────────────────────────────

def run_with_retry(
    fn: Callable,
    pipeline_name: str,
    max_retries: int = None,
    retry_delay: int = None,
    **kwargs,
) -> bool:
    """
    Execute a pipeline function with exponential backoff retry.
    Returns True on success, False if all retries exhausted.

    In Azure Data Factory: this maps to Activity Retry Policy settings.
    """
    max_retries  = max_retries  or pipeline_config.max_retries
    retry_delay  = retry_delay  or pipeline_config.retry_delay_secs
    attempt      = 0

    while attempt <= max_retries:
        try:
            if attempt > 0:
                wait = retry_delay * (2 ** (attempt - 1))   # exponential backoff
                print(f"  [RETRY] {pipeline_name} attempt {attempt}/{max_retries} "
                      f"in {wait}s...")
                time.sleep(wait)

            fn(**kwargs)
            return True

        except Exception as exc:
            attempt += 1
            print(f"  [ERROR] {pipeline_name} attempt {attempt} failed: {exc}")

            if attempt > max_retries:
                send_alert(
                    subject=f"PIPELINE FAILURE: {pipeline_name}",
                    message=f"All {max_retries} retries exhausted. Error: {exc}",
                    severity="CRITICAL",
                )
                return False

    return False


# ─── CHILD PIPELINE: BRONZE ─────────────────────────────────────────────────

def child_pipeline_bronze(spark, tables: List[str] = None) -> PipelineStatus:
    print(f"\n  ╔══ CHILD PIPELINE: Bronze Ingestion ══╗")
    success = run_with_retry(
        run_bronze_ingestion,
        "bronze_ingestion",
        spark=spark,
        tables=tables,
    )
    status = PipelineStatus.SUCCESS if success else PipelineStatus.FAILED
    print(f"  ╚══ Bronze: {status.value} ══╝")
    return status


# ─── CHILD PIPELINE: SILVER ─────────────────────────────────────────────────

def child_pipeline_silver(spark) -> PipelineStatus:
    print(f"\n  ╔══ CHILD PIPELINE: Silver Transformation ══╗")
    success = run_with_retry(
        run_silver_transformation,
        "silver_transformation",
        spark=spark,
    )
    status = PipelineStatus.SUCCESS if success else PipelineStatus.FAILED
    print(f"  ╚══ Silver: {status.value} ══╝")
    return status


# ─── CHILD PIPELINE: QUALITY ────────────────────────────────────────────────

def child_pipeline_quality(spark, layer: str = "silver") -> PipelineStatus:
    print(f"\n  ╔══ CHILD PIPELINE: Data Quality ({layer}) ══╗")
    try:
        results = run_quality_checks(
            spark, layer=layer,
            pipeline_name=f"dq_{layer}",
            raise_on_critical=False,  # Orchestrator handles this
        )
        # Count failures
        critical_fails = [r for r in results if r.status == "FAIL" and r.severity == "CRITICAL"]
        warnings       = [r for r in results if r.status == "WARNING"]

        if warnings:
            send_alert(
                subject=f"Data Quality Warnings ({layer})",
                message=f"{len(warnings)} quality warnings found. Review data_quality_log.",
                severity="WARNING",
            )

        if critical_fails:
            send_alert(
                subject=f"Data Quality CRITICAL FAILURES ({layer})",
                message=(
                    f"{len(critical_fails)} critical rules failed: "
                    f"{[r.rule_name for r in critical_fails]}"
                ),
                severity="CRITICAL",
            )
            print(f"  ╚══ Quality ({layer}): FAILED ══╝")
            return PipelineStatus.FAILED

        print(f"  ╚══ Quality ({layer}): SUCCESS ══╝")
        return PipelineStatus.SUCCESS

    except Exception as e:
        print(f"  [Quality ERROR] {e}")
        print(f"  ╚══ Quality ({layer}): FAILED ══╝")
        return PipelineStatus.FAILED


# ─── CHILD PIPELINE: GOLD ───────────────────────────────────────────────────

def child_pipeline_gold(spark) -> PipelineStatus:
    print(f"\n  ╔══ CHILD PIPELINE: Gold Layer ══╗")
    success = run_with_retry(
        run_gold_pipeline,
        "gold_pipeline",
        spark=spark,
    )
    status = PipelineStatus.SUCCESS if success else PipelineStatus.FAILED
    print(f"  ╚══ Gold: {status.value} ══╝")
    return status


# ─── MASTER PIPELINE ────────────────────────────────────────────────────────

def run_master_pipeline(
    run_bronze: bool = True,
    run_silver: bool = True,
    run_gold:   bool = True,
    run_quality: bool = True,
    tables: List[str] = None,
    fail_fast: bool = False,
) -> Dict[str, PipelineStatus]:
    """
    Master pipeline that orchestrates all child pipelines.

    Branching logic:
      - Bronze fail → Silver skipped (no source data)
      - Silver fail → Gold skipped + Quality WARN (no clean data)
      - Quality critical fail → Alert but Gold continues (data delivered)
      - Gold fail → Alert, upstream data still available in Silver

    Parameterisation:
      - tables: run only specific source tables
      - fail_fast: stop entire pipeline on first failure
    """
    master_start = datetime.now(timezone.utc)
    master_run_id = generate_run_id("master_pipeline")

    print(f"\n{'╔' + '═'*63 + '╗'}")
    print(f"║  RETAILNOVA MASTER PIPELINE                                  ║")
    print(f"║  run_id : {master_run_id:<52} ║")
    print(f"║  start  : {master_start.strftime('%Y-%m-%d %H:%M:%S UTC'):<52} ║")
    print(f"╚{'═'*63}╝")

    spark   = build_spark_session("RetailNova-Master-Pipeline")
    results = {}

    try:
        # ── STAGE 1: Bronze Ingestion ────────────────────────────────────
        if run_bronze:
            results["bronze"] = child_pipeline_bronze(spark, tables)
            check_sla(master_start, "bronze_ingestion")

            if results["bronze"] == PipelineStatus.FAILED:
                send_alert("Bronze Ingestion Failed",
                           "Cannot proceed to Silver without source data.",
                           "CRITICAL")
                if fail_fast:
                    return results
                # Skip Silver and Gold — no source data
                results["silver"]  = PipelineStatus.SKIPPED
                results["quality"] = PipelineStatus.SKIPPED
                results["gold"]    = PipelineStatus.SKIPPED
                return results

        # ── STAGE 2: Silver Transformation ───────────────────────────────
        if run_silver:
            results["silver"] = child_pipeline_silver(spark)
            check_sla(master_start, "silver_transformation")

            if results["silver"] == PipelineStatus.FAILED:
                send_alert("Silver Transformation Failed",
                           "Gold pipeline will be skipped.",
                           "CRITICAL")
                if fail_fast:
                    return results

        # ── STAGE 3: Data Quality (Silver) ────────────────────────────────
        if run_quality and results.get("silver") != PipelineStatus.SKIPPED:
            results["quality_silver"] = child_pipeline_quality(spark, "silver")

        # ── STAGE 4: Gold Layer ───────────────────────────────────────────
        if run_gold and results.get("silver") != PipelineStatus.FAILED:
            results["gold"] = child_pipeline_gold(spark)
            check_sla(master_start, "gold_pipeline")

            if results["gold"] == PipelineStatus.SUCCESS and run_quality:
                results["quality_gold"] = child_pipeline_quality(spark, "gold")
        else:
            if results.get("silver") == PipelineStatus.FAILED:
                results["gold"] = PipelineStatus.SKIPPED
                print("  [SKIP] Gold pipeline skipped due to Silver failure.")

        # ── SUMMARY ──────────────────────────────────────────────────────
        elapsed = (datetime.now(timezone.utc) - master_start).total_seconds()
        log_metric(master_run_id, "master_pipeline", "total_duration_seconds",
                   elapsed, "seconds")

        print(f"\n{'╔' + '═'*63 + '╗'}")
        print(f"║  MASTER PIPELINE SUMMARY                                    ║")
        print(f"╠{'═'*63}╣")
        for stage, status in results.items():
            icon = "✓" if status == PipelineStatus.SUCCESS else (
                   "○" if status == PipelineStatus.SKIPPED else "✗")
            print(f"║  {icon} {stage:<20} : {status.value:<38} ║")
        print(f"╠{'═'*63}╣")
        print(f"║  Duration: {elapsed:.1f}s{' ':>50}║")
        print(f"╚{'═'*63}╝\n")

        # Final SLA check
        check_sla(master_start, "master_pipeline")

        return results

    except Exception as e:
        send_alert(
            "MASTER PIPELINE FATAL ERROR",
            str(e),
            "CRITICAL",
        )
        log_error(master_run_id, "master_pipeline", "FatalError", str(e), "CRITICAL")
        raise

    finally:
        stop_session(spark)


# ─── CLI ENTRY POINT ────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="RetailNova Enterprise Data Platform - Master Pipeline"
    )
    parser.add_argument(
        "--layer", choices=["all", "bronze", "silver", "gold", "quality"],
        default="all", help="Which layer(s) to run"
    )
    parser.add_argument(
        "--tables", nargs="+", default=None,
        help="Specific source tables to ingest (bronze only)"
    )
    parser.add_argument(
        "--fail-fast", action="store_true",
        help="Stop pipeline on first failure"
    )
    parser.add_argument(
        "--skip-quality", action="store_true",
        help="Skip data quality checks"
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    run_master_pipeline(
        run_bronze  = args.layer in ("all", "bronze"),
        run_silver  = args.layer in ("all", "silver"),
        run_gold    = args.layer in ("all", "gold"),
        run_quality = not args.skip_quality,
        tables      = args.tables,
        fail_fast   = args.fail_fast,
    )
