"""
RetailNova - Pipeline Logger
====================================
Structured logging to PostgreSQL metadata tables.
Provides: run tracking, duration capture, error recording,
          metric recording, SLA alerting.
"""

import uuid
import json
import traceback
from datetime import datetime, timezone
from contextlib import contextmanager
from typing import Optional, Dict, Any
import psycopg2
from psycopg2.extras import Json
from pipelines.config import metadata_config, pipeline_config


def _get_conn():
    return psycopg2.connect(
        host=metadata_config.host,
        port=metadata_config.port,
        dbname=metadata_config.database,
        user=metadata_config.username,
        password=metadata_config.password,
        connect_timeout=10,
    )


def generate_run_id(pipeline_name: str) -> str:
    """Generate a unique run ID: pipeline_name_YYYYMMDD_HHMMSS_uuid4[:8]"""
    ts  = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    uid = str(uuid.uuid4())[:8]
    safe_name = pipeline_name.replace(" ", "_").lower()
    return f"{safe_name}_{ts}_{uid}"


def log_execution_start(
    run_id: str,
    pipeline_name: str,
    pipeline_type: str = "ingestion",
    layer: str = None,
    source_table: str = None,
    target_table: str = None,
    metadata: Dict[str, Any] = None,
) -> None:
    """Insert a RUNNING record into execution_log."""
    sql = """
        INSERT INTO execution_log
            (run_id, pipeline_name, pipeline_type, layer,
             source_table, target_table, status, started_at, metadata)
        VALUES (%s, %s, %s, %s, %s, %s, 'RUNNING', NOW(), %s)
        ON CONFLICT DO NOTHING
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    run_id, pipeline_name, pipeline_type, layer,
                    source_table, target_table,
                    Json(metadata or {})
                ))
            conn.commit()
    except Exception as e:
        print(f"[LOGGER WARNING] Could not write execution start: {e}")


def log_execution_end(
    run_id: str,
    pipeline_name: str,
    status: str,           # SUCCESS | FAILED | SKIPPED
    rows_read: int = 0,
    rows_written: int = 0,
    rows_skipped: int = 0,
    rows_failed: int = 0,
    error_message: str = None,
    metadata: Dict[str, Any] = None,
) -> None:
    """Update execution_log with final status and metrics."""
    sql = """
        UPDATE execution_log
        SET
            status           = %s,
            rows_read        = %s,
            rows_written     = %s,
            rows_skipped     = %s,
            rows_failed      = %s,
            completed_at     = NOW(),
            duration_seconds = EXTRACT(EPOCH FROM (NOW() - started_at)),
            error_message    = %s,
            metadata         = metadata || %s::jsonb
        WHERE run_id = %s AND pipeline_name = %s
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    status, rows_read, rows_written,
                    rows_skipped, rows_failed, error_message,
                    Json(metadata or {}),
                    run_id, pipeline_name
                ))
            conn.commit()
    except Exception as e:
        print(f"[LOGGER WARNING] Could not write execution end: {e}")


def log_error(
    run_id: str,
    pipeline_name: str,
    error_type: str,
    error_message: str,
    severity: str = "ERROR",
    source_table: str = None,
    target_table: str = None,
    stack_trace: str = None,
    record_count: int = None,
) -> None:
    """Insert a record into error_log."""
    sql = """
        INSERT INTO error_log
            (run_id, pipeline_name, error_type, severity,
             error_message, stack_trace, source_table, target_table, record_count)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    run_id, pipeline_name, error_type, severity,
                    error_message, stack_trace, source_table, target_table,
                    record_count
                ))
            conn.commit()
    except Exception as e:
        print(f"[LOGGER WARNING] Could not write error: {e}")


def log_metric(
    run_id: str,
    pipeline_name: str,
    metric_name: str,
    metric_value: float,
    metric_unit: str = "count",
    layer: str = None,
) -> None:
    """Insert a single metric into pipeline_metrics."""
    sql = """
        INSERT INTO pipeline_metrics
            (run_id, pipeline_name, metric_date, metric_name,
             metric_value, metric_unit, layer)
        VALUES (%s, %s, CURRENT_DATE, %s, %s, %s, %s)
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (
                    run_id, pipeline_name, metric_name,
                    metric_value, metric_unit, layer
                ))
            conn.commit()
    except Exception as e:
        print(f"[LOGGER WARNING] Could not write metric: {e}")


def get_watermark(table_name: str) -> datetime:
    """Read the current watermark for a source table."""
    sql = "SELECT last_watermark FROM pipeline_watermarks WHERE table_name = %s"
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (table_name,))
                row = cur.fetchone()
                if row:
                    return row[0]
    except Exception as e:
        print(f"[LOGGER WARNING] Could not read watermark for {table_name}: {e}")
    return datetime(1900, 1, 1, tzinfo=timezone.utc)


def update_watermark(
    table_name: str,
    new_watermark: datetime,
    rows_extracted: int = 0,
    status: str = "SUCCESS",
) -> None:
    """Update the high-water mark after a successful extraction."""
    sql = """
        INSERT INTO pipeline_watermarks
            (table_name, last_watermark, last_run_at, rows_extracted, status, updated_at)
        VALUES (%s, %s, NOW(), %s, %s, NOW())
        ON CONFLICT (table_name) DO UPDATE SET
            last_watermark = EXCLUDED.last_watermark,
            last_run_at    = EXCLUDED.last_run_at,
            rows_extracted = EXCLUDED.rows_extracted,
            status         = EXCLUDED.status,
            updated_at     = NOW()
    """
    try:
        with _get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (table_name, new_watermark, rows_extracted, status))
            conn.commit()
    except Exception as e:
        print(f"[LOGGER WARNING] Could not update watermark for {table_name}: {e}")


@contextmanager
def pipeline_run(
    pipeline_name: str,
    pipeline_type: str = "ingestion",
    layer: str = None,
    source_table: str = None,
    target_table: str = None,
):
    """
    Context manager for pipeline execution tracking.

    Usage:
        with pipeline_run("bronze_ingestion", layer="bronze") as ctx:
            ctx["run_id"]  # use the run ID
            ctx["rows_written"] = 100
    """
    run_id = generate_run_id(pipeline_name)
    ctx = {
        "run_id":        run_id,
        "rows_read":     0,
        "rows_written":  0,
        "rows_skipped":  0,
        "rows_failed":   0,
    }

    log_execution_start(
        run_id, pipeline_name, pipeline_type,
        layer, source_table, target_table
    )

    try:
        yield ctx
        log_execution_end(
            run_id, pipeline_name, "SUCCESS",
            ctx["rows_read"],    ctx["rows_written"],
            ctx["rows_skipped"], ctx["rows_failed"]
        )
        print(f"[✓] {pipeline_name} | run_id={run_id} | "
              f"rows_written={ctx['rows_written']} | SUCCESS")

    except Exception as exc:
        tb = traceback.format_exc()
        log_execution_end(
            run_id, pipeline_name, "FAILED",
            ctx["rows_read"], ctx["rows_written"],
            ctx["rows_skipped"], ctx["rows_failed"],
            error_message=str(exc)
        )
        log_error(
            run_id, pipeline_name,
            error_type=type(exc).__name__,
            error_message=str(exc),
            severity="CRITICAL",
            stack_trace=tb,
            source_table=source_table,
            target_table=target_table,
        )
        print(f"[✗] {pipeline_name} | run_id={run_id} | FAILED: {exc}")
        raise
