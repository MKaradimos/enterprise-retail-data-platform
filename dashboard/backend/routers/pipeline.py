"""Pipeline monitoring — executions, errors, watermarks."""

from fastapi import APIRouter, HTTPException, Query
from db import query

router = APIRouter(prefix="/api/pipeline", tags=["pipeline"])


@router.get("/executions")
def get_executions(limit: int = Query(50, ge=1, le=200)):
    try:
        rows = query("""
            SELECT log_id, run_id, pipeline_name, pipeline_type, layer,
                   source_table, target_table, status,
                   rows_read, rows_written, rows_skipped, rows_failed,
                   started_at, completed_at,
                   ROUND(duration_seconds::numeric, 1) AS duration_secs,
                   error_message
            FROM execution_log
            ORDER BY started_at DESC
            LIMIT %s
        """, (limit,))
        return {"executions": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/errors")
def get_errors(limit: int = Query(50, ge=1, le=200)):
    try:
        rows = query("""
            SELECT error_id, run_id, pipeline_name, error_type, severity,
                   error_message, source_table, target_table,
                   record_count, is_resolved, occurred_at
            FROM error_log
            ORDER BY occurred_at DESC
            LIMIT %s
        """, (limit,))
        return {"errors": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/watermarks")
def get_watermarks():
    try:
        rows = query("""
            SELECT table_name, last_watermark, last_run_at,
                   rows_extracted, status, updated_at
            FROM pipeline_watermarks
            ORDER BY table_name
        """)
        return {"watermarks": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
