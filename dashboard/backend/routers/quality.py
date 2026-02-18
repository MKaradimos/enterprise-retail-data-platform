"""Data quality endpoints — rules, log, summary."""

from fastapi import APIRouter, HTTPException
from db import query

router = APIRouter(prefix="/api/quality", tags=["quality"])


@router.get("/rules")
def get_rules():
    try:
        rows = query("""
            SELECT rule_id, rule_name, target_table, target_column,
                   rule_type, rule_params, threshold_pct, severity, is_active
            FROM data_quality_rules
            ORDER BY target_table, rule_name
        """)
        return {"rules": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/log")
def get_log():
    try:
        rows = query("""
            SELECT log_id, rule_id, run_id, pipeline_name, layer,
                   target_table, target_column, rule_type,
                   total_records, passed_records, failed_records,
                   ROUND(pass_rate_pct::numeric, 2) AS pass_rate_pct,
                   threshold_pct, status, severity, details, run_at
            FROM data_quality_log
            ORDER BY run_at DESC
            LIMIT 100
        """)
        return {"log": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/summary")
def get_summary():
    try:
        rows = query("""
            SELECT status, COUNT(*) AS count
            FROM data_quality_log
            WHERE run_at = (SELECT MAX(run_at) FROM data_quality_log)
            GROUP BY status
            ORDER BY status
        """)
        total = sum(r["count"] for r in rows)
        return {
            "summary": rows,
            "total_checks": total,
            "pass_rate": round(
                next((r["count"] for r in rows if r["status"] == "PASS"), 0)
                / total * 100, 1
            ) if total else None,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
