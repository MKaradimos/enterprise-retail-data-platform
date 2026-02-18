"""Overview endpoint — headline metrics + latest runs."""

from fastapi import APIRouter, HTTPException
from db import query
from delta_reader import read_gold_table

router = APIRouter(prefix="/api/overview", tags=["overview"])


@router.get("")
def get_overview():
    try:
        # Latest pipeline executions
        latest_runs = query("""
            SELECT pipeline_name, status, rows_written,
                   ROUND(duration_seconds::numeric, 1) AS duration_secs,
                   started_at, completed_at
            FROM execution_log
            ORDER BY started_at DESC
            LIMIT 10
        """)

        # Status counts
        status_counts = query("""
            SELECT status, COUNT(*) AS count
            FROM execution_log
            WHERE started_at >= NOW() - INTERVAL '7 days'
            GROUP BY status
            ORDER BY count DESC
        """)

        # DQ summary
        dq_summary = query("""
            SELECT status, COUNT(*) AS count
            FROM data_quality_log
            WHERE run_at = (SELECT MAX(run_at) FROM data_quality_log)
            GROUP BY status
        """)

        # Headline KPIs from Gold
        kpis = {}
        try:
            df = read_gold_table("agg_monthly_kpis")
            if not df.empty:
                latest = df.sort_values("year_month", ascending=False).iloc[0]
                kpis = {
                    "total_revenue": round(float(latest.get("total_revenue", 0)), 2),
                    "total_orders": int(latest.get("total_orders", 0)),
                    "unique_customers": int(latest.get("unique_customers", 0)),
                    "avg_order_value": round(float(latest.get("avg_order_value", 0)), 2),
                    "year_month": str(latest.get("year_month", "")),
                }
        except Exception:
            kpis = {"error": "Gold tables not available"}

        # DQ pass rate
        dq_pass_rate = None
        for row in dq_summary:
            if row["status"] == "PASS":
                total = sum(r["count"] for r in dq_summary)
                dq_pass_rate = round(row["count"] / total * 100, 1) if total else None

        return {
            "latest_runs": latest_runs,
            "status_counts": status_counts,
            "dq_summary": dq_summary,
            "dq_pass_rate": dq_pass_rate,
            "headline_kpis": kpis,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
