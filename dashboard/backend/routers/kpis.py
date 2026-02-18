"""KPI endpoints — monthly business metrics from Gold layer."""

from fastapi import APIRouter, HTTPException
from delta_reader import read_gold_table

router = APIRouter(prefix="/api/kpis", tags=["kpis"])


@router.get("/monthly")
def get_monthly_kpis():
    try:
        df = read_gold_table("agg_monthly_kpis")
        df = df.sort_values("year_month")

        # Convert numeric columns to Python floats
        records = []
        for _, row in df.iterrows():
            records.append({
                "year_month": str(row.get("year_month", "")),
                "order_year": int(row.get("order_year", 0)),
                "order_month": int(row.get("order_month", 0)),
                "total_revenue": round(float(row.get("total_revenue", 0)), 2),
                "total_orders": int(row.get("total_orders", 0)),
                "unique_customers": int(row.get("unique_customers", 0)),
                "avg_order_value": round(float(row.get("avg_order_value", 0)), 2),
                "avg_basket_size": round(float(row.get("avg_basket_size", 0)), 2) if row.get("avg_basket_size") else None,
                "avg_basket_lines": round(float(row.get("avg_basket_lines", 0)), 2) if row.get("avg_basket_lines") else None,
                "repeat_purchase_rate": round(float(row.get("repeat_purchase_rate", 0)), 2),
                "avg_customer_ltv": round(float(row.get("avg_customer_ltv", 0)), 2),
                "median_customer_ltv": round(float(row.get("median_customer_ltv", 0)), 2),
            })
        return {"monthly_kpis": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
