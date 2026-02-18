"""Customer analytics — RFM segments + cohort retention."""

from fastapi import APIRouter, HTTPException
from delta_reader import read_gold_table

router = APIRouter(prefix="/api/customers", tags=["customers"])


@router.get("/segments")
def get_segments():
    try:
        df = read_gold_table("customer_segments")

        # Segment distribution
        segment_dist = (
            df.groupby("segment")
            .agg(
                count=("customer_id", "count"),
                avg_monetary=("monetary", "mean"),
                avg_frequency=("frequency", "mean"),
                avg_recency=("recency_days", "mean"),
            )
            .reset_index()
        )
        segments = []
        for _, row in segment_dist.iterrows():
            segments.append({
                "segment": row["segment"],
                "count": int(row["count"]),
                "avg_monetary": round(float(row["avg_monetary"]), 2),
                "avg_frequency": round(float(row["avg_frequency"]), 2),
                "avg_recency": round(float(row["avg_recency"]), 1),
            })

        # Individual customers with scores
        customers = []
        for _, row in df.iterrows():
            customers.append({
                "customer_id": int(row["customer_id"]),
                "recency_days": int(row["recency_days"]),
                "frequency": int(row["frequency"]),
                "monetary": round(float(row["monetary"]), 2),
                "r_score": int(row["r_score"]),
                "f_score": int(row["f_score"]),
                "m_score": int(row["m_score"]),
                "rfm_score": int(row["rfm_score"]),
                "segment": row["segment"],
            })

        return {"segment_distribution": segments, "customers": customers}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/cohorts")
def get_cohorts():
    try:
        df = read_gold_table("cohort_analysis")
        df = df.sort_values(["cohort_year", "cohort_month", "order_period_months"])

        records = []
        for _, row in df.iterrows():
            records.append({
                "cohort_label": str(row.get("cohort_label", "")),
                "cohort_year": int(row["cohort_year"]),
                "cohort_month": int(row["cohort_month"]),
                "order_period_months": int(row["order_period_months"]),
                "active_customers": int(row["active_customers"]),
                "cohort_size": int(row["cohort_size"]),
                "retention_rate": round(float(row.get("retention_rate", 0)), 2),
            })
        return {"cohorts": records}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
