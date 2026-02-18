import { useEffect, useState } from "react";
import api from "../api";
import MetricCard from "../components/MetricCard";
import StatusBadge from "../components/StatusBadge";

export default function Overview() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get("/api/overview").then((r) => {
      setData(r.data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) return <p className="loading">Loading...</p>;
  if (!data) return <p className="error">Failed to load overview</p>;

  const kpi = data.headline_kpis || {};

  return (
    <div>
      <h1>Overview</h1>

      <div className="cards-grid">
        <MetricCard
          title="Revenue"
          value={kpi.total_revenue ? `$${kpi.total_revenue.toLocaleString()}` : "N/A"}
          subtitle={kpi.year_month}
          color="#4f46e5"
        />
        <MetricCard
          title="Orders"
          value={kpi.total_orders ?? "N/A"}
          subtitle={kpi.year_month}
          color="#0891b2"
        />
        <MetricCard
          title="Customers"
          value={kpi.unique_customers ?? "N/A"}
          subtitle={kpi.year_month}
          color="#059669"
        />
        <MetricCard
          title="DQ Pass Rate"
          value={data.dq_pass_rate != null ? `${data.dq_pass_rate}%` : "N/A"}
          subtitle="Latest run"
          color="#d97706"
        />
      </div>

      <h2>Latest Pipeline Runs</h2>
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>Pipeline</th>
              <th>Status</th>
              <th>Rows</th>
              <th>Duration</th>
              <th>Started</th>
            </tr>
          </thead>
          <tbody>
            {(data.latest_runs || []).map((r, i) => (
              <tr key={i}>
                <td>{r.pipeline_name}</td>
                <td><StatusBadge status={r.status} /></td>
                <td>{r.rows_written?.toLocaleString()}</td>
                <td>{r.duration_secs}s</td>
                <td>{new Date(r.started_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2>Status Summary (Last 7 Days)</h2>
      <div className="cards-grid">
        {(data.status_counts || []).map((s, i) => (
          <MetricCard
            key={i}
            title={s.status}
            value={s.count}
            color={s.status === "SUCCESS" ? "#059669" : s.status === "FAILED" ? "#dc2626" : "#6b7280"}
          />
        ))}
      </div>
    </div>
  );
}
