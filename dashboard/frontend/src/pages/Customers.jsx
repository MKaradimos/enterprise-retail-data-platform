import { useEffect, useState } from "react";
import {
  PieChart, Pie, Cell, Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import api from "../api";

const COLORS = ["#4f46e5", "#059669", "#d97706", "#dc2626", "#0891b2", "#7c3aed", "#ec4899"];

export default function Customers() {
  const [segments, setSegments] = useState(null);
  const [cohorts, setCohorts] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.get("/api/customers/segments"),
      api.get("/api/customers/cohorts"),
    ]).then(([seg, coh]) => {
      setSegments(seg.data);
      setCohorts(coh.data);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) return <p className="loading">Loading...</p>;

  // Build cohort heatmap data
  const cohortData = cohorts?.cohorts || [];
  const cohortLabels = [...new Set(cohortData.map((c) => c.cohort_label))];
  const periods = [...new Set(cohortData.map((c) => c.order_period_months))].sort((a, b) => a - b);
  const cohortMap = {};
  cohortData.forEach((c) => {
    if (!cohortMap[c.cohort_label]) cohortMap[c.cohort_label] = {};
    cohortMap[c.cohort_label][c.order_period_months] = c.retention_rate;
  });

  const retentionColor = (rate) => {
    if (rate >= 80) return "#166534";
    if (rate >= 60) return "#15803d";
    if (rate >= 40) return "#ca8a04";
    if (rate >= 20) return "#ea580c";
    return "#dc2626";
  };

  const retentionBg = (rate) => {
    if (rate >= 80) return "#dcfce7";
    if (rate >= 60) return "#d9f99d";
    if (rate >= 40) return "#fef9c3";
    if (rate >= 20) return "#fed7aa";
    return "#fee2e2";
  };

  const segDist = segments?.segment_distribution || [];

  return (
    <div>
      <h1>Customer Analytics</h1>

      <h2>RFM Segments</h2>
      <div className="two-col">
        <div className="chart-container">
          <ResponsiveContainer width="100%" height={300}>
            <PieChart>
              <Pie
                data={segDist}
                dataKey="count"
                nameKey="segment"
                cx="50%"
                cy="50%"
                outerRadius={100}
                label={({ segment, count }) => `${segment} (${count})`}
              >
                {segDist.map((_, i) => (
                  <Cell key={i} fill={COLORS[i % COLORS.length]} />
                ))}
              </Pie>
              <Tooltip />
              <Legend />
            </PieChart>
          </ResponsiveContainer>
        </div>

        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>Segment</th>
                <th>Count</th>
                <th>Avg Monetary</th>
                <th>Avg Frequency</th>
                <th>Avg Recency</th>
              </tr>
            </thead>
            <tbody>
              {segDist.map((s, i) => (
                <tr key={i}>
                  <td>
                    <span style={{
                      display: "inline-block",
                      width: 10,
                      height: 10,
                      borderRadius: "50%",
                      backgroundColor: COLORS[i % COLORS.length],
                      marginRight: 6,
                    }} />
                    {s.segment}
                  </td>
                  <td>{s.count}</td>
                  <td>${s.avg_monetary.toLocaleString()}</td>
                  <td>{s.avg_frequency}</td>
                  <td>{s.avg_recency} days</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      <h2>Cohort Retention</h2>
      <div className="table-container">
        <table className="cohort-table">
          <thead>
            <tr>
              <th>Cohort</th>
              {periods.map((p) => (
                <th key={p}>M{p}</th>
              ))}
            </tr>
          </thead>
          <tbody>
            {cohortLabels.map((label) => (
              <tr key={label}>
                <td>{label}</td>
                {periods.map((p) => {
                  const rate = cohortMap[label]?.[p];
                  return (
                    <td
                      key={p}
                      style={{
                        backgroundColor: rate != null ? retentionBg(rate) : "#f9fafb",
                        color: rate != null ? retentionColor(rate) : "#9ca3af",
                        fontWeight: 600,
                        textAlign: "center",
                      }}
                    >
                      {rate != null ? `${rate}%` : "-"}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
