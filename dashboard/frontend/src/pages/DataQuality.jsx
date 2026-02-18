import { useEffect, useState } from "react";
import api from "../api";
import MetricCard from "../components/MetricCard";
import StatusBadge from "../components/StatusBadge";

export default function DataQuality() {
  const [summary, setSummary] = useState(null);
  const [rules, setRules] = useState([]);
  const [log, setLog] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.get("/api/quality/summary"),
      api.get("/api/quality/rules"),
      api.get("/api/quality/log"),
    ]).then(([s, r, l]) => {
      setSummary(s.data);
      setRules(r.data.rules || []);
      setLog(l.data.log || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) return <p className="loading">Loading...</p>;

  const summaryItems = summary?.summary || [];

  return (
    <div>
      <h1>Data Quality</h1>

      <div className="cards-grid">
        <MetricCard
          title="Overall Pass Rate"
          value={summary?.pass_rate != null ? `${summary.pass_rate}%` : "N/A"}
          subtitle={`${summary?.total_checks || 0} checks`}
          color="#059669"
        />
        {summaryItems.map((s, i) => (
          <MetricCard
            key={i}
            title={s.status}
            value={s.count}
            color={s.status === "PASS" ? "#059669" : s.status === "FAIL" ? "#dc2626" : "#d97706"}
          />
        ))}
      </div>

      <h2>Rules</h2>
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>Rule</th>
              <th>Table</th>
              <th>Column</th>
              <th>Type</th>
              <th>Threshold</th>
              <th>Severity</th>
            </tr>
          </thead>
          <tbody>
            {rules.map((r) => (
              <tr key={r.rule_id}>
                <td>{r.rule_name}</td>
                <td>{r.target_table}</td>
                <td>{r.target_column || "-"}</td>
                <td>{r.rule_type}</td>
                <td>{r.threshold_pct}%</td>
                <td><StatusBadge status={r.severity} /></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <h2>Latest Results</h2>
      <div className="table-container">
        <table>
          <thead>
            <tr>
              <th>Table</th>
              <th>Rule Type</th>
              <th>Pass Rate</th>
              <th>Status</th>
              <th>Severity</th>
              <th>Run At</th>
            </tr>
          </thead>
          <tbody>
            {log.map((l) => (
              <tr key={l.log_id}>
                <td>{l.target_table}</td>
                <td>{l.rule_type}</td>
                <td>{l.pass_rate_pct != null ? `${l.pass_rate_pct}%` : "-"}</td>
                <td><StatusBadge status={l.status} /></td>
                <td><StatusBadge status={l.severity} /></td>
                <td>{new Date(l.run_at).toLocaleString()}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
