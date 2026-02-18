import { useEffect, useState } from "react";
import api from "../api";
import StatusBadge from "../components/StatusBadge";

const TABS = ["Executions", "Errors", "Watermarks"];

export default function PipelineMonitor() {
  const [tab, setTab] = useState("Executions");
  const [executions, setExecutions] = useState([]);
  const [errors, setErrors] = useState([]);
  const [watermarks, setWatermarks] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    Promise.all([
      api.get("/api/pipeline/executions"),
      api.get("/api/pipeline/errors"),
      api.get("/api/pipeline/watermarks"),
    ]).then(([e, err, w]) => {
      setExecutions(e.data.executions || []);
      setErrors(err.data.errors || []);
      setWatermarks(w.data.watermarks || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) return <p className="loading">Loading...</p>;

  return (
    <div>
      <h1>Pipeline Monitor</h1>

      <div className="tabs">
        {TABS.map((t) => (
          <button
            key={t}
            className={`tab-btn ${tab === t ? "active" : ""}`}
            onClick={() => setTab(t)}
          >
            {t}
            {t === "Errors" && errors.length > 0 && (
              <span className="tab-badge">{errors.length}</span>
            )}
          </button>
        ))}
      </div>

      {tab === "Executions" && (
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>Pipeline</th>
                <th>Layer</th>
                <th>Status</th>
                <th>Rows Read</th>
                <th>Rows Written</th>
                <th>Duration</th>
                <th>Started</th>
              </tr>
            </thead>
            <tbody>
              {executions.map((e) => (
                <tr key={e.log_id}>
                  <td>{e.pipeline_name}</td>
                  <td>{e.layer || "-"}</td>
                  <td><StatusBadge status={e.status} /></td>
                  <td>{e.rows_read?.toLocaleString()}</td>
                  <td>{e.rows_written?.toLocaleString()}</td>
                  <td>{e.duration_secs}s</td>
                  <td>{new Date(e.started_at).toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {tab === "Errors" && (
        <div className="table-container">
          {errors.length === 0 ? (
            <p style={{ padding: 16, color: "#6b7280" }}>No errors recorded</p>
          ) : (
            <table>
              <thead>
                <tr>
                  <th>Pipeline</th>
                  <th>Type</th>
                  <th>Severity</th>
                  <th>Message</th>
                  <th>Occurred</th>
                </tr>
              </thead>
              <tbody>
                {errors.map((e) => (
                  <tr key={e.error_id}>
                    <td>{e.pipeline_name}</td>
                    <td>{e.error_type}</td>
                    <td><StatusBadge status={e.severity} /></td>
                    <td className="truncate">{e.error_message}</td>
                    <td>{new Date(e.occurred_at).toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}

      {tab === "Watermarks" && (
        <div className="table-container">
          <table>
            <thead>
              <tr>
                <th>Table</th>
                <th>Last Watermark</th>
                <th>Rows Extracted</th>
                <th>Status</th>
                <th>Updated</th>
              </tr>
            </thead>
            <tbody>
              {watermarks.map((w) => (
                <tr key={w.table_name}>
                  <td>{w.table_name}</td>
                  <td>{new Date(w.last_watermark).toLocaleString()}</td>
                  <td>{w.rows_extracted?.toLocaleString()}</td>
                  <td><StatusBadge status={w.status} /></td>
                  <td>{w.updated_at ? new Date(w.updated_at).toLocaleString() : "-"}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}
