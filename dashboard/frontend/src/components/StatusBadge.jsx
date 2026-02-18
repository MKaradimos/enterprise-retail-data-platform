const colors = {
  SUCCESS: { bg: "#dcfce7", text: "#166534" },
  PASS: { bg: "#dcfce7", text: "#166534" },
  RUNNING: { bg: "#dbeafe", text: "#1e40af" },
  FAILED: { bg: "#fee2e2", text: "#991b1b" },
  FAIL: { bg: "#fee2e2", text: "#991b1b" },
  WARNING: { bg: "#fef9c3", text: "#854d0e" },
  SKIPPED: { bg: "#f3f4f6", text: "#6b7280" },
  ERROR: { bg: "#fee2e2", text: "#991b1b" },
  CRITICAL: { bg: "#fee2e2", text: "#991b1b" },
  INFO: { bg: "#dbeafe", text: "#1e40af" },
};

export default function StatusBadge({ status }) {
  const s = status?.toUpperCase() || "UNKNOWN";
  const c = colors[s] || { bg: "#f3f4f6", text: "#6b7280" };
  return (
    <span
      className="status-badge"
      style={{ backgroundColor: c.bg, color: c.text }}
    >
      {status}
    </span>
  );
}
