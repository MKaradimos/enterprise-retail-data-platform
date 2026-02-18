export default function MetricCard({ title, value, subtitle, color = "#4f46e5" }) {
  return (
    <div className="metric-card">
      <div className="metric-card-bar" style={{ backgroundColor: color }} />
      <div className="metric-card-body">
        <p className="metric-title">{title}</p>
        <p className="metric-value">{value}</p>
        {subtitle && <p className="metric-subtitle">{subtitle}</p>}
      </div>
    </div>
  );
}
