import { useEffect, useState } from "react";
import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis, CartesianGrid,
  Tooltip, Legend, ResponsiveContainer,
} from "recharts";
import api from "../api";
import MetricCard from "../components/MetricCard";

export default function KPIs() {
  const [data, setData] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    api.get("/api/kpis/monthly").then((r) => {
      setData(r.data.monthly_kpis || []);
      setLoading(false);
    }).catch(() => setLoading(false));
  }, []);

  if (loading) return <p className="loading">Loading...</p>;
  if (!data.length) return <p className="error">No KPI data available</p>;

  const latest = data[data.length - 1];

  return (
    <div>
      <h1>Monthly KPIs</h1>

      <div className="cards-grid">
        <MetricCard title="Avg Order Value" value={`$${latest.avg_order_value}`} subtitle={latest.year_month} color="#4f46e5" />
        <MetricCard title="Basket Size" value={latest.avg_basket_size ?? "N/A"} subtitle="items/order" color="#0891b2" />
        <MetricCard title="Repeat Rate" value={`${latest.repeat_purchase_rate}%`} subtitle="all time" color="#059669" />
        <MetricCard title="Avg CLV" value={`$${latest.avg_customer_ltv}`} subtitle="lifetime" color="#d97706" />
      </div>

      <h2>Revenue</h2>
      <div className="chart-container">
        <ResponsiveContainer width="100%" height={300}>
          <BarChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year_month" />
            <YAxis />
            <Tooltip formatter={(v) => `$${v.toLocaleString()}`} />
            <Bar dataKey="total_revenue" fill="#4f46e5" name="Revenue" />
          </BarChart>
        </ResponsiveContainer>
      </div>

      <h2>Orders & Customers</h2>
      <div className="chart-container">
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year_month" />
            <YAxis />
            <Tooltip />
            <Legend />
            <Line type="monotone" dataKey="total_orders" stroke="#4f46e5" name="Orders" />
            <Line type="monotone" dataKey="unique_customers" stroke="#059669" name="Customers" />
          </LineChart>
        </ResponsiveContainer>
      </div>

      <h2>Average Order Value</h2>
      <div className="chart-container">
        <ResponsiveContainer width="100%" height={300}>
          <LineChart data={data}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="year_month" />
            <YAxis />
            <Tooltip formatter={(v) => `$${v.toFixed(2)}`} />
            <Line type="monotone" dataKey="avg_order_value" stroke="#d97706" name="AOV" strokeWidth={2} />
          </LineChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
