import { NavLink } from "react-router-dom";
import {
  LayoutDashboard,
  TrendingUp,
  Users,
  ShieldCheck,
  Activity,
} from "lucide-react";

const links = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/kpis", label: "KPIs", icon: TrendingUp },
  { to: "/customers", label: "Customers", icon: Users },
  { to: "/quality", label: "Data Quality", icon: ShieldCheck },
  { to: "/pipeline", label: "Pipeline", icon: Activity },
];

export default function Sidebar() {
  return (
    <aside className="sidebar">
      <div className="sidebar-brand">
        <span className="brand-icon">R</span>
        <span className="brand-text">RetailNova</span>
      </div>
      <nav>
        {links.map((l) => (
          <NavLink
            key={l.to}
            to={l.to}
            className={({ isActive }) =>
              "nav-link" + (isActive ? " active" : "")
            }
          >
            <l.icon size={18} />
            <span>{l.label}</span>
          </NavLink>
        ))}
      </nav>
    </aside>
  );
}
