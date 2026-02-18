import { BrowserRouter, Routes, Route } from "react-router-dom";
import Layout from "./components/Layout";
import Overview from "./pages/Overview";
import KPIs from "./pages/KPIs";
import Customers from "./pages/Customers";
import DataQuality from "./pages/DataQuality";
import PipelineMonitor from "./pages/PipelineMonitor";

export default function App() {
  return (
    <BrowserRouter>
      <Layout>
        <Routes>
          <Route path="/" element={<Overview />} />
          <Route path="/kpis" element={<KPIs />} />
          <Route path="/customers" element={<Customers />} />
          <Route path="/quality" element={<DataQuality />} />
          <Route path="/pipeline" element={<PipelineMonitor />} />
        </Routes>
      </Layout>
    </BrowserRouter>
  );
}
