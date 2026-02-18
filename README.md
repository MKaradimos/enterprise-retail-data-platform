<p align="center">
  <h1 align="center">RetailNova Enterprise Data Platform</h1>
  <p align="center">
    <strong>Production-grade data engineering platform</strong> built with Azure-equivalent technologies.<br/>
    End-to-end Medallion Architecture — from raw OLTP extraction to business-ready analytics and live dashboards.
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/PySpark-3.5.0-orange?logo=apachespark" alt="PySpark"/>
    <img src="https://img.shields.io/badge/Delta_Lake-3.1.0-00ADD8?logo=delta" alt="Delta Lake"/>
    <img src="https://img.shields.io/badge/FastAPI-0.109+-009688?logo=fastapi" alt="FastAPI"/>
    <img src="https://img.shields.io/badge/React-18-61DAFB?logo=react" alt="React"/>
    <img src="https://img.shields.io/badge/Docker_Compose-8_services-2496ED?logo=docker" alt="Docker"/>
    <img src="https://img.shields.io/badge/Tests-30_passing-brightgreen?logo=pytest" alt="Tests"/>
  </p>
</p>

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
- [Running the Pipeline](#running-the-pipeline)
- [Analytics Dashboard](#analytics-dashboard)
- [Data Flow & Patterns](#data-flow--patterns)
- [Data Quality Framework](#data-quality-framework)
- [Gold Layer — KPIs & Analytics](#gold-layer--kpis--analytics)
- [Test Suite](#test-suite)
- [Monitoring](#monitoring)
- [Security & Governance](#security--governance)
- [Scalability Roadmap](#scalability-roadmap)
- [CLI Reference](#cli-reference)

---

## Executive Summary

**RetailNova** is a Greek retail chain operating 4 physical stores and an online channel. Their legacy setup — on-prem SQL Server with manual Excel reporting — could not support growth, lacked data governance, and had zero automation.

This platform delivers:

| Business Objective | Solution |
|---|---|
| Replace manual Excel reporting | Gold layer star schema + React analytics dashboard |
| Automate daily data refresh | Master pipeline with retry logic & exponential backoff |
| Ensure data accuracy | Data Quality Framework with 18 configurable rules |
| Enable cloud scalability | Delta Lake on S3-compatible storage (Azure Data Lake Gen2 equivalent) |
| Unlock AI/ML capabilities | Feature-ready Gold layer (RFM segmentation, CLV, cohort retention) |
| GDPR compliance | PII masking (SHA-256) at Silver layer + full audit trail |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│  SOURCE LAYER                                                            │
│  SQL Server 2022 (RetailNova_OLTP)                                      │
│  Tables: customers · products · stores · sales_orders · order_lines     │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │  JDBC Incremental Extraction
                            │  (Watermark-based CDC)
                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Raw / Immutable)                                         │
│  Format: Delta Lake │ Partition: year/month │ Mode: APPEND only          │
│  Audit columns: _extracted_at, _run_id, _source_table                   │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │  Clean · Validate · Deduplicate · SCD2
                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Cleaned / Business-Validated)                            │
│  Format: Delta Lake │ Mode: MERGE (upsert)                               │
│  SCD Type 2 (customers) │ PII masked (SHA-256) │ DQ validated           │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │  Star Schema · KPIs · Aggregations
                            ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (Business-Ready)                                            │
│  Dimensions: dim_customer · dim_product · dim_store · dim_date          │
│  Facts: fact_sales                                                       │
│  Analytics: agg_monthly_kpis · cohort_analysis · customer_segments      │
└───────────────────────────┬──────────────────────────────────────────────┘
                            │
              ┌─────────────┴─────────────┐
              ▼                           ▼
     React Dashboard              Grafana Monitoring
     (FastAPI backend)            (Pipeline Operations)
```

### Infrastructure (Docker Compose)

| Service | Azure Equivalent | Local Port | Purpose |
|---|---|---|---|
| SQL Server 2022 | Azure SQL Database | `1433` | OLTP source system |
| MinIO | Azure Data Lake Gen2 | `9000` / `9001` | Delta Lake storage |
| Apache Spark 3.5 | Azure Databricks | `7077` / `8081` | Processing engine |
| Spark Worker | Databricks Worker | — | Distributed compute |
| PostgreSQL 15 | Azure SQL (metadata) | `5433` | Pipeline metadata store |
| Jupyter Lab | Databricks Notebooks | `8888` | Interactive exploration |
| Grafana | Power BI / Azure Monitor | `3001` | Operations dashboards |

---

## Tech Stack

| Layer | Technologies |
|---|---|
| **Processing** | PySpark 3.5.0, Delta Lake 3.1.0 |
| **Storage** | MinIO (S3-compatible), Delta format, Parquet |
| **Source DB** | SQL Server 2022 (JDBC) |
| **Metadata DB** | PostgreSQL 15 |
| **Dashboard** | React 18, Vite, Recharts, Axios |
| **API** | FastAPI, Uvicorn, deltalake (delta-rs) |
| **Monitoring** | Grafana (pre-configured dashboards) |
| **Testing** | pytest (30 unit tests), local Spark mode |
| **Infrastructure** | Docker Compose (8 services) |
| **Languages** | Python, SQL, JavaScript (JSX) |

---

## Project Structure

```
enterprise-retail-data-platform/
│
├── pipelines/                          # Core ETL pipeline code
│   ├── config.py                       #   Central configuration (env-aware)
│   ├── spark_session.py                #   SparkSession builder (Delta + S3A)
│   ├── logger.py                       #   Structured logging to PostgreSQL
│   ├── bronze_ingestion.py             #   Layer 1: Raw JDBC extraction
│   ├── silver_transformation.py        #   Layer 2: Clean + SCD2 + masking
│   ├── gold_pipeline.py                #   Layer 3: Star schema + KPIs
│   └── master_pipeline.py              #   Orchestrator (retry, branching, SLA)
│
├── quality_framework/
│   └── dq_engine.py                    # Metadata-driven DQ rule engine
│
├── dashboard/
│   ├── backend/                        # FastAPI API server
│   │   ├── main.py                     #   App + CORS + router mounting
│   │   ├── config.py                   #   Reads project .env
│   │   ├── db.py                       #   PostgreSQL query helper
│   │   ├── delta_reader.py             #   Gold Delta table reader (delta-rs)
│   │   └── routers/
│   │       ├── overview.py             #   GET /api/overview
│   │       ├── kpis.py                 #   GET /api/kpis/monthly
│   │       ├── customers.py            #   GET /api/customers/segments, /cohorts
│   │       ├── quality.py              #   GET /api/quality/rules, /log, /summary
│   │       └── pipeline.py             #   GET /api/pipeline/executions, /errors, /watermarks
│   └── frontend/                       # React + Vite SPA
│       └── src/
│           ├── pages/
│           │   ├── Overview.jsx        #   KPI cards + latest runs
│           │   ├── KPIs.jsx            #   Revenue, AOV, basket charts
│           │   ├── Customers.jsx       #   RFM pie chart + cohort heatmap
│           │   ├── DataQuality.jsx     #   DQ rules + pass rates
│           │   └── PipelineMonitor.jsx #   Executions / errors / watermarks
│           └── components/
│               ├── Layout.jsx          #   Sidebar + content shell
│               ├── Sidebar.jsx         #   Navigation with icons
│               ├── MetricCard.jsx      #   KPI card component
│               └── StatusBadge.jsx     #   Colored status badges
│
├── tests/
│   ├── conftest.py                     # Auto-mock PG, session-scoped Spark
│   └── test_pipeline.py               # 30 unit tests (7 test classes)
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb       # Interactive Bronze demo
│   ├── 02_silver_scd2.ipynb            # SCD2 + DQ walkthrough
│   └── 03_gold_analytics.ipynb         # Star schema + KPIs
│
├── scripts/
│   ├── sql/01_seed_oltp.sql            # SQL Server seed data
│   └── postgres/01_init_metadata.sql   # Metadata schema + DQ rules
│
├── docker/docker-compose.yml           # 8-service Docker stack
├── monitoring/grafana/                 # Pre-configured Grafana dashboards
├── documentation/architecture.md       # Design decisions document
│
├── run_pipeline.py                     # CLI entry point
├── Makefile                            # Shortcut commands (Linux/macOS)
├── setup.sh                            # First-time setup script
├── requirements.txt                    # Python dependencies
└── .env.example                        # Environment template
```

---

## Getting Started

### Prerequisites

| Requirement | Minimum | Notes |
|---|---|---|
| Docker Desktop | 4 GB RAM | Runs all 8 services |
| Python | 3.8+ | PySpark, FastAPI |
| Java | 11+ (OpenJDK) | Required by Spark |
| Node.js | 16+ | React dashboard |
| Git | Any | Version control |

### 1. Clone the Repository

```bash
git clone https://github.com/MKaradimos/enterprise-retail-data-platform.git
cd enterprise-retail-data-platform
```

### 2. First-Time Setup

```bash
# Linux / macOS
make setup

# Or manually:
cp .env.example .env
pip install -r requirements.txt
docker compose -f docker/docker-compose.yml up -d
# Wait for SQL Server to become healthy (~40s), then seed:
make seed
```

### 3. Verify Services

```bash
make status
```

Expected output:
```
  ✓ localhost:1433  reachable    (SQL Server)
  ✓ localhost:9000  reachable    (MinIO)
  ✓ localhost:5433  reachable    (PostgreSQL)
  ✓ localhost:7077  reachable    (Spark Master)
  ✓ localhost:8888  reachable    (Jupyter)
  ✓ localhost:3001  reachable    (Grafana)
```

---

## Running the Pipeline

### Full Pipeline (Bronze → Silver → Gold → Quality)

```bash
make run
# or: python run_pipeline.py --layer all
```

### Individual Layers

```bash
make run-bronze       # Extract from SQL Server → Bronze Delta tables
make run-silver       # Clean, SCD2, mask → Silver Delta tables
make run-gold         # Star schema, KPIs, RFM, cohorts → Gold Delta tables
make run-quality      # Run 18 data quality checks
```

### Pipeline Output Example

```
  ══════════════════════════════════════════════════════
    BRONZE LAYER  |  2026-02-17 12:45:33
  ══════════════════════════════════════════════════════
    [customers]          10 rows extracted ✓
    [products]           20 rows extracted ✓
    [stores]              5 rows extracted ✓
    [sales_orders]       20 rows extracted ✓
    [sales_order_lines]  33 rows extracted ✓

  ══════════════════════════════════════════════════════
    GOLD LAYER BUILD  |  2026-02-17 12:46:01
  ══════════════════════════════════════════════════════
    [dim_date]     4,018 rows written ✓
    [dim_customer]    10 rows written ✓
    [dim_product]     20 rows written ✓
    [dim_store]        5 rows written ✓
    [fact_sales]      30 rows written ✓
    [monthly_kpis]     4 rows written ✓
    [cohort]           7 rows written ✓
    [segments]        10 rows written ✓

  DATA QUALITY: 17/18 PASS, 1 WARNING (orders_freshness)
```

---

## Analytics Dashboard

The React + FastAPI dashboard visualises all pipeline results in real-time.

### Start the Dashboard

```bash
# Terminal 1 — Backend (port 8000)
cd dashboard/backend
pip install -r requirements.txt
uvicorn main:app --port 8000 --reload

# Terminal 2 — Frontend (port 5173)
cd dashboard/frontend
npm install
npm run dev
```

Open **http://localhost:5173** in your browser.

### Dashboard Pages

| Page | Description | Data Source |
|---|---|---|
| **Overview** | 4 KPI cards (revenue, orders, customers, DQ pass rate) + latest pipeline runs | PostgreSQL + Gold Delta |
| **KPIs** | Monthly revenue bar chart, orders/customers line chart, AOV trend | Gold `agg_monthly_kpis` |
| **Customers** | RFM segment pie chart with stats table + cohort retention heatmap | Gold `customer_segments` + `cohort_analysis` |
| **Data Quality** | Overall pass rate, rules table, latest check results with severity badges | PostgreSQL `data_quality_log` |
| **Pipeline Monitor** | 3 tabs: Execution History, Errors, Watermarks | PostgreSQL metadata tables |

### API Endpoints

| Endpoint | Method | Description |
|---|---|---|
| `/api/health` | GET | Health check |
| `/api/overview` | GET | Headline KPIs + latest runs + DQ summary |
| `/api/kpis/monthly` | GET | Monthly KPI array (revenue, AOV, basket, CLV) |
| `/api/customers/segments` | GET | RFM segment distribution + per-customer scores |
| `/api/customers/cohorts` | GET | Cohort retention matrix |
| `/api/quality/rules` | GET | Active DQ rule definitions |
| `/api/quality/log` | GET | Latest 100 DQ check results |
| `/api/quality/summary` | GET | PASS/FAIL/WARNING counts |
| `/api/pipeline/executions` | GET | Paginated execution history |
| `/api/pipeline/errors` | GET | Latest 50 pipeline errors |
| `/api/pipeline/watermarks` | GET | CDC watermark statuses |

---

## Data Flow & Patterns

### Incremental Load (Watermark CDC)

```
                    PostgreSQL (Watermarks)
                    ┌──────────────────────────────────────┐
                    │ table_name       │ last_watermark     │
                    │ src_customers    │ 2026-02-17 12:45   │
                    │ src_sales_orders │ 2026-02-17 12:45   │
                    └──────────────────────────────────────┘
                              │
                              ▼
Extract:  SELECT * FROM customers WHERE last_modified > '2026-02-17 12:45'
                              │
                              ▼
Success:  UPDATE pipeline_watermarks SET last_watermark = NOW()
```

Only new/changed records are extracted on each run — no full table scans.

### SCD Type 2 (Customers)

When a tracked column changes (address, email, loyalty_tier), the Silver layer preserves history:

```
Before: customer_id=1, city=Athens,  is_current=TRUE

After MERGE:
  customer_id=1, city=Athens,  effective_end=2026-02-17, is_current=FALSE  ← expired
  customer_id=1, city=Piraeus, effective_end=NULL,       is_current=TRUE   ← new current
```

### PII Masking (Silver Layer)

```python
# Email: user@example.com → SHA-256 hash
# Phone: +30-210-1234567 → SHA-256 hash
# Applied at Silver layer — Gold layer contains no recoverable PII
```

---

## Data Quality Framework

The DQ engine is **metadata-driven** — rules are stored in PostgreSQL, not hardcoded:

```sql
SELECT rule_name, target_table, rule_type, threshold_pct, severity
FROM data_quality_rules WHERE is_active = TRUE;
```

### Supported Rule Types

| Rule Type | Description | Example |
|---|---|---|
| `not_null` | Column must not contain NULLs | `email` in customers |
| `unique` | Column values must be unique | `product_code` in products |
| `range` | Values within min/max bounds | `unit_price` between 0.01 and 99,999 |
| `regex` | Values match a pattern | Email format `^[^@]+@[^@]+\.[^@]+$` |
| `freshness` | Latest record within N days | Orders not older than 2 days |
| `row_count` | Minimum row count threshold | At least 1 customer exists |
| `completeness` | % of non-null values above threshold | 80%+ phones filled |

### Current Rules (18 active)

- **5** Not Null checks (CRITICAL)
- **3** Uniqueness checks (CRITICAL)
- **3** Range checks (WARNING)
- **2** Regex checks (WARNING/INFO)
- **2** Row Count checks (CRITICAL)
- **1** Freshness check (WARNING)
- **2** Completeness checks (INFO/WARNING)

Adding a new rule requires only a database INSERT — no code changes needed.

---

## Gold Layer — KPIs & Analytics

### Star Schema

```
                    ┌──────────┐
                    │ dim_date │
                    └────┬─────┘
                         │
┌──────────────┐    ┌────┴─────┐    ┌─────────────┐
│ dim_customer ├────┤fact_sales├────┤ dim_product  │
└──────────────┘    └────┬─────┘    └─────────────┘
                         │
                    ┌────┴─────┐
                    │dim_store │
                    └──────────┘
```

### Computed KPIs

| KPI | Description | Table |
|---|---|---|
| Total Revenue | Sum of line amounts (Delivered/Shipped orders) | `agg_monthly_kpis` |
| Average Order Value | Revenue / Order Count | `agg_monthly_kpis` |
| Basket Size | Avg items per order | `agg_monthly_kpis` |
| Unique Customers | Monthly distinct buyers | `agg_monthly_kpis` |
| Customer Lifetime Value | Avg total spend per customer (lifetime) | `agg_monthly_kpis` |
| Repeat Purchase Rate | % of customers with 2+ orders | `agg_monthly_kpis` |
| RFM Segments | Champions, Loyal, Promising, At Risk, Need Attention | `customer_segments` |
| Cohort Retention | Month-over-month retention by acquisition cohort | `cohort_analysis` |

### RFM Segmentation Logic

Each customer is scored 1–4 on three dimensions:

| Dimension | Score 4 | Score 3 | Score 2 | Score 1 |
|---|---|---|---|---|
| **Recency** (days since last order) | ≤ 30 | ≤ 60 | ≤ 90 | > 90 |
| **Frequency** (order count) | ≥ 5 | ≥ 3 | ≥ 2 | 1 |
| **Monetary** (total spend) | ≥ $2,000 | ≥ $1,000 | ≥ $300 | < $300 |

Segment assignment:
- **Champions** — R ≥ 3, F ≥ 3, M ≥ 3
- **Loyal Customers** — R ≥ 3, F ≥ 2
- **Promising** — R ≥ 3, F = 1
- **At Risk** — R = 2, F ≥ 2
- **Need Attention** — R ≤ 2

---

## Test Suite

30 unit tests across 7 test classes, running in **local Spark mode** (no Docker needed):

```bash
make test          # Quick run
make test-verbose  # Full output
make test-coverage # With coverage report
```

### Test Classes

| Class | Tests | What It Validates |
|---|---|---|
| `TestSilverCustomerTransformation` | 7 | Email lowercase/trim, name initcap, country uppercase, age calculation, regex, dedup |
| `TestSilverProductTransformation` | 3 | Product code uppercase, name trim, zero-price filter |
| `TestSilverOrderTransformation` | 3 | Negative shipping → 0, discount > 100% → 0, suspicious order flag |
| `TestDataQualityRules` | 6 | Not null, unique, range rules — PASS and FAIL scenarios |
| `TestSchemaValidation` | 3 | Required columns present, missing detected, extras allowed |
| `TestRowCountReconciliation` | 2 | Bronze-to-Silver count, dedup reduces count |
| `TestNegativeScenarios` | 6 | Null email, negative qty, schema mismatch, SCD2 change detection |

---

## Monitoring

### Grafana (Operations Dashboard)

Pre-configured at **http://localhost:3001** (admin / RetailNova@2024):
- Pipeline execution timeline
- Success/failure rates
- Duration trends
- Data quality pass rates
- Error log

### PostgreSQL Metadata (Quick Queries)

```bash
make watermarks       # CDC watermark statuses
make quality-report   # Latest DQ results
make exec-log         # Pipeline execution history
make errors           # Recent errors
```

---

## Security & Governance

### PII Handling

| Layer | PII Treatment |
|---|---|
| Bronze | Raw (as extracted from source) |
| Silver | SHA-256 hashed (email, phone) |
| Gold | No direct PII (aggregates / segments only) |

### Access Control (Azure RBAC Simulation)

```
Bronze  → data_engineers (read/write)
Silver  → data_engineers (write), data_analysts (read)
Gold    → data_engineers (write), bi_users (read)
```

### GDPR Right to Erasure

```
1. Source: UPDATE customers SET is_active = FALSE WHERE customer_id = ?
2. Next pipeline run: SCD2 expires the record
3. Silver: PII already masked → no recoverable personal data
4. Audit trail retained for 90 days
```

---

## Scalability Roadmap

| Phase | Feature | Impact |
|---|---|---|
| **Current** | Daily batch pipeline | Production baseline |
| **Phase 2** | Streaming micro-batch (5 min) | Near-real-time analytics |
| **Phase 3** | Feature Store + Azure ML | Churn prediction, price optimisation |
| **Phase 4** | Data Mesh | Decentralised domain ownership |

### Scaling to 100x Volume

| Change | Rationale |
|---|---|
| JDBC → Debezium CDC + Event Hubs + Auto Loader | JDBC polling doesn't scale for high-volume CDC |
| `shuffle.partitions`: 4 → 400+ | Avoid task skew with large datasets |
| Z-ORDER on `fact_sales(order_date, customer_id)` | Faster range scans |
| Delta Liquid Clustering | Auto-optimise data layout at write time |
| Separate job clusters per layer | Different latency SLAs for Silver vs Gold |
| Databricks Photon engine | 2-3x faster for Gold aggregations |

---

## CLI Reference

```bash
# ─── Pipeline ────────────────────────────────
make run              # Full pipeline: Bronze → Silver → Gold → Quality
make run-bronze       # Bronze layer only
make run-silver       # Silver layer only
make run-gold         # Gold layer only
make run-quality      # Data quality checks only
make run-fast         # Full pipeline, skip quality checks

# ─── Testing ─────────────────────────────────
make test             # Unit tests (local Spark, no Docker)
make test-verbose     # Tests with full output
make test-coverage    # Tests with coverage report

# ─── Infrastructure ──────────────────────────
make setup            # First-time setup (Docker + DB seed)
make status           # Container health + port check
make seed             # Re-seed SQL Server data
make restart          # Restart all containers
make down             # Stop containers
make clean            # Remove containers + volumes (destroys data)

# ─── Monitoring ──────────────────────────────
make logs             # Tail all container logs
make watermarks       # Show CDC watermark statuses
make quality-report   # Latest DQ check results
make exec-log         # Pipeline execution history
make errors           # Recent pipeline errors

# ─── Browser Shortcuts ───────────────────────
make jupyter          # Open Jupyter Lab
make grafana          # Open Grafana
make spark            # Open Spark UI
make minio            # Open MinIO Console
make psql             # Connect to PostgreSQL
```

---

## Environment Variables

Copy `.env.example` to `.env` and adjust if needed:

| Variable | Default | Description |
|---|---|---|
| `RETAILNOVA_ENV` | `dev` | Environment (dev / test / prod) |
| `SQLSERVER_HOST` | `localhost` | SQL Server hostname |
| `SQLSERVER_PORT` | `1433` | SQL Server port |
| `MINIO_ENDPOINT` | `http://localhost:9000` | MinIO S3 endpoint |
| `MINIO_BUCKET` | `retailnova-dev` | Storage bucket name |
| `POSTGRES_HOST` | `localhost` | Metadata DB hostname |
| `POSTGRES_PORT` | `5433` | Metadata DB port |
| `SPARK_MASTER` | `local[*]` | Spark master URL |

---

## License

This project was built as an enterprise data engineering case study for RetailNova Analytics.

---

<p align="center">
  <sub>Built with PySpark · Delta Lake · FastAPI · React · Docker</sub>
</p>
