# 🏭 RetailNova Enterprise Data Platform

> **Production-ready cloud data engineering platform** built with Azure-equivalent technologies.
> Simulates a real consulting delivery for a retail company migrating from on-prem SQL Server to a cloud data lake.

---

## 📋 Executive Summary

RetailNova is a Greek retail chain operating 4 physical stores and an online channel.
Their legacy setup — on-prem SQL Server + manual Excel reporting — could not support
growth, lacked data governance, and had zero automation.

This platform delivers:
- **Automated daily ingestion** from SQL Server via incremental CDC
- **Medallion Architecture** (Bronze → Silver → Gold) in Delta Lake format
- **Star Schema** dimensional model for analytics
- **Data Quality Framework** with 20+ configurable rules
- **Real-time monitoring** dashboard via Grafana
- **Unit test suite** covering all transformation logic

---

## 🎯 Business Objectives

| Objective | Solution |
|-----------|----------|
| Replace manual Excel reporting | Gold layer + Grafana/Power BI |
| Automate daily data refresh | Master pipeline with retry logic |
| Ensure data accuracy | DQ Framework with configurable thresholds |
| Cloud scalability | Delta Lake on object storage (S3/ADLS) |
| Enable future AI/ML | Feature-ready Gold layer (RFM, CLV, cohorts) |
| GDPR compliance | PII masking at Silver layer + audit logs |

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  SOURCE LAYER                                                │
│  SQL Server (RetailNova_OLTP)                               │
│  Tables: customers, products, stores, orders, order_lines   │
└─────────────────────┬───────────────────────────────────────┘
                      │ JDBC Incremental (watermark CDC)
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (Raw / Immutable)                             │
│  Format: Delta | Partition: year/month                      │
│  Mode: APPEND only (never overwrite)                        │
└─────────────────────┬───────────────────────────────────────┘
                      │ Clean + Validate + SCD2
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (Cleaned / Business-validated)                │
│  Format: Delta | Mode: MERGE (upsert)                       │
│  SCD2 customers | PII masked | DQ validated                 │
└─────────────────────┬───────────────────────────────────────┘
                      │ Star Schema + KPIs + Aggregations
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (Business-Ready)                                │
│  dim_customer | dim_product | dim_store | dim_date          │
│  fact_sales | agg_monthly_kpis | cohort_analysis            │
│  customer_segments (RFM)                                    │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
              Power BI / Grafana Dashboards
```

### Local Stack (Docker)
| Component | Azure Equivalent | Port |
|-----------|-----------------|------|
| SQL Server 2022 | Azure SQL Database | 1433 |
| MinIO | Azure Data Lake Gen2 | 9000/9001 |
| Apache Spark 3.5 | Azure Databricks | 7077/8080 |
| Jupyter Lab | Databricks Notebooks | 8888 |
| PostgreSQL | Azure SQL (metadata) | 5432 |
| Grafana | Power BI / Azure Monitor | 3000 |

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop (4GB RAM minimum)
- Python 3.9+
- Git

### 1. Clone & Setup
```bash
git clone https://github.com/your-username/enterprise-retail-data-platform
cd enterprise-retail-data-platform

# Windows
setup_windows.bat

# macOS / Linux
chmod +x setup.sh && ./setup.sh
```

### 2. Run the Full Pipeline
```bash
python run_pipeline.py --layer all
```

### 3. Run Tests
```bash
python run_pipeline.py --tests
# or
python -m pytest tests/test_pipeline.py -v
```

### 4. Open Jupyter (interactive)
```
http://localhost:8888   (token: retailnova2024)

Notebooks:
  01_bronze_ingestion.ipynb  - Bronze CDC demo
  02_silver_scd2.ipynb       - SCD2 + DQ checks
  03_gold_analytics.ipynb    - Star schema + KPIs
```

### 5. Monitor in Grafana
```
http://localhost:3000   (admin / RetailNova@2024)
Dashboard: "RetailNova - Pipeline Operations Dashboard"
```

---

## 📁 Project Structure

```
enterprise-retail-data-platform/
├── docker/
│   └── docker-compose.yml          # All services
├── scripts/
│   ├── sql/01_seed_oltp.sql         # SQL Server source data
│   └── postgres/01_init_metadata.sql# Metadata DB schema + DQ rules
├── pipelines/
│   ├── config.py                    # Central configuration
│   ├── spark_session.py             # SparkSession builder
│   ├── logger.py                    # Structured pipeline logging
│   ├── bronze_ingestion.py          # Layer 1: Raw extraction
│   ├── silver_transformation.py     # Layer 2: Clean + SCD2
│   ├── gold_pipeline.py             # Layer 3: Star schema + KPIs
│   └── master_pipeline.py           # Orchestrator with retry/alerts
├── quality_framework/
│   └── dq_engine.py                 # Generic DQ rule engine
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_scd2.ipynb
│   └── 03_gold_analytics.ipynb
├── monitoring/
│   └── grafana/
│       ├── datasources/             # Auto-configured PostgreSQL
│       └── dashboards/              # Pre-built operations dashboard
├── tests/
│   └── test_pipeline.py             # 25+ unit tests
├── documentation/
│   └── architecture.md              # Design decisions
├── run_pipeline.py                  # CLI entry point
├── requirements.txt
├── setup.sh                         # macOS/Linux setup
└── setup_windows.bat                # Windows setup
```

---

## 🔄 Data Flow & Patterns

### Incremental Load (CDC)
```python
# Watermark-based: only extract rows changed since last run
WHERE last_modified > '2024-11-30 00:00:00'  # from watermarks table

# After success: update watermark to NOW()
UPDATE pipeline_watermarks SET last_watermark = NOW() WHERE table_name = 'customers'
```

### SCD Type 2 (Customers)
```
Customer 1 changes city: Athens → Piraeus

Before:
  customer_id=1, city=Athens, is_current=TRUE

After MERGE:
  customer_id=1, city=Athens,  effective_end=2024-06-15, is_current=FALSE
  customer_id=1, city=Piraeus, effective_end=NULL,       is_current=TRUE
```

### Data Quality Rules (from DB)
```sql
-- Rules stored in data_quality_rules table
-- Engine reads rules dynamically and executes them
SELECT * FROM data_quality_rules WHERE is_active = TRUE;

-- Results written to data_quality_log
SELECT * FROM data_quality_log ORDER BY run_at DESC LIMIT 20;
```

---

## 📊 KPIs Computed

| KPI | Description |
|-----|-------------|
| **Total Revenue** | Sum of line amounts for Delivered/Shipped orders |
| **Average Order Value (AOV)** | Revenue / Order Count |
| **Basket Size** | Avg items per order |
| **Unique Customers** | Monthly distinct buyers |
| **Customer Lifetime Value (CLV)** | Avg total spend per customer |
| **Repeat Purchase Rate** | % customers with 2+ orders |
| **RFM Segments** | Champions, Loyal, At-Risk, Lost, New |
| **Cohort Retention** | Month-0 through Month-N retention by acquisition cohort |

---

## 🛡 Data Governance

### PII Handling
- **Silver layer**: email and phone SHA-256 hashed
- **Gold layer**: no direct PII (aggregate/segment only)
- **Audit trail**: every pipeline run logged with run_id

### Access Control (Azure RBAC simulation)
```
Bronze  → data_engineers group only
Silver  → data_engineers (write), data_analysts (read)
Gold    → data_engineers (write), bi_users (read)
```

### GDPR Right to Erasure
```sql
-- Soft-delete customer (no hard deletes in Delta)
UPDATE dbo.customers SET is_active = FALSE WHERE customer_id = ?

-- Next pipeline run: SCD2 expires the record
-- Masked data in Silver means no recoverable PII
```

---

## ⚙️ Pipeline CLI Reference

```bash
# Full end-to-end pipeline
python run_pipeline.py --layer all

# Individual layers
python run_pipeline.py --layer bronze
python run_pipeline.py --layer silver
python run_pipeline.py --layer gold
python run_pipeline.py --layer quality

# Specific tables only (bronze)
python run_pipeline.py --layer bronze --tables customers products

# Stop on first failure
python run_pipeline.py --layer all --fail-fast

# Skip DQ checks (faster dev iteration)
python run_pipeline.py --layer all --skip-quality

# Check service connectivity
python run_pipeline.py --status

# Run test suite
python run_pipeline.py --tests
```

---

## 🧪 Test Coverage

```
tests/test_pipeline.py - 25 unit tests

TestSilverCustomerTransformation  (7 tests)
  ✓ email lowercase and trim
  ✓ first name initcap
  ✓ country uppercase
  ✓ age calculation
  ✓ email regex valid
  ✓ email regex invalid
  ✓ deduplication keeps latest record

TestSilverProductTransformation   (3 tests)
  ✓ product code uppercase
  ✓ product name trimmed
  ✓ zero price filtered

TestSilverOrderTransformation     (3 tests)
  ✓ negative shipping cost zeroed
  ✓ discount > 100% zeroed
  ✓ suspicious order flag

TestDataQualityRules              (6 tests)
  ✓ not_null PASS / FAIL
  ✓ unique PASS / FAIL
  ✓ range PASS / FAIL
  ✓ completeness threshold

TestSchemaValidation              (3 tests)
  ✓ required columns present
  ✓ missing column detected
  ✓ extra columns allowed

TestRowCountReconciliation        (2 tests)
  ✓ bronze-to-silver count
  ✓ deduplication reduces count

TestNegativeScenarios             (4 tests)
  ✓ null email caught by DQ
  ✓ negative quantity filtered
  ✓ corrupt file schema mismatch
  ✓ order total mismatch detection
  ✓ SCD2 address change detection
```

---

## 🧠 Interview Q&A Guide

**Q: How did you implement SCD Type 2?**
> Delta MERGE with change detection condition. When tracked columns (address, email, loyalty_tier) differ between source and target, the old row gets `effective_end_date = NOW()` and `is_current = FALSE`. A new row is inserted by `whenNotMatchedInsertAll()`. Non-tracked column updates (name, phone) use `whenMatchedUpdate()` without expiring the row.

**Q: How does your incremental loading work?**
> Watermark table in PostgreSQL stores the `last_modified` timestamp per source table. Each run reads `WHERE last_modified > watermark`, processes the data, then updates the watermark to `NOW()`. This is a high-water mark pattern. For production we'd upgrade to Debezium CDC → Event Hubs → Auto Loader.

**Q: How did you design the data quality framework?**
> Rules are stored as rows in `data_quality_rules` table (rule_type, threshold, severity). The DQ engine loads all active rules, dispatches each to a type-specific executor function, captures pass/fail rates, and writes results to `data_quality_log`. Critical failures trigger alerts. This makes it metadata-driven — adding a new rule needs only a DB INSERT, not a code change.

**Q: How do you handle pipeline failures?**
> Master pipeline uses a retry wrapper with exponential backoff (delay * 2^attempt). Each child pipeline runs in its own `pipeline_run()` context manager which logs start/end/duration/rows. If Bronze fails, Silver is automatically skipped (conditional branching). Alerts are sent on failure and SLA breach. All errors are logged to `error_log` with stack traces.

**Q: How would you scale this to 100x data?**
> Switch from JDBC polling to streaming CDC (Debezium → Event Hubs → Auto Loader). Increase `spark.sql.shuffle.partitions` from 4 to 400+. Add Z-ORDER indexing on `fact_sales(order_date, customer_id)`. Use Delta Liquid Clustering for auto-layout. Separate Gold and Silver job clusters with different SLAs. Consider Databricks Photon engine for Gold aggregations.

---

## 📈 Scalability Roadmap

| Phase | Feature | Value |
|-------|---------|-------|
| Current | Batch, daily | ✓ Done |
| Phase 2 | Streaming (5-min micro-batch) | Near-real-time analytics |
| Phase 3 | Feature Store + Azure ML | Churn prediction, price optimisation |
| Phase 4 | Data Mesh | Decentralised domain ownership |

---

## 🎓 Lessons Learned

1. **Delta MERGE is powerful but has edge cases** — always test SCD2 merge conditions with duplicate keys before production
2. **Watermark updates must be atomic** — if the pipeline fails after write but before watermark update, you'll re-process data → ensure idempotent writes (Delta MERGE handles this)
3. **Shuffle partitions for local dev** — the default 200 partitions is fine for prod but crushes local performance; set to 2-4 for dev
4. **DQ rules in DB, not code** — code changes require deployment; DB changes can be hotfixed without deployment
5. **Test negative scenarios explicitly** — corrupt data injection is the only way to verify your DQ rules actually fire

---

*Built as a consulting case study for RetailNova Analytics.
Demonstrates production-level Azure data engineering patterns.*
