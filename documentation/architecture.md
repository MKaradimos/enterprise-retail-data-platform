# RetailNova Enterprise Data Platform
# Architecture & Design Decisions

## 1. Medallion Architecture

```
SQL Server (On-Prem)
       │
       ▼ [Incremental CDC / JDBC]
   ┌────────────────────────────────────────────────────────────────┐
   │  BRONZE  (Raw / Immutable)                                     │
   │  - Exact copy of source                                         │
   │  - Delta format, append-only                                    │
   │  - Partitioned: /bronze/table/year=YYYY/month=MM/              │
   │  - Audit cols: _extracted_at, _run_id, _source_table           │
   └────────────────────────────────────────────────────────────────┘
       │
       ▼ [Clean, Validate, Deduplicate, SCD2]
   ┌────────────────────────────────────────────────────────────────┐
   │  SILVER  (Cleaned / Standardised)                              │
   │  - Business-validated data                                      │
   │  - SCD Type 2 for customers                                     │
   │  - PII masked (SHA-256)                                         │
   │  - Delta MERGE (upsert by PK)                                   │
   └────────────────────────────────────────────────────────────────┘
       │
       ▼ [Star Schema, KPIs, Aggregations]
   ┌────────────────────────────────────────────────────────────────┐
   │  GOLD  (Business-Ready)                                         │
   │  - Star schema: dim_* + fact_*                                  │
   │  - Pre-aggregated KPIs                                          │
   │  - RFM segments, cohort analysis                                │
   │  - Optimised for BI queries                                     │
   └────────────────────────────────────────────────────────────────┘
       │
       ▼
   Power BI / Grafana Dashboards
```

## 2. Storage Strategy

### Folder Hierarchy (MinIO / Azure Data Lake Gen2)
```
retailnova-{env}/
├── bronze/
│   ├── customers/year=2024/month=01/part-0001.parquet
│   ├── products/
│   ├── stores/
│   ├── sales_orders/year=2024/month=01/
│   └── sales_order_lines/year=2024/month=01/
├── silver/
│   ├── customers/          (SCD2 — no partition, full table)
│   ├── products/
│   ├── stores/
│   ├── sales_orders/
│   └── order_lines/
└── gold/
    ├── dim_date/
    ├── dim_customer/
    ├── dim_product/
    ├── dim_store/
    ├── fact_sales/
    ├── agg_monthly_kpis/
    ├── cohort_analysis/
    └── customer_segments/
```

### Delta Format Justification
- ACID transactions → no partial writes
- Time travel → 7-day history for debugging
- Schema evolution → additive column changes auto-merged
- Auto-optimise → small file compaction

## 3. Cost Optimisation Strategy

### Storage Costs
| Action | Saving |
|--------|--------|
| Delta auto-compaction | Reduces small files → fewer S3 LIST calls |
| Log retention = 7 days | Avoids accumulating delta logs indefinitely |
| Bronze partitioning by year/month | Downstream reads scan only required partitions |
| Stores not partitioned | < 10 rows — partitioning would add overhead |

### Compute Costs
| Action | Saving |
|--------|--------|
| Incremental loads (watermark) | Never re-process existing data |
| Broadcast join for dim tables | Avoids shuffle for joins with small tables |
| Cluster auto-termination (Azure) | Terminate after 30min inactivity |
| Local dev: `shuffle.partitions=4` | Avoids 200 empty tasks in dev |
| AQE (Adaptive Query Execution) | Auto-coalesces partitions at runtime |

### When to Terminate Clusters (Azure Databricks)
- Dev clusters: terminate after **30 minutes** of inactivity
- Job clusters: **terminate immediately** after job completion
- Use **spot/preemptible** VMs for bronze/silver (fault-tolerant with checkpoints)
- Gold pipeline uses **standard VMs** (shorter, needs reliability)

## 4. Incremental Load Design

```
Watermark Table (PostgreSQL)
┌───────────────────┬────────────────────┬──────────────────────┐
│ table_name        │ last_watermark      │ status               │
├───────────────────┼────────────────────┼──────────────────────┤
│ src_customers     │ 2024-11-30 00:00:00│ SUCCESS              │
│ src_sales_orders  │ 2024-11-30 00:00:00│ SUCCESS              │
└───────────────────┴────────────────────┴──────────────────────┘
         │
         ▼
Extract: WHERE last_modified > '2024-11-30 00:00:00'
         │
         ▼
After success: UPDATE watermark to NOW()
```

**CDC Simulation**: Without Debezium/SQL Server CDC enabled,
we use `last_modified` column as high-water mark.
In Azure: replace with ADF Change Tracking or Debezium → Event Hubs.

## 5. SCD Type 2 Implementation

### Tracked Columns
- `address_line1`, `city`, `country`, `email`, `loyalty_tier`

### Example
```
┌──────────────┬──────────────┬──────────────────────┬────────────────┬────────────┐
│ customer_id  │ city         │ effective_start_date  │ effective_end  │ is_current │
├──────────────┼──────────────┼──────────────────────┼────────────────┼────────────┤
│ 1            │ Athens       │ 2022-01-10           │ 2024-06-15     │ FALSE      │
│ 1            │ Piraeus      │ 2024-06-15           │ NULL           │ TRUE       │
└──────────────┴──────────────┴──────────────────────┴────────────────┴────────────┘
```

## 6. Naming Conventions

### Storage Paths
- snake_case for all table names
- Environment prefix in bucket name: `retailnova-dev`, `retailnova-prod`
- No environment prefix inside bucket (bucket IS the namespace)

### Python Code
- snake_case functions and variables
- UPPER_CASE constants
- PascalCase classes

### Pipeline Run IDs
Format: `{pipeline_name}_{YYYYMMDD}_{HHMMSS}_{uuid8}`
Example: `bronze_customers_20241201_143022_a3b4c5d6`

## 7. Performance Optimisation

### Partition Pruning
```python
# BAD: Full scan
df.filter(F.year(F.col("order_date")) == 2024)

# GOOD: Partition column
df.filter(F.col("order_year") == 2024)
```

### Broadcast Joins
```python
# dim tables < 10MB → auto-broadcast
# spark.sql.autoBroadcastJoinThreshold = 10MB
fact.join(F.broadcast(dim_product), "product_id")
```

### Caching Strategy
```python
# Cache ONLY when DataFrame is reused multiple times in same job
df.cache()          # persist to memory
df.persist(StorageLevel.DISK_ONLY)  # if memory tight
df.unpersist()      # always unpersist after use
```

### Cluster Sizing (Azure Databricks)
| Job | Driver | Workers | Worker Type |
|-----|--------|---------|-------------|
| Bronze | 2 cores | 2-4 | Standard_DS3_v2 |
| Silver | 4 cores | 2-4 | Standard_DS4_v2 |
| Gold   | 4 cores | 4-8 | Standard_DS4_v2 (spot) |

## 8. Security & Governance

### Role-Based Access (RBAC Simulation)
```
Bronze container → Data Engineers only
Silver container → Data Engineers + Data Analysts (read)
Gold container   → All BI users (read), Data Engineers (write)
```

### PII Masking
- Applied at Silver layer (SHA-256 on email, phone)
- Only `PII_ACCESS` role can see originals
- In Azure: Column-Level Security in Synapse

### GDPR Compliance
- Right to erasure: implemented via `is_active = FALSE`
- Data masking at Silver layer
- Audit log kept for 90 days
- No PII in Gold layer

## 9. Scalability Design

### If Data Grows 100x
| Change | Reason |
|--------|--------|
| Switch from JDBC to Event Hubs + Auto Loader | JDBC doesn't scale for high-volume CDC |
| Increase shuffle partitions: 4 → 400 | Avoid task skew with large data |
| Add Z-ORDER on fact_sales(order_date, customer_id) | Faster range scans |
| Use Delta Liquid Clustering | Auto-optimises layout at write time |
| Separate Gold from Silver cluster | Different latency SLAs |

### Streaming Support
```
SQL Server → Debezium → Azure Event Hubs → Auto Loader → Bronze (streaming)
                                                              │
                                                         Structured Streaming
                                                              │
                                                    Silver (micro-batch, 5min)
```

### ML Use Cases
```
Gold layer → Feature Store → Azure ML → Model training
                           → MLflow → Model registry
                           → Real-time scoring API
```

## 10. CI/CD Git Strategy

### Branch Strategy (Trunk-based)
```
main (prod)
  └── develop (dev/test)
        └── feature/bronze-incremental
        └── feature/scd2-customers
        └── bugfix/quality-threshold
```

### Promotion Path
```
feature/* → develop → main

develop = dev environment
main    = prod environment
```

### Versioning
- Each notebook has a version header: `# v1.2.3 | YYYY-MM-DD`
- Breaking schema changes → major version bump
- config.py has ENV-based defaults
