# ⚡ Quick Start — Linux

Αυτός ο οδηγός σε βάζει online σε **5 βήματα**.

---

## Βήμα 1: Αποσυμπίεση + εγκατάσταση

```bash
unzip enterprise-retail-data-platform.zip
cd enterprise-retail-data-platform

# Setup (Docker pull + DB seed + Python deps)
chmod +x setup.sh
./setup.sh
```

> ⏱️ Πρώτη φορά: ~5 λεπτά (κατεβαίνουν Docker images)
> Μετά: ~30 δευτερόλεπτα

---

## Βήμα 2: Έλεγξε ότι όλα τρέχουν

```bash
make status
```

Πρέπει να δεις 6 containers `Up` και 6 ports `✓`:

```
NAMES                     STATUS          PORTS
retailnova_sqlserver      Up (healthy)    0.0.0.0:1433->1433/tcp
retailnova_minio          Up (healthy)    0.0.0.0:9000->9000/tcp
retailnova_metadata       Up (healthy)    0.0.0.0:5432->5432/tcp
retailnova_spark          Up (healthy)    0.0.0.0:8080->8080/tcp
retailnova_jupyter        Up              0.0.0.0:8888->8888/tcp
retailnova_grafana        Up              0.0.0.0:3000->3000/tcp
```

---

## Βήμα 3: Τρέξε τα unit tests (χωρίς Docker!)

```bash
make test
```

Τα tests τρέχουν σε **local Spark mode** — δεν χρειάζονται services.

Αναμενόμενο αποτέλεσμα:
```
tests/test_pipeline.py::TestSilverCustomerTransformation::test_email_lowercase_trim  PASSED
tests/test_pipeline.py::TestSilverCustomerTransformation::test_first_name_initcap    PASSED
...
===================== 25 passed in 42.3s =====================
```

> **Πρώτη φορά**: Spark κατεβάζει JARs (~2 λεπτά). Μετά: ~30 δευτερόλεπτα.

---

## Βήμα 4: Τρέξε το πλήρες pipeline

```bash
make run
```

Ή αν θέλεις ένα layer κάθε φορά:

```bash
make run-bronze    # Extracts from SQL Server → MinIO
make run-silver    # Cleans + SCD2 → Silver Delta tables
make run-gold      # Star schema + KPIs + RFM → Gold tables
make run-quality   # Data quality checks → logs results to PostgreSQL
```

---

## Βήμα 5: Εξερεύνησε τα αποτελέσματα

### Jupyter (interactive notebooks)
```
http://localhost:8888   (token: retailnova2024)
```
Άνοιξε τα notebooks με τη σειρά:
1. `notebooks/01_bronze_ingestion.ipynb`
2. `notebooks/02_silver_scd2.ipynb`
3. `notebooks/03_gold_analytics.ipynb`

### Grafana (monitoring)
```
http://localhost:3000   (admin / RetailNova@2024)
```
Dashboard: **RetailNova - Pipeline Operations Dashboard**

### MinIO (data lake)
```
http://localhost:9001   (retailnova_admin / RetailNova@2024)
```
Browser: `retailnova-dev` bucket → `bronze/` `silver/` `gold/`

### Spark UI
```
http://localhost:8080
```

---

## Χρήσιμες εντολές

```bash
# Δες watermarks (CDC state)
make watermarks

# Δες quality report
make quality-report

# Δες execution log
make exec-log

# Δες errors
make errors

# Συνδέσου στο PostgreSQL
make psql

# Tail logs σε real-time
make logs

# Κάνε stop τα πάντα
make down

# Διάβασε όλες τις εντολές
make help
```

---

## Troubleshooting

### SQL Server αργεί να ξεκινήσει
```bash
# Περίμενε 40 δευτερόλεπτα και ξανατρύπα
docker logs retailnova_sqlserver --tail=20

# Αν χρειαστεί, ξανά-σπάρε manually
make seed
```

### "Permission denied" στα scripts
```bash
chmod +x setup.sh
chmod +x run_pipeline.py
```

### Spark "OutOfMemoryError" σε local mode
```bash
# Αύξησε μνήμη στο .env
echo "SPARK_MASTER=local[2]" >> .env
```

### MinIO "Connection refused"
```bash
docker logs retailnova_minio --tail=20
# Βεβαιώσου ότι το port 9000 δεν χρησιμοποιείται
sudo lsof -i :9000
```

### Tests αποτυγχάνουν με "Java not found"
```bash
# Εγκατάστησε Java (Spark το χρειάζεται)
sudo apt install openjdk-11-jdk   # Ubuntu/Debian
sudo dnf install java-11-openjdk  # Fedora/RHEL
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

---

## Interview Q&A — Γρήγορη αναφορά

| Ερώτηση | Αρχείο |
|---------|--------|
| SCD2 implementation | `pipelines/silver_transformation.py` → `apply_scd2_customers()` |
| Incremental loading | `pipelines/bronze_ingestion.py` → `extract_incremental()` |
| DQ Framework | `quality_framework/dq_engine.py` → `run_quality_checks()` |
| Retry logic | `pipelines/master_pipeline.py` → `run_with_retry()` |
| Watermark CDC | `pipelines/logger.py` → `get_watermark() / update_watermark()` |
| Star schema | `pipelines/gold_pipeline.py` → `build_fact_sales()` |
| RFM segmentation | `pipelines/gold_pipeline.py` → `build_customer_segments()` |
| Cohort analysis | `pipelines/gold_pipeline.py` → `build_cohort_analysis()` |
| Negative tests | `tests/test_pipeline.py` → `TestNegativeScenarios` |
| Monitoring | `monitoring/grafana/dashboards/operations.json` |
