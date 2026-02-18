# =============================================================
# RetailNova Enterprise Data Platform
# Makefile — Linux shortcut commands
#
# Usage:
#   make setup     → first-time setup (pulls images, seeds DB)
#   make run       → run full pipeline
#   make test      → run unit tests (local mode, no Docker needed)
#   make status    → show container health
#   make logs      → tail all logs
#   make down      → stop all containers
# =============================================================

# ─── Auto-detect docker compose command ──────────────────────
COMPOSE := $(shell docker compose version > /dev/null 2>&1 && echo "docker compose" || echo "docker-compose")
PYTHON  := $(shell command -v python3 || command -v python)
PIP     := $(shell command -v pip3 || command -v pip)

.PHONY: help setup run run-bronze run-silver run-gold run-quality \
        test test-verbose seed status logs down clean restart \
        jupyter grafana spark minio psql

# ─── Default target ──────────────────────────────────────────
help:
	@echo ""
	@echo "  RetailNova Data Platform — Available Commands"
	@echo "  ─────────────────────────────────────────────"
	@echo "  make setup        First-time setup (Docker + DB seed)"
	@echo "  make run          Full pipeline: Bronze → Silver → Gold"
	@echo "  make run-bronze   Bronze layer only"
	@echo "  make run-silver   Silver layer only"
	@echo "  make run-gold     Gold layer only"
	@echo "  make run-quality  Data quality checks only"
	@echo "  make test         Unit tests (local Spark, no Docker needed)"
	@echo "  make test-verbose Unit tests with full output"
	@echo "  make seed         Re-seed SQL Server"
	@echo "  make status       Show container health"
	@echo "  make logs         Tail all container logs"
	@echo "  make logs-spark   Tail Spark logs"
	@echo "  make logs-meta    Tail PostgreSQL logs"
	@echo "  make jupyter      Open Jupyter in browser"
	@echo "  make grafana      Open Grafana in browser"
	@echo "  make spark        Open Spark UI in browser"
	@echo "  make minio        Open MinIO console in browser"
	@echo "  make psql         Connect to metadata PostgreSQL"
	@echo "  make restart      Restart all containers"
	@echo "  make down         Stop and remove containers"
	@echo "  make clean        Remove containers + volumes (WARNING: deletes data)"
	@echo ""

# ─── First-time setup ────────────────────────────────────────
setup:
	@chmod +x setup.sh
	@./setup.sh

# ─── Pipeline runners ────────────────────────────────────────
run:
	@echo "\n[Pipeline] Running full stack: Bronze → Silver → Gold\n"
	@$(PYTHON) run_pipeline.py --layer all

run-bronze:
	@$(PYTHON) run_pipeline.py --layer bronze

run-silver:
	@$(PYTHON) run_pipeline.py --layer silver

run-gold:
	@$(PYTHON) run_pipeline.py --layer gold

run-quality:
	@$(PYTHON) run_pipeline.py --layer quality

run-fast:
	@$(PYTHON) run_pipeline.py --layer all --skip-quality

# ─── Tests (run locally — uses local[*] Spark, no cluster) ───
test:
	@echo "\n[Tests] Running unit test suite (local Spark mode)\n"
	@SPARK_MASTER=local[*] RETAILNOVA_ENV=dev \
	 $(PYTHON) -m pytest tests/test_pipeline.py -v --tb=short -q

test-verbose:
	@SPARK_MASTER=local[*] RETAILNOVA_ENV=dev \
	 $(PYTHON) -m pytest tests/test_pipeline.py -v --tb=long -s

test-coverage:
	@SPARK_MASTER=local[*] RETAILNOVA_ENV=dev \
	 $(PYTHON) -m pytest tests/test_pipeline.py --cov=pipelines --cov=quality_framework \
	 --cov-report=term-missing -q

# ─── DB seed ─────────────────────────────────────────────────
seed:
	@echo "[Seed] Seeding SQL Server..."
	@docker exec retailnova_sqlserver bash -c \
	  "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'RetailNova@2024' \
	   -i /scripts/01_seed_oltp.sql -C -b 2>/dev/null || \
	   /opt/mssql-tools/bin/sqlcmd  -S localhost -U sa -P 'RetailNova@2024' \
	   -i /scripts/01_seed_oltp.sql -b"
	@echo "[Seed] Done"

# ─── Container management ────────────────────────────────────
status:
	@echo "\n[Status] Container health:\n"
	@docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" \
	  --filter "name=retailnova" 2>/dev/null || echo "No containers running"
	@echo ""
	@echo "[Status] Checking service ports:"
	@for port in 1433 9000 9001 7077 8080 8888 5432 3000; do \
	  if nc -z localhost $$port 2>/dev/null; then \
	    echo "  ✓ localhost:$$port reachable"; \
	  else \
	    echo "  ✗ localhost:$$port NOT reachable"; \
	  fi; \
	done
	@echo ""

logs:
	@$(COMPOSE) -f docker/docker-compose.yml logs -f --tail=50

logs-spark:
	@docker logs -f retailnova_spark --tail=50

logs-meta:
	@docker logs -f retailnova_metadata --tail=50

logs-sql:
	@docker logs -f retailnova_sqlserver --tail=50

restart:
	@$(COMPOSE) -f docker/docker-compose.yml restart

down:
	@echo "[Down] Stopping containers..."
	@$(COMPOSE) -f docker/docker-compose.yml down
	@echo "[Down] Done"

clean:
	@echo "[Clean] Removing containers AND volumes (data will be lost)..."
	@read -p "Are you sure? [y/N] " confirm && [ "$$confirm" = "y" ] || exit 1
	@$(COMPOSE) -f docker/docker-compose.yml down -v
	@echo "[Clean] Done"

# ─── Browser shortcuts ───────────────────────────────────────
jupyter:
	@xdg-open http://localhost:8888/lab?token=retailnova2024 2>/dev/null || \
	 echo "Open: http://localhost:8888  (token: retailnova2024)"

grafana:
	@xdg-open http://localhost:3000 2>/dev/null || \
	 echo "Open: http://localhost:3000  (admin / RetailNova@2024)"

spark:
	@xdg-open http://localhost:8080 2>/dev/null || \
	 echo "Open: http://localhost:8080"

minio:
	@xdg-open http://localhost:9001 2>/dev/null || \
	 echo "Open: http://localhost:9001  (retailnova_admin / RetailNova@2024)"

# ─── DB access ───────────────────────────────────────────────
psql:
	@docker exec -it retailnova_metadata \
	 psql -U retailnova -d retailnova_metadata

# ─── Useful queries ──────────────────────────────────────────
watermarks:
	@docker exec retailnova_metadata \
	 psql -U retailnova -d retailnova_metadata \
	 -c "SELECT table_name, last_watermark, status, rows_extracted FROM pipeline_watermarks ORDER BY table_name;"

quality-report:
	@docker exec retailnova_metadata \
	 psql -U retailnova -d retailnova_metadata \
	 -c "SELECT target_table, rule_type, status, ROUND(pass_rate_pct,1) AS pass_pct, run_at FROM data_quality_log ORDER BY run_at DESC LIMIT 20;"

exec-log:
	@docker exec retailnova_metadata \
	 psql -U retailnova -d retailnova_metadata \
	 -c "SELECT pipeline_name, status, rows_written, ROUND(duration_seconds,1) AS secs, started_at FROM execution_log ORDER BY started_at DESC LIMIT 15;"

errors:
	@docker exec retailnova_metadata \
	 psql -U retailnova -d retailnova_metadata \
	 -c "SELECT pipeline_name, error_type, severity, LEFT(error_message,80) AS msg, occurred_at FROM error_log ORDER BY occurred_at DESC LIMIT 10;"
