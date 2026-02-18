#!/usr/bin/env bash
# =============================================================
# RetailNova Enterprise Data Platform
# Linux Setup Script
# =============================================================
# Requirements: Docker (with Compose plugin), Python 3.9+
# Tested on: Ubuntu 22.04 / Debian 12 / Fedora 38
# =============================================================

set -euo pipefail

# ─── Colours ────────────────────────────────────────────────────────────────
RED="\033[0;31m"; GREEN="\033[0;32m"; YELLOW="\033[1;33m"
BOLD="\033[1m";   NC="\033[0m"

info()    { echo -e "${GREEN}[✓]${NC} $*"; }
warn()    { echo -e "${YELLOW}[!]${NC} $*"; }
error()   { echo -e "${RED}[✗]${NC} $*"; exit 1; }
section() { echo -e "\n${BOLD}── $* ──────────────────────────────────────────${NC}"; }

echo ""
echo -e "${BOLD}╔════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║   RetailNova Enterprise Data Platform  •  Linux Setup  ║${NC}"
echo -e "${BOLD}╚════════════════════════════════════════════════════════╝${NC}"
echo ""

# ─── Pre-flight checks ──────────────────────────────────────────────────────
section "Pre-flight checks"

# Docker
if ! command -v docker &>/dev/null; then
    error "Docker not found. Install: https://docs.docker.com/engine/install/"
fi
info "Docker: $(docker --version)"

# Docker Compose (v2 plugin preferred, v1 fallback)
if docker compose version &>/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
    info "Docker Compose v2: $(docker compose version --short)"
elif command -v docker-compose &>/dev/null; then
    COMPOSE_CMD="docker-compose"
    info "Docker Compose v1: $(docker-compose --version)"
else
    error "Docker Compose not found. Install plugin: sudo apt install docker-compose-plugin"
fi

# Python
PYTHON=$(command -v python3 || command -v python || true)
[[ -z "$PYTHON" ]] && error "Python 3 not found."
info "Python: $($PYTHON --version)"

# pip
PIP=$(command -v pip3 || command -v pip || true)
[[ -z "$PIP" ]] && error "pip not found. Install: sudo apt install python3-pip"

# Docker daemon running?
docker info &>/dev/null || error "Docker daemon not running. Start it: sudo systemctl start docker"

# ─── .env file ──────────────────────────────────────────────────────────────
section "Creating .env"
cat > .env <<'EOF'
RETAILNOVA_ENV=dev
SQLSERVER_HOST=localhost
SQLSERVER_PORT=1433
SQLSERVER_DB=RetailNova_OLTP
SQLSERVER_USER=sa
SQLSERVER_PASS=RetailNova@2024
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=retailnova_admin
MINIO_SECRET_KEY=RetailNova@2024
MINIO_BUCKET=retailnova-dev
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=retailnova_metadata
POSTGRES_USER=retailnova
POSTGRES_PASS=RetailNova@2024
SPARK_MASTER=local[*]
ALERT_EMAIL=dataeng@retailnova.gr
EOF
# Inside containers, services are reached by service name (not localhost).
# The .env above is for scripts running ON the host machine.
info ".env created (host-mode settings)"

# ─── Python dependencies ────────────────────────────────────────────────────
section "Python dependencies"
$PIP install -r requirements.txt --quiet --break-system-packages 2>/dev/null \
    || $PIP install -r requirements.txt --quiet
info "Python packages installed"

# ─── Docker services ────────────────────────────────────────────────────────
section "Starting Docker services"
cd docker
$COMPOSE_CMD pull --quiet
$COMPOSE_CMD up -d
cd ..
info "Containers started (background)"

# ─── Wait for PostgreSQL ────────────────────────────────────────────────────
section "Waiting for PostgreSQL"
echo -n "  Waiting"
for i in $(seq 1 40); do
    if docker exec retailnova_metadata pg_isready -U retailnova -d retailnova_metadata -q 2>/dev/null; then
        echo ""
        info "PostgreSQL ready"
        break
    fi
    echo -n "."
    sleep 3
    [[ $i -eq 40 ]] && error "PostgreSQL did not start in time. Run: docker logs retailnova_metadata"
done

# ─── Wait for SQL Server ────────────────────────────────────────────────────
section "Waiting for SQL Server"
echo -n "  Waiting (SQL Server takes ~30s on first run)"
for i in $(seq 1 30); do
    if docker exec retailnova_sqlserver \
        bash -c "/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P 'RetailNova@2024' -Q 'SELECT 1' -C -b 2>/dev/null || \
                 /opt/mssql-tools/bin/sqlcmd  -S localhost -U sa -P 'RetailNova@2024' -Q 'SELECT 1' -b 2>/dev/null" \
        &>/dev/null; then
        echo ""
        info "SQL Server ready"
        break
    fi
    echo -n "."
    sleep 4
    [[ $i -eq 30 ]] && { echo ""; warn "SQL Server slow — seed script may need a retry. Run: make seed"; }
done

# ─── Seed SQL Server ────────────────────────────────────────────────────────
section "Seeding SQL Server"
# Try both sqlcmd paths (mssql-tools18 for newer images, mssql-tools for older)
SEEDED=false
for SQLCMD in /opt/mssql-tools18/bin/sqlcmd /opt/mssql-tools/bin/sqlcmd; do
    if docker exec retailnova_sqlserver \
        bash -c "$SQLCMD -S localhost -U sa -P 'RetailNova@2024' \
                 -i /scripts/01_seed_oltp.sql -C -b 2>&1 | tail -5" 2>/dev/null; then
        SEEDED=true
        break
    fi
done

if $SEEDED; then
    info "SQL Server seeded with RetailNova_OLTP data"
else
    warn "SQL Server seed had an issue. Try manually: make seed"
fi

# ─── Done ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  SETUP COMPLETE!                                                  ║${NC}"
echo -e "${BOLD}║                                                                   ║${NC}"
echo -e "${BOLD}║  Service URLs:                                                    ║${NC}"
echo -e "${BOLD}║    Jupyter Lab   → http://localhost:8888                          ║${NC}"
echo -e "${BOLD}║                    Token: retailnova2024                          ║${NC}"
echo -e "${BOLD}║    Spark UI      → http://localhost:8080                          ║${NC}"
echo -e "${BOLD}║    MinIO Console → http://localhost:9001                          ║${NC}"
echo -e "${BOLD}║                    admin: retailnova_admin / RetailNova@2024      ║${NC}"
echo -e "${BOLD}║    Grafana       → http://localhost:3000                          ║${NC}"
echo -e "${BOLD}║                    admin / RetailNova@2024                        ║${NC}"
echo -e "${BOLD}║                                                                   ║${NC}"
echo -e "${BOLD}║  Next steps:                                                      ║${NC}"
echo -e "${BOLD}║    make run        → full pipeline (Bronze→Silver→Gold)           ║${NC}"
echo -e "${BOLD}║    make test       → unit test suite                              ║${NC}"
echo -e "${BOLD}║    make status     → check service health                         ║${NC}"
echo -e "${BOLD}║    make logs       → tail all container logs                      ║${NC}"
echo -e "${BOLD}║    make down       → stop everything                              ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════════════════════════╝${NC}"
echo ""
