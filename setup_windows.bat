@echo off
REM ============================================================
REM RetailNova Enterprise Data Platform - Windows Setup Script
REM ============================================================
REM Requirements: Docker Desktop, Python 3.x, Git
REM ============================================================

echo.
echo  ╔══════════════════════════════════════════════════╗
echo  ║  RetailNova Enterprise Data Platform Setup       ║
echo  ╚══════════════════════════════════════════════════╝
echo.

REM ── Check Docker ─────────────────────────────────────────────────────────
docker --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo  [ERROR] Docker Desktop not found! Please install from:
    echo          https://www.docker.com/products/docker-desktop
    exit /b 1
)
echo  [OK] Docker found

REM ── Check Python ─────────────────────────────────────────────────────────
python --version >nul 2>&1
IF %ERRORLEVEL% NEQ 0 (
    echo  [ERROR] Python not found!
    exit /b 1
)
echo  [OK] Python found

REM ── Step 1: Create .env file ──────────────────────────────────────────────
echo  Creating .env file...
(
echo RETAILNOVA_ENV=dev
echo SQLSERVER_HOST=sqlserver
echo SQLSERVER_PORT=1433
echo SQLSERVER_DB=RetailNova_OLTP
echo SQLSERVER_USER=sa
echo SQLSERVER_PASS=RetailNova@2024
echo MINIO_ENDPOINT=http://minio:9000
echo MINIO_ACCESS_KEY=retailnova_admin
echo MINIO_SECRET_KEY=RetailNova@2024
echo MINIO_BUCKET=retailnova-dev
echo POSTGRES_HOST=metadata_db
echo POSTGRES_PORT=5432
echo POSTGRES_DB=retailnova_metadata
echo POSTGRES_USER=retailnova
echo POSTGRES_PASS=RetailNova@2024
echo SPARK_MASTER=spark://spark:7077
echo ALERT_EMAIL=dataeng@retailnova.gr
) > .env
echo  [OK] .env created

REM ── Step 2: Start Docker services ────────────────────────────────────────
echo.
echo  Starting Docker services (this may take 3-5 minutes on first run)...
echo  Pulling images...

cd docker
docker-compose pull
docker-compose up -d

echo.
echo  Waiting for services to be healthy (60 seconds)...
timeout /t 60 /nobreak >nul

REM ── Step 3: Create MinIO buckets ─────────────────────────────────────────
echo  Setting up MinIO storage buckets...
docker exec retailnova_minio mc alias set local http://localhost:9000 retailnova_admin RetailNova@2024
docker exec retailnova_minio mc mb local/retailnova-dev --ignore-existing
docker exec retailnova_minio mc mb local/retailnova-dev/bronze --ignore-existing
docker exec retailnova_minio mc mb local/retailnova-dev/silver --ignore-existing
docker exec retailnova_minio mc mb local/retailnova-dev/gold --ignore-existing
echo  [OK] MinIO buckets created

cd ..

REM ── Step 4: Python dependencies ──────────────────────────────────────────
echo.
echo  Installing Python dependencies...
pip install -r requirements.txt
echo  [OK] Python packages installed

REM ── Done! ────────────────────────────────────────────────────────────────
echo.
echo  ╔══════════════════════════════════════════════════════════════╗
echo  ║  SETUP COMPLETE! Services are running:                       ║
echo  ║                                                              ║
echo  ║  Jupyter Lab  : http://localhost:8888 (token: retailnova2024)║
echo  ║  Spark UI     : http://localhost:8080                        ║
echo  ║  MinIO Console: http://localhost:9001 (admin/RetailNova@2024)║
echo  ║  Grafana      : http://localhost:3000 (admin/RetailNova@2024)║
echo  ║                                                              ║
echo  ║  To run the full pipeline:                                   ║
echo  ║    python run_pipeline.py --layer all                        ║
echo  ║                                                              ║
echo  ║  To run tests:                                               ║
echo  ║    python -m pytest tests/ -v                                ║
echo  ╚══════════════════════════════════════════════════════════════╝
echo.
pause
