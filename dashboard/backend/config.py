"""Dashboard backend configuration — reads from project root .env"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load project root .env
_env_path = Path(__file__).resolve().parents[2] / ".env"
if _env_path.exists():
    load_dotenv(_env_path, override=False)

# PostgreSQL metadata
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", "5433"))
PG_DB = os.getenv("POSTGRES_DB", "retailnova_metadata")
PG_USER = os.getenv("POSTGRES_USER", "retailnova")
PG_PASS = os.getenv("POSTGRES_PASS", "RetailNova@2024")

# MinIO / S3
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "retailnova_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "RetailNova@2024")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "retailnova-dev")

GOLD_PREFIX = "gold"
