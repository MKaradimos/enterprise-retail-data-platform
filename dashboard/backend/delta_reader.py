"""Read Gold Delta tables from MinIO using deltalake (delta-rs)."""

import pandas as pd
from deltalake import DeltaTable

from config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET, GOLD_PREFIX

# S3-compatible storage options for MinIO
_storage_opts = {
    "AWS_ACCESS_KEY_ID": MINIO_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": MINIO_SECRET_KEY,
    "AWS_ENDPOINT_URL": MINIO_ENDPOINT,
    "AWS_REGION": "us-east-1",
    "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    "AWS_ALLOW_HTTP": "true",
}


def read_gold_table(table_name: str) -> pd.DataFrame:
    """Read a Gold-layer Delta table and return a pandas DataFrame."""
    uri = f"s3://{MINIO_BUCKET}/{GOLD_PREFIX}/{table_name}"
    dt = DeltaTable(uri, storage_options=_storage_opts)
    return dt.to_pandas()


def gold_table_exists(table_name: str) -> bool:
    """Check if a Gold Delta table exists."""
    try:
        read_gold_table(table_name)
        return True
    except Exception:
        return False
