"""Central configuration, all sourced from environment variables with safe defaults.

Keeping every tunable in one module means jobs, tests, and docker-compose stay in
sync: change an env var in `.env` and every job that needs it picks it up.
"""

from __future__ import annotations

import os


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name, str(default)))


# --- Object storage (LocalStack S3 stands in for real S3) -----------------
S3_ENDPOINT: str = os.environ.get("S3_ENDPOINT", "http://localstack:4566")
S3_BUCKET: str = os.environ.get("S3_BUCKET", "lake")
AWS_ACCESS_KEY_ID: str = os.environ.get("AWS_ACCESS_KEY_ID", "test")
AWS_SECRET_ACCESS_KEY: str = os.environ.get("AWS_SECRET_ACCESS_KEY", "test")
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")

RAW_PATH: str = f"s3a://{S3_BUCKET}/raw"
STAGING_PATH: str = f"s3a://{S3_BUCKET}/staging"
REJECTS_PATH: str = f"s3a://{S3_BUCKET}/staging/_rejects"
WAREHOUSE_PATH: str = f"s3a://{S3_BUCKET}/warehouse"
REPORTS_PATH: str = f"s3a://{S3_BUCKET}/reports"

# --- Postgres (stands in for Redshift) -------------------------------------
POSTGRES_HOST: str = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT: str = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB: str = os.environ.get("POSTGRES_DB", "warehouse")
POSTGRES_USER: str = os.environ.get("POSTGRES_USER", "warehouse")
POSTGRES_PASSWORD: str = os.environ.get("POSTGRES_PASSWORD", "warehouse")
JDBC_URL: str = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"

# --- Local mirror of report output (so a reviewer doesn't need S3 tooling) -
LOCAL_REPORTS_DIR: str = os.environ.get("LOCAL_REPORTS_DIR", "/workspace/data/reports")

# --- Business rules ----------------------------------------------------------
CTR_THRESHOLD: int = _env_int("CTR_THRESHOLD", 2_000_000)

# --- Synthetic data generation (kept deterministic for reproducible runs) --
RANDOM_SEED: int = _env_int("RANDOM_SEED", 42)
NUM_CUSTOMERS: int = _env_int("NUM_CUSTOMERS", 2_000)
NUM_ACCOUNTS: int = _env_int("NUM_ACCOUNTS", 3_000)
NUM_TRANSACTIONS: int = _env_int("NUM_TRANSACTIONS", 60_000)
DATA_END_DATE: str = os.environ.get("DATA_END_DATE", "2026-06-30")
DATA_WINDOW_DAYS: int = _env_int("DATA_WINDOW_DAYS", 90)
