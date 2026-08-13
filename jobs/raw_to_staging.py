"""raw -> staging: validate/clean/dedupe the raw CSV feeds into Parquet.

Reference data (branches, accounts, customer snapshots) is schema-cast and
passed through as-is. The transactions feed is the one raw source seeded with
deliberately dirty rows, so it runs through `transforms.clean_transactions`;
rejected rows land in s3a://lake/staging/_rejects/transactions with a reason
column, and the counts are printed so a reviewer can see the quality gate
working without opening the files.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline import config, schemas
from pipeline.session import get_spark_session
from pipeline.transforms import clean_transactions


def main() -> None:
    spark = get_spark_session("raw_to_staging")

    branches = spark.read.option("header", "true").schema(schemas.RAW_BRANCHES_SCHEMA).csv(
        f"{config.RAW_PATH}/branches"
    )
    accounts = spark.read.option("header", "true").schema(schemas.RAW_ACCOUNTS_SCHEMA).csv(
        f"{config.RAW_PATH}/accounts"
    )
    customers_snapshot1 = spark.read.option("header", "true").schema(schemas.RAW_CUSTOMERS_SCHEMA).csv(
        f"{config.RAW_PATH}/customers_snapshot1"
    )
    customers_snapshot2 = spark.read.option("header", "true").schema(schemas.RAW_CUSTOMERS_SCHEMA).csv(
        f"{config.RAW_PATH}/customers_snapshot2"
    )
    raw_transactions = spark.read.option("header", "true").schema(schemas.RAW_TRANSACTIONS_SCHEMA).csv(
        f"{config.RAW_PATH}/transactions"
    )

    valid_transactions, rejected_transactions = clean_transactions(raw_transactions)

    total = raw_transactions.count()
    valid_count = valid_transactions.count()
    rejected_count = rejected_transactions.count()
    print(f"[raw_to_staging] transactions total={total} valid={valid_count} rejected={rejected_count}")
    rejected_transactions.groupBy("reject_reason").count().show(truncate=False)

    branches.write.mode("overwrite").parquet(f"{config.STAGING_PATH}/branches")
    accounts.write.mode("overwrite").parquet(f"{config.STAGING_PATH}/accounts")
    customers_snapshot1.write.mode("overwrite").parquet(f"{config.STAGING_PATH}/customers_snapshot1")
    customers_snapshot2.write.mode("overwrite").parquet(f"{config.STAGING_PATH}/customers_snapshot2")
    valid_transactions.write.mode("overwrite").parquet(f"{config.STAGING_PATH}/transactions")
    rejected_transactions.write.mode("overwrite").parquet(f"{config.REJECTS_PATH}/transactions")

    print("[raw_to_staging] staging zone written to", config.STAGING_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
