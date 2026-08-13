"""staging -> warehouse: build the star schema and load it into Postgres.

Dimensions are built once, the customer snapshots are applied in order
through `transforms.scd2_merge` (snapshot1 then snapshot2) so dim_customer
ends up with real history, and fact_transactions is joined against the
resulting dimensions. Everything is written as Parquet under
s3a://lake/warehouse/ (the "Glue Catalog table" analogue) and then loaded
into Postgres over JDBC, which plays the role of Redshift in this showcase.
"""

from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pyspark.sql import functions as F

from pipeline import config, schemas
from pipeline.session import get_spark_session
from pipeline.transforms import (
    build_dim_account,
    build_dim_branch,
    build_dim_date,
    build_fact_transactions,
    scd2_merge,
)

TABLES_IN_LOAD_ORDER = ["dim_date", "dim_branch", "dim_account", "dim_customer", "fact_transactions"]


def write_warehouse_parquet(df, name: str) -> None:
    df.write.mode("overwrite").parquet(f"{config.WAREHOUSE_PATH}/{name}")


def write_jdbc(df, table: str) -> None:
    (
        df.write.format("jdbc")
        .option("url", config.JDBC_URL)
        .option("dbtable", table)
        .option("user", config.POSTGRES_USER)
        .option("password", config.POSTGRES_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save()
    )


def main() -> None:
    spark = get_spark_session("staging_to_warehouse")

    end_date = date.fromisoformat(config.DATA_END_DATE)
    window_start = end_date - timedelta(days=config.DATA_WINDOW_DAYS - 1)

    raw_branches = spark.read.parquet(f"{config.STAGING_PATH}/branches")
    raw_accounts = spark.read.parquet(f"{config.STAGING_PATH}/accounts")
    snapshot1 = spark.read.parquet(f"{config.STAGING_PATH}/customers_snapshot1").withColumn(
        "snapshot_date", F.to_date("snapshot_date")
    )
    snapshot2 = spark.read.parquet(f"{config.STAGING_PATH}/customers_snapshot2").withColumn(
        "snapshot_date", F.to_date("snapshot_date")
    )
    transactions = spark.read.parquet(f"{config.STAGING_PATH}/transactions")

    dim_date = build_dim_date(spark, window_start.isoformat(), end_date.isoformat())
    dim_branch = build_dim_branch(raw_branches)
    dim_account = build_dim_account(raw_accounts)

    empty_dim_customer = spark.createDataFrame([], schema=schemas.EMPTY_DIM_CUSTOMER_SCHEMA)
    after_snapshot1 = scd2_merge(empty_dim_customer, snapshot1)
    dim_customer = scd2_merge(after_snapshot1, snapshot2)

    fact_transactions = build_fact_transactions(transactions, dim_customer, dim_account, dim_branch)

    current_count = dim_customer.filter(F.col("is_current")).count()
    total_customer_rows = dim_customer.count()
    print(f"[staging_to_warehouse] dim_customer rows={total_customer_rows} is_current={current_count}")
    print(f"[staging_to_warehouse] dim_branch={dim_branch.count()} dim_account={dim_account.count()} dim_date={dim_date.count()}")
    print(f"[staging_to_warehouse] fact_transactions={fact_transactions.count()}")

    tables = {
        "dim_date": dim_date,
        "dim_branch": dim_branch,
        "dim_account": dim_account,
        "dim_customer": dim_customer,
        "fact_transactions": fact_transactions,
    }
    for name in TABLES_IN_LOAD_ORDER:
        write_warehouse_parquet(tables[name], name)

    for name in TABLES_IN_LOAD_ORDER:
        write_jdbc(tables[name], name)
        print(f"[staging_to_warehouse] loaded {name} into Postgres")

    spark.stop()


if __name__ == "__main__":
    main()
