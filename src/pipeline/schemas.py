"""Explicit StructTypes for every raw file the lake ingests.

Explicit schemas (rather than inferSchema) keep raw ingestion deterministic
and make the "dirty row" rejection rules in raw_to_staging predictable.
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

RAW_CUSTOMERS_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("province", StringType(), True),
        StructField("kyc_level", StringType(), True),
        StructField("snapshot_date", StringType(), True),
    ]
)

RAW_ACCOUNTS_SCHEMA = StructType(
    [
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_type", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("opened_date", StringType(), True),
        StructField("status", StringType(), True),
    ]
)

RAW_BRANCHES_SCHEMA = StructType(
    [
        StructField("branch_code", StringType(), True),
        StructField("branch_name", StringType(), True),
        StructField("province", StringType(), True),
        StructField("region", StringType(), True),
    ]
)

RAW_TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("txn_id", StringType(), True),
        StructField("txn_date", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("branch_code", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("txn_type", StringType(), True),
        StructField("is_cash", BooleanType(), True),
        StructField("amount", DoubleType(), True),
        StructField("fee", DoubleType(), True),
    ]
)

# Empty typed frame to seed the SCD2 merge on the very first run (no history yet).
EMPTY_DIM_CUSTOMER_SCHEMA = StructType(
    [
        StructField("customer_key", IntegerType(), True),
        StructField("customer_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("segment", StringType(), True),
        StructField("province", StringType(), True),
        StructField("kyc_level", StringType(), True),
        StructField("effective_from", DateType(), True),
        StructField("effective_to", DateType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)

__all__ = [
    "EMPTY_DIM_CUSTOMER_SCHEMA",
    "RAW_ACCOUNTS_SCHEMA",
    "RAW_BRANCHES_SCHEMA",
    "RAW_CUSTOMERS_SCHEMA",
    "RAW_TRANSACTIONS_SCHEMA",
]
