"""Pure PySpark transform functions.

Every function here takes DataFrame(s) in and returns DataFrame(s) out with no
S3 / Postgres / filesystem access, so `tests/` can exercise the real business
logic against a plain local SparkSession (see tests/conftest.py).

The only I/O in the whole pipeline lives in `jobs/*.py`, which read/write and
then call into this module for the actual logic.
"""

from __future__ import annotations

from datetime import date
from typing import Final

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

# Columns tracked for change detection in the customer SCD2 dimension.
CUSTOMER_TRACKED_COLS: Final[list[str]] = ["name", "segment", "province", "kyc_level"]

# Sentinel used only for range comparisons; never stored (open rows keep NULL).
_FAR_FUTURE = date(9999, 12, 31)

DIM_CUSTOMER_SCHEMA: Final[list[str]] = [
    "customer_key",
    "customer_id",
    "name",
    "segment",
    "province",
    "kyc_level",
    "effective_from",
    "effective_to",
    "is_current",
]


# --------------------------------------------------------------------------
# Data quality / cleaning
# --------------------------------------------------------------------------


def clean_transactions(raw: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Split raw transactions into (valid, rejected).

    Rejection rules, applied in order and recorded in `reject_reason`:
      - a required field is null (txn_id, txn_date, account_id, customer_id,
        branch_code, amount)
      - amount is negative
      - txn_id is a duplicate (first occurrence by txn_id wins, the rest of
        the duplicates are rejected)
    """
    required_cols = ["txn_id", "txn_date", "account_id", "customer_id", "branch_code", "amount"]
    null_mask = F.lit(False)
    for c in required_cols:
        null_mask = null_mask | F.col(c).isNull()

    negative_mask = F.col("amount").isNotNull() & (F.col("amount") < 0)

    tagged = raw.withColumn(
        "reject_reason",
        F.when(null_mask, F.lit("null_required_field"))
        .when(negative_mask, F.lit("negative_amount"))
        .otherwise(F.lit(None).cast(StringType())),
    )

    clean_so_far = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    already_rejected = tagged.filter(F.col("reject_reason").isNotNull())

    # Deduplicate remaining rows by txn_id: keep the first row seen (by input
    # order via monotonically_increasing_id), reject the rest as duplicates.
    ordered = clean_so_far.withColumn("_ord", F.monotonically_increasing_id())
    window = Window.partitionBy("txn_id").orderBy("_ord")
    ranked = ordered.withColumn("_rank", F.row_number().over(window))

    valid = ranked.filter(F.col("_rank") == 1).drop("_ord", "_rank")
    duplicates = (
        ranked.filter(F.col("_rank") > 1)
        .drop("_ord", "_rank")
        .withColumn("reject_reason", F.lit("duplicate_txn_id"))
    )

    rejected = already_rejected.unionByName(duplicates)
    return valid, rejected


# --------------------------------------------------------------------------
# Dimension builders
# --------------------------------------------------------------------------


def build_dim_date(spark: SparkSession, start_date: str, end_date: str) -> DataFrame:
    """Generate one row per calendar day in [start_date, end_date] (inclusive)."""
    bounds = spark.createDataFrame(
        [(start_date, end_date)], schema=StructType(
            [StructField("start_date", StringType()), StructField("end_date", StringType())]
        )
    )
    days = bounds.select(
        F.explode(
            F.sequence(F.to_date("start_date"), F.to_date("end_date"))
        ).alias("date")
    )
    return days.select(
        F.date_format("date", "yyyyMMdd").cast(IntegerType()).alias("date_key"),
        F.col("date"),
        F.year("date").alias("year"),
        F.quarter("date").alias("quarter"),
        F.month("date").alias("month"),
        F.date_format("date", "MMMM").alias("month_name"),
        F.dayofmonth("date").alias("day"),
        F.dayofweek("date").alias("day_of_week"),
        (F.dayofweek("date").isin(1, 7)).alias("is_weekend"),
    )


def build_dim_branch(raw_branches: DataFrame) -> DataFrame:
    """Assign a surrogate branch_key. Branches are static reference data, no SCD."""
    window = Window.orderBy("branch_code")
    return raw_branches.withColumn("branch_key", F.row_number().over(window)).select(
        "branch_key", "branch_code", "branch_name", "province", "region"
    )


def build_dim_account(raw_accounts: DataFrame) -> DataFrame:
    """Assign a surrogate account_key. Accounts are treated as slowly changing
    only in `status`, which we overwrite in place (SCD1) since a rewrite of
    account status history was not required for this showcase."""
    window = Window.orderBy("account_id")
    return raw_accounts.withColumn("account_key", F.row_number().over(window)).select(
        "account_key",
        "account_id",
        "customer_id",
        "product_type",
        "currency",
        F.to_date("opened_date").alias("opened_date"),
        "status",
    )


def _assign_new_keys(df: DataFrame, start_key: int) -> DataFrame:
    """Assign surrogate customer_key values start_key+1, start_key+2, ... in a
    deterministic order so re-running the merge on identical input is stable."""
    window = Window.orderBy("customer_id", "effective_from")
    return df.withColumn("customer_key", F.lit(start_key) + F.row_number().over(window))


def scd2_merge(current_dim: DataFrame, incoming: DataFrame) -> DataFrame:
    """Apply one incoming customer snapshot to the SCD2 dim_customer table.

    `current_dim` has the DIM_CUSTOMER_SCHEMA columns (may be empty).
    `incoming` has: customer_id, name, segment, province, kyc_level,
    snapshot_date (DateType) — one row per customer for this snapshot.

    Behaviour:
      - customer not seen before          -> new open row (is_current=True)
      - customer seen, no tracked change  -> existing row left untouched
      - customer seen, a tracked column
        changed                           -> existing current row closed
                                              (effective_to=snapshot_date,
                                              is_current=False) and a new
                                              open row is inserted
      - customer existed before but is
        absent from this snapshot         -> existing row left untouched
      - rows already closed by a prior
        merge                             -> carried forward untouched
    """
    if current_dim.rdd.isEmpty():
        opened = incoming.select(
            "customer_id",
            "name",
            "segment",
            "province",
            "kyc_level",
            F.col("snapshot_date").alias("effective_from"),
            F.lit(None).cast(DateType()).alias("effective_to"),
            F.lit(True).alias("is_current"),
        )
        return _assign_new_keys(opened, 0).select(DIM_CUSTOMER_SCHEMA)

    max_key = current_dim.agg(F.max("customer_key")).collect()[0][0] or 0

    current_open = current_dim.filter(F.col("is_current"))
    current_closed = current_dim.filter(~F.col("is_current"))

    joined = current_open.alias("cur").join(
        incoming.alias("inc"), on="customer_id", how="full_outer"
    )

    only_in_current = joined.filter(F.col("inc.snapshot_date").isNull()).select(
        [F.col(f"cur.{c}").alias(c) for c in DIM_CUSTOMER_SCHEMA]
    )

    both = joined.filter(
        F.col("cur.customer_key").isNotNull() & F.col("inc.snapshot_date").isNotNull()
    )

    changed_expr = F.lit(False)
    for c in CUSTOMER_TRACKED_COLS:
        changed_expr = changed_expr | (
            F.coalesce(F.col(f"cur.{c}"), F.lit("")) != F.coalesce(F.col(f"inc.{c}"), F.lit(""))
        )

    unchanged = both.filter(~changed_expr).select(
        [F.col(f"cur.{c}").alias(c) for c in DIM_CUSTOMER_SCHEMA]
    )

    changed = both.filter(changed_expr)
    closed_rows = changed.select(
        F.col("cur.customer_key").alias("customer_key"),
        F.col("cur.customer_id").alias("customer_id"),
        F.col("cur.name").alias("name"),
        F.col("cur.segment").alias("segment"),
        F.col("cur.province").alias("province"),
        F.col("cur.kyc_level").alias("kyc_level"),
        F.col("cur.effective_from").alias("effective_from"),
        F.col("inc.snapshot_date").alias("effective_to"),
        F.lit(False).alias("is_current"),
    )

    new_open_from_changed = changed.select(
        F.col("inc.customer_id").alias("customer_id"),
        F.col("inc.name").alias("name"),
        F.col("inc.segment").alias("segment"),
        F.col("inc.province").alias("province"),
        F.col("inc.kyc_level").alias("kyc_level"),
        F.col("inc.snapshot_date").alias("effective_from"),
    )

    only_in_incoming = joined.filter(F.col("cur.customer_key").isNull()).select(
        F.col("inc.customer_id").alias("customer_id"),
        F.col("inc.name").alias("name"),
        F.col("inc.segment").alias("segment"),
        F.col("inc.province").alias("province"),
        F.col("inc.kyc_level").alias("kyc_level"),
        F.col("inc.snapshot_date").alias("effective_from"),
    )

    pending_new = new_open_from_changed.unionByName(only_in_incoming).withColumn(
        "effective_to", F.lit(None).cast(DateType())
    ).withColumn("is_current", F.lit(True))

    new_rows = _assign_new_keys(pending_new, max_key).select(DIM_CUSTOMER_SCHEMA)

    return (
        unchanged.select(DIM_CUSTOMER_SCHEMA)
        .unionByName(closed_rows.select(DIM_CUSTOMER_SCHEMA))
        .unionByName(only_in_current.select(DIM_CUSTOMER_SCHEMA))
        .unionByName(current_closed.select(DIM_CUSTOMER_SCHEMA))
        .unionByName(new_rows)
    )


# --------------------------------------------------------------------------
# Fact builder
# --------------------------------------------------------------------------


def build_fact_transactions(
    clean_txns: DataFrame,
    dim_customer: DataFrame,
    dim_account: DataFrame,
    dim_branch: DataFrame,
) -> DataFrame:
    """Join cleaned transactions to dimension surrogate keys.

    `channel` is kept as a degenerate dimension directly on the fact row: it
    has a handful of low-cardinality values (branch/mobile/internet/atm) with
    no attributes of its own, so a separate dim table would only add a join.

    The customer_key lookup is point-in-time: a transaction is matched to the
    dim_customer row whose half-open [effective_from, effective_to) range
    contains the transaction date, so a transaction is attributed to the
    customer attributes that were actually in effect on that day. The upper
    bound is exclusive so a transaction dated exactly on the day a change
    took effect matches the new row, not the one being closed.
    """
    txns = clean_txns.withColumn("txn_date_d", F.to_date("txn_date")).withColumn(
        "date_key", F.date_format(F.col("txn_date_d"), "yyyyMMdd").cast(IntegerType())
    )

    dim_customer_ranged = dim_customer.withColumn(
        "effective_to_bound", F.coalesce(F.col("effective_to"), F.lit(_FAR_FUTURE))
    )

    fact = (
        txns.join(
            dim_customer_ranged.select(
                "customer_id", "customer_key", "effective_from", "effective_to_bound"
            ),
            (txns.customer_id == dim_customer_ranged.customer_id)
            & (txns.txn_date_d >= dim_customer_ranged.effective_from)
            & (txns.txn_date_d < dim_customer_ranged.effective_to_bound),
            "left",
        )
        .join(dim_account.select("account_key", "account_id"), "account_id", "left")
        .join(dim_branch.select("branch_key", "branch_code"), "branch_code", "left")
    )

    return fact.select(
        "txn_id",
        "date_key",
        "customer_key",
        "account_key",
        "branch_key",
        "channel",
        "txn_type",
        "is_cash",
        "amount",
        "fee",
    )


# --------------------------------------------------------------------------
# Regulatory report aggregations
# --------------------------------------------------------------------------


def ctr_report(
    fact_transactions: DataFrame,
    dim_customer: DataFrame,
    dim_branch: DataFrame,
    dim_date: DataFrame,
    threshold: int,
) -> DataFrame:
    """Cash transactions at or above `threshold`, one row per transaction.

    Mirrors a currency-transaction-threshold (CTR-style) AML report: every
    cash movement at or above the reporting threshold with enough customer
    and branch context for a filing. The customer_key on each fact row
    already resolves to whichever dim_customer row (current or historical)
    was in effect on the transaction date, so the join here uses the full
    dimension rather than only is_current rows.
    """
    hits = fact_transactions.filter(F.col("is_cash") & (F.col("amount") >= F.lit(threshold)))

    return (
        hits.join(dim_date.select("date_key", "date"), "date_key", "left")
        .join(
            dim_customer.select("customer_key", "customer_id", "name", "segment"),
            "customer_key",
            "left",
        )
        .join(
            dim_branch.select("branch_key", "branch_code", "branch_name", "province"),
            "branch_key",
            "left",
        )
        .select(
            "txn_id",
            "date",
            F.date_format("date", "yyyy-MM").alias("report_month"),
            "customer_id",
            "name",
            "segment",
            "branch_code",
            "branch_name",
            "province",
            "channel",
            "txn_type",
            "is_cash",
            "amount",
            "fee",
        )
        .orderBy("date", "txn_id")
    )


def monthly_summary_report(
    fact_transactions: DataFrame,
    dim_branch: DataFrame,
    dim_account: DataFrame,
    dim_date: DataFrame,
) -> DataFrame:
    """Month x branch x product_type x txn_type rollup: count and total amount.

    Stands in for a central-bank-style monthly summary return.
    """
    enriched = (
        fact_transactions.join(dim_date.select("date_key", "date"), "date_key", "left")
        .join(dim_branch.select("branch_key", "branch_name", "province"), "branch_key", "left")
        .join(
            dim_account.select("account_key", "product_type").dropDuplicates(["account_key"]),
            "account_key",
            "left",
        )
    )

    return (
        enriched.groupBy(
            F.date_format("date", "yyyy-MM").alias("report_month"),
            "branch_name",
            "province",
            "product_type",
            "txn_type",
        )
        .agg(
            F.count("*").alias("txn_count"),
            F.sum("amount").alias("total_amount"),
        )
        .orderBy("report_month", "branch_name", "product_type", "txn_type")
    )
