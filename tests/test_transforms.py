from __future__ import annotations

from datetime import date

from pyspark.sql import Row

from pipeline import schemas
from pipeline.transforms import (
    build_dim_account,
    build_dim_branch,
    build_dim_date,
    build_fact_transactions,
    clean_transactions,
    ctr_report,
    monthly_summary_report,
)


def _txn_row(**overrides):
    base = {
        "txn_id": "TXN0000001",
        "txn_date": "2026-04-05",
        "account_id": "ACC0000001",
        "customer_id": "CUST000001",
        "branch_code": "BR001",
        "channel": "branch",
        "txn_type": "deposit",
        "is_cash": True,
        "amount": 1000.0,
        "fee": 0.0,
    }
    base.update(overrides)
    return Row(**base)


def test_clean_transactions_keeps_valid_rows(spark):
    df = spark.createDataFrame([_txn_row(), _txn_row(txn_id="TXN0000002")])
    valid, rejected = clean_transactions(df)
    assert valid.count() == 2
    assert rejected.count() == 0


def test_clean_transactions_rejects_null_required_field(spark):
    df = spark.createDataFrame([_txn_row(), _txn_row(txn_id="TXN0000002", customer_id=None)])
    valid, rejected = clean_transactions(df)
    assert valid.count() == 1
    reasons = {r.reject_reason for r in rejected.collect()}
    assert reasons == {"null_required_field"}


def test_clean_transactions_rejects_negative_amount(spark):
    df = spark.createDataFrame([_txn_row(), _txn_row(txn_id="TXN0000002", amount=-50.0)])
    valid, rejected = clean_transactions(df)
    assert valid.count() == 1
    assert rejected.collect()[0].reject_reason == "negative_amount"


def test_clean_transactions_rejects_duplicate_txn_id(spark):
    df = spark.createDataFrame(
        [_txn_row(), _txn_row(), _txn_row(txn_id="TXN0000002")]
    )
    valid, rejected = clean_transactions(df)
    # one duplicate of TXN0000001 kept as valid, the extra copy rejected
    assert valid.count() == 2
    assert rejected.count() == 1
    assert rejected.collect()[0].reject_reason == "duplicate_txn_id"


def test_build_dim_date_row_count_and_fields(spark):
    dim_date = build_dim_date(spark, "2026-01-01", "2026-01-10")
    rows = dim_date.orderBy("date_key").collect()
    assert len(rows) == 10
    assert rows[0].date_key == 20260101
    assert rows[0].date == date(2026, 1, 1)
    assert rows[0].year == 2026
    assert rows[0].month == 1
    assert rows[0].quarter == 1
    # 2026-01-03 is a Saturday
    saturday = next(r for r in rows if r.date == date(2026, 1, 3))
    assert saturday.is_weekend is True
    monday = next(r for r in rows if r.date == date(2026, 1, 5))
    assert monday.is_weekend is False


def test_build_dim_branch_assigns_unique_keys(spark):
    df = spark.createDataFrame(
        [
            Row(branch_code="BR002", branch_name="B", province="Phuket", region="Southern"),
            Row(branch_code="BR001", branch_name="A", province="Bangkok", region="Central"),
        ]
    )
    dim_branch = build_dim_branch(df)
    keys = sorted(r.branch_key for r in dim_branch.collect())
    assert keys == [1, 2]
    assert dim_branch.count() == df.count()


def test_build_dim_account_assigns_keys_and_casts_date(spark):
    df = spark.createDataFrame(
        [
            Row(
                account_id="ACC0000001",
                customer_id="CUST000001",
                product_type="savings",
                currency="THB",
                opened_date="2020-01-15",
                status="active",
            )
        ]
    )
    dim_account = build_dim_account(df)
    row = dim_account.collect()[0]
    assert row.account_key == 1
    assert row.opened_date == date(2020, 1, 15)


def _dim_customer_row(**overrides):
    base = {
        "customer_key": 1,
        "customer_id": "CUST000001",
        "name": "Somchai Srisawat",
        "segment": "retail",
        "province": "Bangkok",
        "kyc_level": "basic",
        "effective_from": date(2026, 1, 1),
        "effective_to": None,
        "is_current": True,
    }
    base.update(overrides)
    return Row(**base)


def test_build_fact_transactions_attaches_surrogate_keys(spark):
    dim_customer = spark.createDataFrame([_dim_customer_row()], schema=schemas.EMPTY_DIM_CUSTOMER_SCHEMA)
    dim_account = spark.createDataFrame(
        [
            Row(
                account_key=1,
                account_id="ACC0000001",
                customer_id="CUST000001",
                product_type="savings",
                currency="THB",
                opened_date=date(2020, 1, 1),
                status="active",
            )
        ]
    )
    dim_branch = spark.createDataFrame(
        [Row(branch_key=1, branch_code="BR001", branch_name="Bangkok Branch", province="Bangkok", region="Central")]
    )
    txns = spark.createDataFrame([_txn_row()])

    fact = build_fact_transactions(txns, dim_customer, dim_account, dim_branch)
    row = fact.collect()[0]
    assert row.customer_key == 1
    assert row.account_key == 1
    assert row.branch_key == 1
    assert row.date_key == 20260405


def test_ctr_report_filters_cash_at_or_above_threshold(spark):
    fact = spark.createDataFrame(
        [
            Row(txn_id="T1", date_key=20260405, customer_key=1, account_key=1, branch_key=1, channel="branch", txn_type="deposit", is_cash=True, amount=3_000_000.0, fee=0.0),
            Row(txn_id="T2", date_key=20260405, customer_key=1, account_key=1, branch_key=1, channel="branch", txn_type="deposit", is_cash=True, amount=1_000.0, fee=0.0),
            Row(txn_id="T3", date_key=20260405, customer_key=1, account_key=1, branch_key=1, channel="mobile", txn_type="transfer_out", is_cash=False, amount=5_000_000.0, fee=1.0),
        ]
    )
    dim_customer = spark.createDataFrame([_dim_customer_row()], schema=schemas.EMPTY_DIM_CUSTOMER_SCHEMA)
    dim_branch = spark.createDataFrame(
        [Row(branch_key=1, branch_code="BR001", branch_name="Bangkok Branch", province="Bangkok", region="Central")]
    )
    dim_date = build_dim_date(spark, "2026-04-01", "2026-04-10")

    ctr = ctr_report(fact, dim_customer, dim_branch, dim_date, threshold=2_000_000)
    rows = ctr.collect()
    assert len(rows) == 1
    assert rows[0].txn_id == "T1"


def test_monthly_summary_report_aggregates_correctly(spark):
    fact = spark.createDataFrame(
        [
            Row(txn_id="T1", date_key=20260405, customer_key=1, account_key=1, branch_key=1, channel="branch", txn_type="deposit", is_cash=True, amount=100.0, fee=0.0),
            Row(txn_id="T2", date_key=20260406, customer_key=1, account_key=1, branch_key=1, channel="branch", txn_type="deposit", is_cash=True, amount=200.0, fee=0.0),
            Row(txn_id="T3", date_key=20260406, customer_key=1, account_key=1, branch_key=1, channel="mobile", txn_type="payment", is_cash=False, amount=50.0, fee=1.0),
        ]
    )
    dim_branch = spark.createDataFrame(
        [Row(branch_key=1, branch_code="BR001", branch_name="Bangkok Branch", province="Bangkok", region="Central")]
    )
    dim_account = spark.createDataFrame(
        [Row(account_key=1, account_id="ACC0000001", customer_id="CUST000001", product_type="savings", currency="THB", opened_date=date(2020, 1, 1), status="active")]
    )
    dim_date = build_dim_date(spark, "2026-04-01", "2026-04-10")

    summary = monthly_summary_report(fact, dim_branch, dim_account, dim_date).collect()
    by_type = {r.txn_type: r for r in summary}
    assert by_type["deposit"].txn_count == 2
    assert by_type["deposit"].total_amount == 300.0
    assert by_type["payment"].txn_count == 1
