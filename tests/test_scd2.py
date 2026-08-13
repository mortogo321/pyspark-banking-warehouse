from __future__ import annotations

from datetime import date

from pyspark.sql import Row

from pipeline import schemas
from pipeline.transforms import scd2_merge


def _incoming_row(**overrides):
    base = dict(
        customer_id="CUST000001",
        name="Somchai Srisawat",
        segment="retail",
        province="Bangkok",
        kyc_level="basic",
        snapshot_date=date(2026, 4, 1),
    )
    base.update(overrides)
    return Row(**base)


def test_first_snapshot_opens_new_rows_for_all_customers(spark):
    empty_dim = spark.createDataFrame([], schema=schemas.EMPTY_DIM_CUSTOMER_SCHEMA)
    incoming = spark.createDataFrame(
        [_incoming_row(), _incoming_row(customer_id="CUST000002", name="Suda Boonmee")]
    )

    result = scd2_merge(empty_dim, incoming).collect()
    assert len(result) == 2
    for row in result:
        assert row.is_current is True
        assert row.effective_to is None
        assert row.effective_from == date(2026, 4, 1)
    assert {r.customer_key for r in result} == {1, 2}


def test_unchanged_customer_row_is_left_untouched(spark):
    empty_dim = spark.createDataFrame([], schema=schemas.EMPTY_DIM_CUSTOMER_SCHEMA)
    snapshot1 = spark.createDataFrame([_incoming_row()])
    dim_after_1 = scd2_merge(empty_dim, snapshot1)
    original_row = dim_after_1.collect()[0]

    snapshot2 = spark.createDataFrame([_incoming_row(snapshot_date=date(2026, 5, 17))])
    dim_after_2 = scd2_merge(dim_after_1, snapshot2)
    rows = dim_after_2.collect()

    assert len(rows) == 1
    assert rows[0].customer_key == original_row.customer_key
    assert rows[0].effective_from == original_row.effective_from
    assert rows[0].effective_to is None
    assert rows[0].is_current is True


def test_changed_attribute_closes_old_row_and_opens_new_one(spark):
    empty_dim = spark.createDataFrame([], schema=schemas.EMPTY_DIM_CUSTOMER_SCHEMA)
    snapshot1 = spark.createDataFrame([_incoming_row(province="Bangkok")])
    dim_after_1 = scd2_merge(empty_dim, snapshot1)
    original_key = dim_after_1.collect()[0].customer_key

    snapshot2 = spark.createDataFrame(
        [_incoming_row(province="Chiang Mai", snapshot_date=date(2026, 5, 17))]
    )
    dim_after_2 = scd2_merge(dim_after_1, snapshot2)
    rows = dim_after_2.orderBy("effective_from").collect()

    assert len(rows) == 2

    closed = [r for r in rows if r.is_current is False]
    current = [r for r in rows if r.is_current is True]
    assert len(closed) == 1
    assert len(current) == 1

    assert closed[0].customer_key == original_key
    assert closed[0].province == "Bangkok"
    assert closed[0].effective_to == date(2026, 5, 17)

    assert current[0].customer_key != original_key
    assert current[0].province == "Chiang Mai"
    assert current[0].effective_from == date(2026, 5, 17)
    assert current[0].effective_to is None


def test_new_customer_in_second_snapshot_gets_new_row(spark):
    empty_dim = spark.createDataFrame([], schema=schemas.EMPTY_DIM_CUSTOMER_SCHEMA)
    snapshot1 = spark.createDataFrame([_incoming_row()])
    dim_after_1 = scd2_merge(empty_dim, snapshot1)

    snapshot2 = spark.createDataFrame(
        [
            _incoming_row(snapshot_date=date(2026, 5, 17)),
            _incoming_row(customer_id="CUST000099", name="New Customer", snapshot_date=date(2026, 5, 17)),
        ]
    )
    dim_after_2 = scd2_merge(dim_after_1, snapshot2)
    rows = dim_after_2.collect()

    assert len(rows) == 2
    new_customer = [r for r in rows if r.customer_id == "CUST000099"][0]
    assert new_customer.is_current is True
    assert new_customer.effective_from == date(2026, 5, 17)
    assert len({r.customer_key for r in rows}) == 2
