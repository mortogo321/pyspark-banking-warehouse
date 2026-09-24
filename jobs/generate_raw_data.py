"""Seed job: generate deterministic synthetic banking data into the raw zone.

Uses stdlib `random` with a fixed seed (no Faker) so every run produces the
exact same dataset. Ensures the LocalStack S3 bucket exists (boto3) before
Spark writes into it.

Deliberately injects a small number of dirty rows into the transactions feed
(nulls, duplicate txn_ids, negative amounts) so raw_to_staging has something
to reject, and a handful of large cash transactions so the CTR report is
non-empty. A second customer snapshot is generated with ~5% of customers
changed so staging_to_warehouse has an SCD2 change to apply.
"""

from __future__ import annotations

import random
import sys
from datetime import date, timedelta
from pathlib import Path

import boto3
from botocore.exceptions import ClientError

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline import config, schemas
from pipeline.session import get_spark_session

FIRST_NAMES = [
    "Somchai", "Somsri", "Anong", "Wichai", "Suda", "Prasert", "Malee", "Niran",
    "Kanya", "Chaiwat", "Siriporn", "Thanawat", "Nattaya", "Boonmee", "Pranee",
    "Sakda", "Ratana", "Wirat", "Ampha", "Decha", "Supaporn", "Kittipong",
    "Orathai", "Somkiat", "Yupa", "Anucha", "Ladda", "Pichai", "Sunee", "Thongchai",
]
LAST_NAMES = [
    "Srisawat", "Chaiyaporn", "Boonmee", "Kittikachorn", "Rattanakul", "Panyarat",
    "Suksawat", "Thongdee", "Wongsawat", "Amnuay", "Charoensuk", "Phromma",
    "Saetang", "Wattana", "Intharat", "Kanjanapas", "Loetkanjanamongkol",
]
PROVINCES = [
    "Bangkok", "Chiang Mai", "Khon Kaen", "Phuket", "Nakhon Ratchasima",
    "Songkhla", "Chiang Rai", "Rayong", "Udon Thani", "Surat Thani",
    "Nonthaburi", "Ayutthaya",
]
BRANCH_PROVINCES = ["Bangkok", "Chiang Mai", "Khon Kaen", "Phuket", "Nakhon Ratchasima", "Songkhla", "Chiang Rai", "Rayong"]
REGION_BY_PROVINCE = {
    "Bangkok": "Central", "Nonthaburi": "Central", "Ayutthaya": "Central", "Rayong": "Eastern",
    "Chiang Mai": "Northern", "Chiang Rai": "Northern",
    "Khon Kaen": "Northeastern", "Nakhon Ratchasima": "Northeastern", "Udon Thani": "Northeastern",
    "Phuket": "Southern", "Songkhla": "Southern", "Surat Thani": "Southern",
}
SEGMENTS = ["retail", "sme", "corporate"]
SEGMENT_WEIGHTS = [0.80, 0.15, 0.05]
KYC_LEVELS = ["basic", "standard", "enhanced"]
KYC_WEIGHTS = [0.5, 0.35, 0.15]
PRODUCT_TYPES = ["savings", "current", "fixed_deposit"]
PRODUCT_WEIGHTS = [0.6, 0.3, 0.1]
ACCOUNT_STATUSES = ["active", "dormant", "closed"]
ACCOUNT_STATUS_WEIGHTS = [0.85, 0.1, 0.05]
TXN_TYPES = ["deposit", "withdrawal", "transfer_in", "transfer_out", "payment"]
CASH_CHANNELS = ["branch", "atm"]
NONCASH_CHANNELS = ["branch", "mobile", "internet"]


def _weighted_choice(rng: random.Random, options: list[str], weights: list[float]) -> str:
    return rng.choices(options, weights=weights, k=1)[0]


def ensure_bucket(bucket: str) -> None:
    """Create the LocalStack S3 bucket if it doesn't already exist."""
    client = boto3.client(
        "s3",
        endpoint_url=config.S3_ENDPOINT,
        aws_access_key_id=config.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=config.AWS_SECRET_ACCESS_KEY,
        region_name=config.AWS_REGION,
    )
    try:
        client.head_bucket(Bucket=bucket)
        print(f"[seed] bucket '{bucket}' already exists")
    except ClientError:
        client.create_bucket(Bucket=bucket)
        print(f"[seed] created bucket '{bucket}'")


def generate_branches(rng: random.Random) -> list[dict]:
    branches = []
    for i, province in enumerate(BRANCH_PROVINCES, start=1):
        branches.append(
            {
                "branch_code": f"BR{i:03d}",
                "branch_name": f"{province} Branch",
                "province": province,
                "region": REGION_BY_PROVINCE[province],
            }
        )
    return branches


def generate_customers(rng: random.Random, n: int, snapshot_date: date) -> list[dict]:
    customers = []
    for i in range(1, n + 1):
        customers.append(
            {
                "customer_id": f"CUST{i:06d}",
                "name": f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}",
                "segment": _weighted_choice(rng, SEGMENTS, SEGMENT_WEIGHTS),
                "province": rng.choice(PROVINCES),
                "kyc_level": _weighted_choice(rng, KYC_LEVELS, KYC_WEIGHTS),
                "snapshot_date": snapshot_date.isoformat(),
            }
        )
    return customers


def apply_customer_drift(rng: random.Random, snapshot1: list[dict], snapshot_date: date, drift_rate: float = 0.05) -> list[dict]:
    """Copy snapshot1 forward, changing province and/or segment for a random
    subset of customers so the SCD2 merge has real changes to detect."""
    snapshot2 = []
    for row in snapshot1:
        new_row = dict(row, snapshot_date=snapshot_date.isoformat())
        if rng.random() < drift_rate:
            new_row["province"] = rng.choice([p for p in PROVINCES if p != row["province"]])
            if rng.random() < 0.5:
                new_row["segment"] = rng.choice([s for s in SEGMENTS if s != row["segment"]])
        snapshot2.append(new_row)
    return snapshot2


def generate_accounts(rng: random.Random, customer_ids: list[str], n: int, window_start: date) -> list[dict]:
    accounts = []
    for i in range(1, n + 1):
        customer_id = rng.choice(customer_ids)
        opened = window_start - timedelta(days=rng.randint(30, 365 * 5))
        accounts.append(
            {
                "account_id": f"ACC{i:07d}",
                "customer_id": customer_id,
                "product_type": _weighted_choice(rng, PRODUCT_TYPES, PRODUCT_WEIGHTS),
                "currency": "THB",
                "opened_date": opened.isoformat(),
                "status": _weighted_choice(rng, ACCOUNT_STATUSES, ACCOUNT_STATUS_WEIGHTS),
            }
        )
    return accounts


def generate_transactions(
    rng: random.Random,
    accounts: list[dict],
    branch_codes: list[str],
    n: int,
    window_start: date,
    window_days: int,
) -> list[dict]:
    transactions = []
    for i in range(1, n + 1):
        account = rng.choice(accounts)
        txn_type = rng.choice(TXN_TYPES)
        is_cash = (txn_type in ("deposit", "withdrawal") and rng.random() < 0.9) or (
            txn_type not in ("deposit", "withdrawal") and rng.random() < 0.05
        )
        channel = rng.choice(CASH_CHANNELS if is_cash else NONCASH_CHANNELS)
        txn_date = window_start + timedelta(days=rng.randint(0, window_days - 1))

        if is_cash and rng.random() < 0.01:
            amount = round(rng.uniform(2_000_000, 5_000_000), 2)
        else:
            amount = round(rng.uniform(100, 50_000), 2)

        fee = round(rng.uniform(0, 50), 2) if txn_type in ("transfer_out", "payment") else 0.0

        transactions.append(
            {
                "txn_id": f"TXN{i:08d}",
                "txn_date": txn_date.isoformat(),
                "account_id": account["account_id"],
                "customer_id": account["customer_id"],
                "branch_code": rng.choice(branch_codes),
                "channel": channel,
                "txn_type": txn_type,
                "is_cash": is_cash,
                "amount": amount,
                "fee": fee,
            }
        )
    return transactions


def inject_dirty_rows(rng: random.Random, clean_txns: list[dict], count_per_kind: int = 60) -> list[dict]:
    """Add duplicate / null / negative-amount rows on top of a clean feed."""
    dirty: list[dict] = []

    for row in rng.sample(clean_txns, min(count_per_kind, len(clean_txns))):
        dirty.append(dict(row))  # exact duplicate txn_id

    next_id = len(clean_txns) + 1
    for _ in range(count_per_kind):
        field_to_null = rng.choice(["account_id", "customer_id", "branch_code", "amount"])
        row = dict(rng.choice(clean_txns))
        row["txn_id"] = f"TXN{next_id:08d}"
        row[field_to_null] = None
        dirty.append(row)
        next_id += 1

    for _ in range(count_per_kind):
        row = dict(rng.choice(clean_txns))
        row["txn_id"] = f"TXN{next_id:08d}"
        row["amount"] = -abs(row["amount"] if row["amount"] else 100.0)
        dirty.append(row)
        next_id += 1

    return dirty


def main() -> None:
    rng = random.Random(config.RANDOM_SEED)
    ensure_bucket(config.S3_BUCKET)

    end_date = date.fromisoformat(config.DATA_END_DATE)
    window_start = end_date - timedelta(days=config.DATA_WINDOW_DAYS - 1)
    snapshot2_date = window_start + timedelta(days=45)

    branches = generate_branches(rng)
    customers_snapshot1 = generate_customers(rng, config.NUM_CUSTOMERS, window_start)
    customers_snapshot2 = apply_customer_drift(rng, customers_snapshot1, snapshot2_date)
    accounts = generate_accounts(rng, [c["customer_id"] for c in customers_snapshot1], config.NUM_ACCOUNTS, window_start)
    clean_transactions = generate_transactions(
        rng, accounts, [b["branch_code"] for b in branches], config.NUM_TRANSACTIONS, window_start, config.DATA_WINDOW_DAYS
    )
    dirty_transactions = inject_dirty_rows(rng, clean_transactions)
    all_transactions = clean_transactions + dirty_transactions
    rng.shuffle(all_transactions)

    changed = sum(1 for a, b in zip(customers_snapshot1, customers_snapshot2, strict=True) if a["province"] != b["province"] or a["segment"] != b["segment"])
    print(f"[seed] window: {window_start.isoformat()} .. {end_date.isoformat()} ({config.DATA_WINDOW_DAYS} days)")
    print(f"[seed] snapshot1={window_start.isoformat()} snapshot2={snapshot2_date.isoformat()} customers_changed={changed}")
    print(f"[seed] branches={len(branches)} customers={len(customers_snapshot1)} accounts={len(accounts)}")
    print(f"[seed] transactions clean={len(clean_transactions)} dirty_injected={len(dirty_transactions)} total={len(all_transactions)}")

    spark = get_spark_session("generate_raw_data")

    spark.createDataFrame(branches, schema=schemas.RAW_BRANCHES_SCHEMA).write.mode("overwrite").option(
        "header", "true"
    ).csv(f"{config.RAW_PATH}/branches")

    spark.createDataFrame(customers_snapshot1, schema=schemas.RAW_CUSTOMERS_SCHEMA).write.mode(
        "overwrite"
    ).option("header", "true").csv(f"{config.RAW_PATH}/customers_snapshot1")

    spark.createDataFrame(customers_snapshot2, schema=schemas.RAW_CUSTOMERS_SCHEMA).write.mode(
        "overwrite"
    ).option("header", "true").csv(f"{config.RAW_PATH}/customers_snapshot2")

    spark.createDataFrame(accounts, schema=schemas.RAW_ACCOUNTS_SCHEMA).write.mode("overwrite").option(
        "header", "true"
    ).csv(f"{config.RAW_PATH}/accounts")

    spark.createDataFrame(all_transactions, schema=schemas.RAW_TRANSACTIONS_SCHEMA).write.mode(
        "overwrite"
    ).option("header", "true").csv(f"{config.RAW_PATH}/transactions")

    print("[seed] raw zone written to", config.RAW_PATH)
    spark.stop()


if __name__ == "__main__":
    main()
