"""Build the two regulatory-style reports from the warehouse and publish them.

Each report is written twice:
  - to s3a://lake/reports/... in Spark's normal (partitioned, multi-file)
    layout, exactly as it would land in a real data lake, and
  - as a single clean CSV file under ./data/reports/ on the host, so a
    reviewer can open the output directly without any S3 tooling.
"""

from __future__ import annotations

import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from pipeline import config
from pipeline.session import get_spark_session
from pipeline.transforms import ctr_report, monthly_summary_report


def export_single_csv(df, local_dir: str, filename: str) -> None:
    """Write `df` as one clean CSV file at local_dir/filename."""
    tmp_dir = f"{local_dir}/_tmp_{filename}"
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(tmp_dir)

    part_files = list(Path(tmp_dir).glob("part-*.csv"))
    if not part_files:
        raise RuntimeError(f"no part file produced for {filename}")

    Path(local_dir).mkdir(parents=True, exist_ok=True)
    shutil.move(str(part_files[0]), f"{local_dir}/{filename}")
    shutil.rmtree(tmp_dir)


def main() -> None:
    spark = get_spark_session("regulatory_reports")

    fact_transactions = spark.read.parquet(f"{config.WAREHOUSE_PATH}/fact_transactions")
    dim_customer = spark.read.parquet(f"{config.WAREHOUSE_PATH}/dim_customer")
    dim_branch = spark.read.parquet(f"{config.WAREHOUSE_PATH}/dim_branch")
    dim_account = spark.read.parquet(f"{config.WAREHOUSE_PATH}/dim_account")
    dim_date = spark.read.parquet(f"{config.WAREHOUSE_PATH}/dim_date")

    ctr = ctr_report(fact_transactions, dim_customer, dim_branch, dim_date, config.CTR_THRESHOLD).cache()
    summary = monthly_summary_report(fact_transactions, dim_branch, dim_account, dim_date)

    ctr_count = ctr.count()
    print(f"[regulatory_reports] CTR threshold={config.CTR_THRESHOLD} hits={ctr_count}")

    ctr.write.mode("overwrite").partitionBy("report_month").csv(
        f"{config.REPORTS_PATH}/ctr", header=True
    )
    summary.write.mode("overwrite").csv(f"{config.REPORTS_PATH}/monthly_summary", header=True)

    months = [row["report_month"] for row in ctr.select("report_month").distinct().collect()]
    for month in sorted(months):
        export_single_csv(ctr.filter(ctr.report_month == month), config.LOCAL_REPORTS_DIR, f"ctr_report_{month}.csv")
        print(f"[regulatory_reports] wrote ctr_report_{month}.csv")

    export_single_csv(summary, config.LOCAL_REPORTS_DIR, "monthly_summary.csv")
    print("[regulatory_reports] wrote monthly_summary.csv")

    spark.stop()


if __name__ == "__main__":
    main()
