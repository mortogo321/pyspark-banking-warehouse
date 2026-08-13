"""SparkSession factory shared by every job.

Jobs that talk to S3 need the LocalStack endpoint override, path-style access,
and credentials from the environment. Tests use a plain local session with no
cloud configuration at all (see tests/conftest.py), so the S3 wiring lives
only in this one function rather than being sprinkled across jobs.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from pipeline import config


def get_spark_session(app_name: str) -> SparkSession:
    """Build a SparkSession configured to talk to LocalStack S3 and Postgres."""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", config.S3_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", config.AWS_ACCESS_KEY_ID)
        .config("spark.hadoop.fs.s3a.secret.key", config.AWS_SECRET_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        )
        .config("spark.sql.session.timeZone", "UTC")
    )
    return builder.getOrCreate()
