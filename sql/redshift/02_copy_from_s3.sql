-- Reference only: example COPY commands for loading the same Parquet files
-- that staging_to_warehouse.py writes to s3a://lake/warehouse/... into the
-- tables from 01_create_tables.sql. Not executed anywhere in this repo.
--
-- In this Docker stack, staging_to_warehouse.py loads Postgres directly over
-- JDBC because there is no Redshift to COPY into locally. On real AWS, the
-- same warehouse-building PySpark script would run unchanged (as a Glue
-- job) up to the Parquet write; only this load step would change from a
-- JDBC write to the COPY statements below, driven off the same S3 Parquet
-- output.

COPY dim_date
FROM 's3://REPLACE_WITH_BUCKET/warehouse/dim_date/'
IAM_ROLE 'arn:aws:iam::REPLACE_WITH_ACCOUNT_ID:role/REPLACE_WITH_REDSHIFT_ROLE'
FORMAT AS PARQUET;

COPY dim_branch
FROM 's3://REPLACE_WITH_BUCKET/warehouse/dim_branch/'
IAM_ROLE 'arn:aws:iam::REPLACE_WITH_ACCOUNT_ID:role/REPLACE_WITH_REDSHIFT_ROLE'
FORMAT AS PARQUET;

COPY dim_account
FROM 's3://REPLACE_WITH_BUCKET/warehouse/dim_account/'
IAM_ROLE 'arn:aws:iam::REPLACE_WITH_ACCOUNT_ID:role/REPLACE_WITH_REDSHIFT_ROLE'
FORMAT AS PARQUET;

COPY dim_customer
FROM 's3://REPLACE_WITH_BUCKET/warehouse/dim_customer/'
IAM_ROLE 'arn:aws:iam::REPLACE_WITH_ACCOUNT_ID:role/REPLACE_WITH_REDSHIFT_ROLE'
FORMAT AS PARQUET;

COPY fact_transactions
FROM 's3://REPLACE_WITH_BUCKET/warehouse/fact_transactions/'
IAM_ROLE 'arn:aws:iam::REPLACE_WITH_ACCOUNT_ID:role/REPLACE_WITH_REDSHIFT_ROLE'
FORMAT AS PARQUET;
