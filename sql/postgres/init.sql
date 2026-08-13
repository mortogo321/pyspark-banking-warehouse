-- Star-schema warehouse DDL, mounted into postgres's docker-entrypoint-initdb.d
-- so it runs automatically the first time the postgres container starts.
--
-- No foreign keys: staging_to_warehouse.py reloads every table with
-- `TRUNCATE` + append on each run, and Postgres refuses to TRUNCATE a table
-- that is still referenced by rows in a child table. Referential integrity
-- is instead guaranteed by the ETL (surrogate keys are always resolved from
-- the dimensions built in the same run), which is the same trade-off a
-- Redshift COPY-based load makes in practice.

CREATE TABLE dim_date (
    date_key    INTEGER PRIMARY KEY,      -- yyyymmdd
    date        DATE NOT NULL,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    month_name  VARCHAR(20) NOT NULL,
    day         SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend  BOOLEAN NOT NULL
);

CREATE TABLE dim_branch (
    branch_key  INTEGER PRIMARY KEY,
    branch_code VARCHAR(10) NOT NULL,
    branch_name VARCHAR(100) NOT NULL,
    province    VARCHAR(50) NOT NULL,
    region      VARCHAR(50) NOT NULL
);

CREATE TABLE dim_account (
    account_key   INTEGER PRIMARY KEY,
    account_id    VARCHAR(20) NOT NULL,
    customer_id   VARCHAR(20) NOT NULL,
    product_type  VARCHAR(20) NOT NULL,
    currency      VARCHAR(5) NOT NULL,
    opened_date   DATE,
    status        VARCHAR(20) NOT NULL
);

-- SCD Type 2: one natural customer_id can have multiple rows, at most one of
-- which has is_current = true.
CREATE TABLE dim_customer (
    customer_key    INTEGER PRIMARY KEY,
    customer_id     VARCHAR(20) NOT NULL,
    name            VARCHAR(200) NOT NULL,
    segment         VARCHAR(20) NOT NULL,
    province        VARCHAR(50) NOT NULL,
    kyc_level       VARCHAR(20) NOT NULL,
    effective_from  DATE NOT NULL,
    effective_to    DATE,
    is_current      BOOLEAN NOT NULL
);

CREATE TABLE fact_transactions (
    txn_id       VARCHAR(20) PRIMARY KEY,
    date_key     INTEGER NOT NULL,
    customer_key INTEGER,
    account_key  INTEGER,
    branch_key   INTEGER,
    channel      VARCHAR(20) NOT NULL,   -- degenerate dimension, see README
    txn_type     VARCHAR(20) NOT NULL,
    is_cash      BOOLEAN NOT NULL,
    amount       NUMERIC(15, 2) NOT NULL,
    fee          NUMERIC(15, 2) NOT NULL
);

CREATE INDEX idx_dim_customer_customer_id ON dim_customer (customer_id);
CREATE INDEX idx_fact_transactions_date_key ON fact_transactions (date_key);
CREATE INDEX idx_fact_transactions_customer_key ON fact_transactions (customer_key);
CREATE INDEX idx_fact_transactions_branch_key ON fact_transactions (branch_key);
