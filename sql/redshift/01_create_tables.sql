-- Reference only: this is the Redshift-dialect equivalent of sql/postgres/init.sql.
-- It is not executed anywhere in this repo (there is no Redshift in the
-- Docker stack) — it documents how the same star schema would be declared on
-- the real target warehouse. See README "Mapping to real AWS".

CREATE TABLE dim_date (
    date_key    INTEGER PRIMARY KEY,
    date        DATE NOT NULL,
    year        SMALLINT NOT NULL,
    quarter     SMALLINT NOT NULL,
    month       SMALLINT NOT NULL,
    month_name  VARCHAR(20) NOT NULL,
    day         SMALLINT NOT NULL,
    day_of_week SMALLINT NOT NULL,
    is_weekend  BOOLEAN NOT NULL
)
-- Small (one row per day) and read by almost every query that filters or
-- groups by time: a full copy on every node avoids a network shuffle for
-- the dim_date join on every single query that touches it.
DISTSTYLE ALL
SORTKEY (date_key);

CREATE TABLE dim_branch (
    branch_key  INTEGER PRIMARY KEY,
    branch_code VARCHAR(10) NOT NULL,
    branch_name VARCHAR(100) NOT NULL,
    province    VARCHAR(50) NOT NULL,
    region      VARCHAR(50) NOT NULL
)
-- Only 8 rows: DISTSTYLE ALL is essentially free and, like dim_date, removes
-- the shuffle for every fact join on branch_key.
DISTSTYLE ALL;

CREATE TABLE dim_account (
    account_key   INTEGER PRIMARY KEY,
    account_id    VARCHAR(20) NOT NULL,
    customer_id   VARCHAR(20) NOT NULL,
    product_type  VARCHAR(20) NOT NULL,
    currency      VARCHAR(5) NOT NULL,
    opened_date   DATE,
    status        VARCHAR(20) NOT NULL
)
-- Thousands, not millions, of rows: still cheap to broadcast to every node.
DISTSTYLE ALL;

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
)
-- SCD2 history keeps this larger than the other dims (more rows than
-- distinct customers) but it is still small next to the fact table, and
-- customer_key is a very common join/filter key, so DISTSTYLE ALL still pays
-- for itself.
DISTSTYLE ALL;

CREATE TABLE fact_transactions (
    txn_id       VARCHAR(20) PRIMARY KEY,
    date_key     INTEGER NOT NULL,
    customer_key INTEGER,
    account_key  INTEGER,
    branch_key   INTEGER,
    channel      VARCHAR(20) NOT NULL,
    txn_type     VARCHAR(20) NOT NULL,
    is_cash      BOOLEAN NOT NULL,
    amount       NUMERIC(15, 2) NOT NULL,
    fee          NUMERIC(15, 2) NOT NULL
)
-- account_key is the highest-cardinality, most-joined column on the biggest
-- table, so it is the DISTKEY: rows for the same account collocate on one
-- slice, making account-level joins/aggregations local instead of shuffled.
-- SORTKEY(date_key) matches how this fact is queried in practice (almost
-- always filtered to a date or month range for a regulatory period) and lets
-- Redshift skip whole blocks outside the requested range.
DISTKEY (account_key)
SORTKEY (date_key);
