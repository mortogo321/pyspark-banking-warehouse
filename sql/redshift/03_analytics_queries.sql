-- Reference only: example star-join analytics queries against the schema
-- from 01_create_tables.sql. Portable to Postgres (the stand-in warehouse
-- actually running in this repo) with no changes.

-- 1. Monthly cash vs. non-cash transaction volume by branch.
SELECT
    d.year,
    d.month,
    b.branch_name,
    f.is_cash,
    COUNT(*)          AS txn_count,
    SUM(f.amount)      AS total_amount
FROM fact_transactions f
JOIN dim_date d   ON f.date_key = d.date_key
JOIN dim_branch b ON f.branch_key = b.branch_key
GROUP BY d.year, d.month, b.branch_name, f.is_cash
ORDER BY d.year, d.month, b.branch_name, f.is_cash;

-- 2. Top 10 customers by total transaction amount, using only the customer's
--    *current* attributes (is_current = true row in the SCD2 dimension).
SELECT
    c.customer_id,
    c.name,
    c.segment,
    c.province,
    COUNT(*)      AS txn_count,
    SUM(f.amount) AS total_amount
FROM fact_transactions f
JOIN dim_customer c ON f.customer_key = c.customer_key
WHERE c.is_current = true
GROUP BY c.customer_id, c.name, c.segment, c.province
ORDER BY total_amount DESC
LIMIT 10;

-- 3. Product mix by branch region: transaction count and average amount per
--    product_type, split by the branch's region.
SELECT
    b.region,
    a.product_type,
    COUNT(*)          AS txn_count,
    AVG(f.amount)      AS avg_amount
FROM fact_transactions f
JOIN dim_branch b  ON f.branch_key = b.branch_key
JOIN dim_account a ON f.account_key = a.account_key
GROUP BY b.region, a.product_type
ORDER BY b.region, a.product_type;

-- 4. Weekday vs. weekend cash activity, a quick sanity check on the kind of
--    seasonality a CTR-style monitoring job would look for.
SELECT
    d.is_weekend,
    COUNT(*)          AS cash_txn_count,
    SUM(f.amount)      AS cash_total_amount
FROM fact_transactions f
JOIN dim_date d ON f.date_key = d.date_key
WHERE f.is_cash = true
GROUP BY d.is_weekend
ORDER BY d.is_weekend;
