USE DATABASE ETL_DB;
USE SCHEMA   STAGING;
USE WAREHOUSE ETL_WH;

-- ════════════════════════════════════════════════════
--  SILVER: Clean sales transactions
--  What we fix:
--    1. Fill NULL regions with 'Unknown'
--    2. Fill NULL discounts with 0
--    3. Standardize status to uppercase
--    4. Add derived columns (net_revenue, discount_band)
--    5. Flag returned transactions
-- ════════════════════════════════════════════════════

CREATE OR REPLACE TABLE STAGING.STG_SALES AS
SELECT
    -- ── Identity columns ─────────────────────────
    transaction_id,
    customer_id,
    product_id,
    product_name,

    -- ── Standardize category & region ────────────
    UPPER(TRIM(category))                        AS category,
    COALESCE(UPPER(TRIM(region)), 'UNKNOWN')     AS region,   -- fix NULLs

    -- ── Numeric columns ───────────────────────────
    quantity,
    unit_price,
    COALESCE(discount, 0)                        AS discount, -- fix NULLs
    total_revenue,

    -- ── Derived: recalculate clean revenue ────────
    ROUND(unit_price * quantity * (1 - COALESCE(discount, 0)), 2)
                                                 AS net_revenue,

    -- ── Derived: discount tier ────────────────────
    CASE
        WHEN COALESCE(discount, 0) = 0          THEN 'No Discount'
        WHEN discount <= 0.10                   THEN 'Low (0-10%)'
        WHEN discount <= 0.20                   THEN 'Medium (10-20%)'
        ELSE                                         'High (20%+)'
    END                                          AS discount_band,

    -- ── Dates ─────────────────────────────────────
    transaction_date,
    YEAR(transaction_date)                       AS txn_year,
    MONTH(transaction_date)                      AS txn_month,
    MONTHNAME(transaction_date)                  AS txn_month_name,
    QUARTER(transaction_date)                    AS txn_quarter,
    DAYOFWEEK(transaction_date)                  AS day_of_week,

    -- ── Status ────────────────────────────────────
    UPPER(TRIM(status))                          AS status,
    CASE WHEN UPPER(status) = 'RETURNED'
         THEN TRUE ELSE FALSE END                AS is_returned,

    -- ── Audit ─────────────────────────────────────
    loaded_at,
    CURRENT_TIMESTAMP()                          AS transformed_at

FROM ETL_DB.RAW.RAW_SALES;

-- ── Quick check ───────────────────────────────────
SELECT
    COUNT(*)                                     AS total_rows,
    COUNT(CASE WHEN region = 'UNKNOWN' THEN 1 END) AS unknown_regions,
    COUNT(CASE WHEN discount = 0 THEN 1 END)     AS zero_discounts,
    MIN(transaction_date)                        AS earliest_txn,
    MAX(transaction_date)                        AS latest_txn,
    SUM(net_revenue)                             AS total_net_revenue
FROM STAGING.STG_SALES;