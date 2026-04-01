USE DATABASE ETL_DB;
USE SCHEMA   ANALYTICS;
USE WAREHOUSE ETL_WH;

-- ════════════════════════════════════════════════════
--  GOLD: Master fact table
--  Joins cleaned sales + customer data
--  This is the main table Power BI will connect to
-- ════════════════════════════════════════════════════;

CREATE OR REPLACE TABLE ANALYTICS.FACT_SALES AS
SELECT
    -- ── Transaction ───────────────────────────────
    s.transaction_id,
    s.transaction_date,
    s.txn_year,
    s.txn_month,
    s.txn_month_name,
    s.txn_quarter,

    -- ── Product ───────────────────────────────────
    s.product_id,
    s.product_name,
    s.category,

    -- ── Geography ─────────────────────────────────
    s.region,
    c.city,
    c.state,

    -- ── Customer ──────────────────────────────────
    s.customer_id,
    c.full_name                                  AS customer_name,
    c.segment                                    AS customer_segment,
    c.age_group,
    c.gender,
    c.loyalty_tier,
    c.tenure_band,

    -- ── Financials ────────────────────────────────
    s.quantity,
    s.unit_price,
    s.discount,
    s.discount_band,
    s.net_revenue,
    s.total_revenue,

    -- ── Flags ─────────────────────────────────────
    s.status,
    s.is_returned,
    c.is_active                                  AS is_active_customer,

    -- ── Audit ─────────────────────────────────────
    CURRENT_TIMESTAMP()                          AS created_at

FROM STAGING.STG_SALES         s
LEFT JOIN STAGING.STG_CUSTOMERS c
    ON s.customer_id = c.customer_id;

-- ── Verify join ───────────────────────────────────;
SELECT
    COUNT(*)                                     AS total_rows,
    COUNT(DISTINCT customer_id)                  AS unique_customers,
    COUNT(DISTINCT product_name)                 AS unique_products,
    COUNT(CASE WHEN customer_name IS NULL
               THEN 1 END)                       AS unmatched_customers,
    ROUND(SUM(net_revenue), 2)                   AS total_revenue,
    ROUND(AVG(net_revenue), 2)                   AS avg_order_value
FROM ANALYTICS.FACT_SALES;