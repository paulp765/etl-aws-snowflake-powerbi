USE DATABASE ETL_DB;
USE SCHEMA   ANALYTICS;
USE WAREHOUSE ETL_WH;

-- ════════════════════════════════════════════════════
--  1. Monthly revenue summary
-- ════════════════════════════════════════════════════;
CREATE OR REPLACE TABLE ANALYTICS.MONTHLY_REVENUE AS
SELECT
    txn_year,
    txn_month,
    txn_month_name,
    txn_quarter,
    COUNT(*)                          AS total_transactions,
    COUNT(DISTINCT customer_id)       AS unique_customers,
    SUM(quantity)                     AS total_units_sold,
    ROUND(SUM(net_revenue), 2)        AS total_revenue,
    ROUND(AVG(net_revenue), 2)        AS avg_order_value,
    COUNT(CASE WHEN is_returned
          THEN 1 END)                 AS total_returns,
    ROUND(
        COUNT(CASE WHEN is_returned THEN 1 END) * 100.0
        / COUNT(*), 2)               AS return_rate_pct
FROM ANALYTICS.FACT_SALES
GROUP BY 1, 2, 3, 4
ORDER BY 1, 2;

-- ════════════════════════════════════════════════════
--  2. Product performance summary
-- ════════════════════════════════════════════════════;
CREATE OR REPLACE TABLE ANALYTICS.PRODUCT_PERFORMANCE AS
SELECT
    product_id,
    product_name,
    category,
    COUNT(*)                          AS total_orders,
    SUM(quantity)                     AS total_units_sold,
    ROUND(SUM(net_revenue), 2)        AS total_revenue,
    ROUND(AVG(net_revenue), 2)        AS avg_order_value,
    ROUND(AVG(discount) * 100, 2)     AS avg_discount_pct,
    COUNT(CASE WHEN is_returned
          THEN 1 END)                 AS total_returns,
    ROUND(
        COUNT(CASE WHEN is_returned THEN 1 END) * 100.0
        / COUNT(*), 2)               AS return_rate_pct
FROM ANALYTICS.FACT_SALES
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC;

-- ════════════════════════════════════════════════════
--  3. Customer segment revenue summary
-- ════════════════════════════════════════════════════;
CREATE OR REPLACE TABLE ANALYTICS.CUSTOMER_SEGMENT_SUMMARY AS
SELECT
    customer_segment,
    loyalty_tier,
    tenure_band,
    COUNT(DISTINCT customer_id)       AS total_customers,
    COUNT(*)                          AS total_orders,
    ROUND(SUM(net_revenue), 2)        AS total_revenue,
    ROUND(AVG(net_revenue), 2)        AS avg_order_value,
    ROUND(SUM(net_revenue)
        / COUNT(DISTINCT customer_id),
        2)                            AS revenue_per_customer
FROM ANALYTICS.FACT_SALES
GROUP BY 1, 2, 3
ORDER BY total_revenue DESC;

-- ════════════════════════════════════════════════════
--  4. Regional performance summary
-- ════════════════════════════════════════════════════;
CREATE OR REPLACE TABLE ANALYTICS.REGIONAL_PERFORMANCE AS
SELECT
    region,
    state,
    COUNT(*)                          AS total_orders,
    COUNT(DISTINCT customer_id)       AS unique_customers,
    ROUND(SUM(net_revenue), 2)        AS total_revenue,
    ROUND(AVG(net_revenue), 2)        AS avg_order_value,
    SUM(quantity)                     AS total_units_sold
FROM ANALYTICS.FACT_SALES
GROUP BY 1, 2
ORDER BY total_revenue DESC;

-- ════════════════════════════════════════════════════
--  5. Quick business insight queries
-- ════════════════════════════════════════════════════;

-- Top 3 products by revenue;
SELECT product_name, total_revenue, total_units_sold
FROM   ANALYTICS.PRODUCT_PERFORMANCE
LIMIT  3;

-- Best performing region;
SELECT region, total_revenue, total_orders
FROM   ANALYTICS.REGIONAL_PERFORMANCE
ORDER  BY total_revenue DESC
LIMIT  1;

-- Revenue by customer segment;
SELECT customer_segment, total_revenue, avg_order_value
FROM   ANALYTICS.CUSTOMER_SEGMENT_SUMMARY
ORDER  BY total_revenue DESC;