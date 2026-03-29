USE DATABASE ETL_DB;
USE SCHEMA   STAGING;
USE WAREHOUSE ETL_WH;

-- ════════════════════════════════════════════════════
--  SILVER: Clean customer profiles
--  What we fix:
--    1. Trim whitespace from all text columns
--    2. Standardize segment and gender
--    3. Add age group buckets
--    4. Calculate customer tenure in days
--    5. Flag high-value customers
-- ════════════════════════════════════════════════════

CREATE OR REPLACE TABLE STAGING.STG_CUSTOMERS AS
SELECT
    -- ── Identity ──────────────────────────────────
    customer_id,
    TRIM(first_name)                             AS first_name,
    TRIM(last_name)                              AS last_name,
    INITCAP(TRIM(first_name || ' ' || last_name))AS full_name,
    LOWER(TRIM(email))                           AS email,   -- normalize email
    phone,

    -- ── Location ──────────────────────────────────
    INITCAP(TRIM(city))                          AS city,
    INITCAP(TRIM(state))                         AS state,

    -- ── Demographics ──────────────────────────────
    age,
    CASE
        WHEN age BETWEEN 22 AND 30 THEN '22-30'
        WHEN age BETWEEN 31 AND 40 THEN '31-40'
        WHEN age BETWEEN 41 AND 50 THEN '41-50'
        ELSE                            '51+'
    END                                          AS age_group,
    UPPER(TRIM(gender))                          AS gender,

    -- ── Segment ───────────────────────────────────
    INITCAP(TRIM(segment))                       AS segment,

    -- ── Dates & tenure ────────────────────────────
    join_date,
    DATEDIFF('day', join_date, CURRENT_DATE())   AS tenure_days,
    CASE
        WHEN DATEDIFF('day', join_date, CURRENT_DATE()) < 180 THEN 'New'
        WHEN DATEDIFF('day', join_date, CURRENT_DATE()) < 540 THEN 'Established'
        ELSE                                                        'Loyal'
    END                                          AS tenure_band,

    -- ── Loyalty ───────────────────────────────────
    loyalty_points,
    CASE
        WHEN loyalty_points >= 3000 THEN 'Gold'
        WHEN loyalty_points >= 1000 THEN 'Silver'
        ELSE                             'Bronze'
    END                                          AS loyalty_tier,

    -- ── Status ────────────────────────────────────
    is_active,

    -- ── Audit ─────────────────────────────────────
    loaded_at,
    CURRENT_TIMESTAMP()                          AS transformed_at

FROM ETL_DB.RAW.RAW_CUSTOMERS;

-- ── Quick check ───────────────────────────────────────────────
SELECT
    COUNT(*)                                       AS total_customers,
    COUNT(CASE WHEN is_active THEN 1 END)          AS active_customers,
    COUNT(CASE WHEN loyalty_tier = 'Gold' THEN 1 END) AS gold_members,
    AVG(tenure_days)                               AS avg_tenure_days,
    AVG(age)                                       AS avg_age
FROM STAGING.STG_CUSTOMERS;