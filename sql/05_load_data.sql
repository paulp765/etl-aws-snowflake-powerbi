USE DATABASE ETL_DB;
USE SCHEMA   RAW;
USE WAREHOUSE ETL_WH;

-- ── Load CSV → RAW_SALES ──────────────────────────────────────
COPY INTO RAW_SALES (
    transaction_id, customer_id, product_id, product_name,
    category, region, quantity, unit_price, discount,
    total_revenue, transaction_date, status
)
FROM @etl_s3_stage/sales_transactions.csv
FILE_FORMAT = csv_format
ON_ERROR    = 'CONTINUE';   -- skip bad rows, don't fail entire load

-- ── Load JSON → RAW_CUSTOMERS ────────────────────────────────
COPY INTO RAW_CUSTOMERS (
    customer_id, first_name, last_name, email, phone,
    city, state, age, gender, segment,
    join_date, loyalty_points, is_active
)
FROM (
    SELECT
        $1:customer_id::VARCHAR,
        $1:first_name::VARCHAR,
        $1:last_name::VARCHAR,
        $1:email::VARCHAR,
        $1:phone::VARCHAR,
        $1:city::VARCHAR,
        $1:state::VARCHAR,
        $1:age::NUMBER,
        $1:gender::VARCHAR,
        $1:segment::VARCHAR,
        $1:join_date::DATE,
        $1:loyalty_points::NUMBER,
        $1:is_active::BOOLEAN
    FROM @etl_s3_stage/customer_profiles.json
    (FILE_FORMAT => json_format)
);

-- ── Verify data loaded correctly ─────────────────────────────
SELECT COUNT(*) AS total_sales     FROM RAW_SALES;       -- expect 500
SELECT COUNT(*) AS total_customers FROM RAW_CUSTOMERS;   -- expect 100

-- Quick preview
SELECT * FROM RAW_SALES     LIMIT 5;
SELECT * FROM RAW_CUSTOMERS LIMIT 5;

-- Check for NULLs (from our intentional dirty data)
SELECT
    COUNT(*)                                  AS total_rows,
    COUNT(*) - COUNT(discount)                AS null_discounts,
    COUNT(*) - COUNT(region)                  AS null_regions,
    COUNT(CASE WHEN status = 'Returned'  THEN 1 END) AS returned,
    COUNT(CASE WHEN status = 'Completed' THEN 1 END) AS completed,
    COUNT(CASE WHEN status = 'Pending'   THEN 1 END) AS pending
FROM RAW_SALES;