USE DATABASE ETL_DB;
USE SCHEMA   RAW;

-- ── Sales transactions table ──────────────────────────────────
CREATE OR REPLACE TABLE RAW_SALES (
    transaction_id   VARCHAR(20),
    customer_id      VARCHAR(20),
    product_id       VARCHAR(20),
    product_name     VARCHAR(100),
    category         VARCHAR(50),
    region           VARCHAR(50),
    quantity         NUMBER(10, 0),
    unit_price       NUMBER(12, 2),
    discount         NUMBER(5, 2),
    total_revenue    NUMBER(15, 2),
    transaction_date DATE,
    status           VARCHAR(20),
    loaded_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP()  -- audit column
);

-- ── Customer profiles table ───────────────────────────────────
CREATE OR REPLACE TABLE RAW_CUSTOMERS (
    customer_id     VARCHAR(20),
    first_name      VARCHAR(100),
    last_name       VARCHAR(100),
    email           VARCHAR(200),
    phone           VARCHAR(30),
    city            VARCHAR(100),
    state           VARCHAR(100),
    age             NUMBER(3, 0),
    gender          VARCHAR(20),
    segment         VARCHAR(30),
    join_date       DATE,
    loyalty_points  NUMBER(10, 0),
    is_active       BOOLEAN,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ── Verify tables exist ───────────────────────────────────────
SHOW TABLES IN SCHEMA ETL_DB.RAW;