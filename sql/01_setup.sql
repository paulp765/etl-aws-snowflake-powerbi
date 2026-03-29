-- ── Step 1: Create a virtual warehouse (compute engine) ──────
CREATE WAREHOUSE IF NOT EXISTS ETL_WH
  WAREHOUSE_SIZE = 'X-SMALL'   -- smallest & cheapest for learning
  AUTO_SUSPEND   = 60           -- pauses after 60s of inactivity (saves credits)
  AUTO_RESUME    = TRUE
  COMMENT        = 'Warehouse for ETL project';

-- ── Step 2: Create the database ──────────────────────────────
CREATE DATABASE IF NOT EXISTS ETL_DB
  COMMENT = 'ETL portfolio project database';

-- ── Step 3: Create schemas ────────────────────────────────────
USE DATABASE ETL_DB;

CREATE SCHEMA IF NOT EXISTS RAW       COMMENT = 'Raw data loaded from S3';
CREATE SCHEMA IF NOT EXISTS STAGING   COMMENT = 'Cleaned and validated data';
CREATE SCHEMA IF NOT EXISTS ANALYTICS COMMENT = 'Final transformed tables for reporting';

-- ── Verify ────────────────────────────────────────────────────
SHOW DATABASES;
SHOW SCHEMAS IN DATABASE ETL_DB;