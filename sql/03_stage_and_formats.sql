USE DATABASE ETL_DB;
USE SCHEMA   RAW;
USE WAREHOUSE ETL_WH;

-- ── File format for CSV ───────────────────────────────────────
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE             = 'CSV'
  FIELD_DELIMITER  = ','
  SKIP_HEADER      = 1            -- skip the header row
  NULL_IF          = ('NULL', 'null', 'NA', '')
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE       = TRUE
  COMMENT          = 'Standard CSV format with header';

-- ── File format for JSON ──────────────────────────────────────
CREATE OR REPLACE FILE FORMAT json_format
  TYPE  = 'JSON'
  STRIP_OUTER_ARRAY = TRUE       -- our JSON is an array [ {...}, {...} ]
  COMMENT = 'JSON array format for customer profiles';

-- ── External stage pointing to S3 ────────────────────────────
CREATE OR REPLACE STAGE etl_s3_stage
  STORAGE_INTEGRATION = s3_etl_integration
  URL                 = 's3://etl-project-priyanka-2024/raw/'
  COMMENT             = 'Stage pointing to raw folder in S3';

-- ── Verify stage is connected ─────────────────────────────────
LIST @etl_s3_stage;
-- You should see your CSV and JSON files listed here!