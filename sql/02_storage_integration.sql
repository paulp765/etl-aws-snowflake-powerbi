-- Run this first to get the values you need for AWS IAM
USE DATABASE ETL_DB;
USE SCHEMA   RAW;
USE WAREHOUSE ETL_WH;

-- Create a storage integration (secure connection to S3)
CREATE STORAGE INTEGRATION IF NOT EXISTS s3_etl_integration
  TYPE                      = EXTERNAL_STAGE
  STORAGE_PROVIDER          = 'S3'
  ENABLED                   = TRUE
  STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/snowflake-etl-role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://etl-project-priyanka-2024/');

-- Get the values you need to set up the AWS IAM role trust policy
DESC INTEGRATION s3_etl_integration;
-- Note down: STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID