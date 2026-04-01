# etl-aws-snowflake-powerbi

**Tagline:** End-to-end ETL pipeline from local files → AWS S3 → Snowflake → Power BI

---

## 1. Overview

`etl-aws-snowflake-powerbi` is a fully automated ETL pipeline designed for modern analytics workflows. It supports raw ingestion, silver-level cleaning, gold-level transformation, aggregation generation, and export for Power BI reporting, with clear separation of staging and analytics layers.

---

## 2. Architecture / Workflow

1. **Source ingestion**: local CSV/JSON raw files stored in `data/raw/`.
2. **Upload to S3**: scripts upload raw files to AWS S3 using `boto3`.
3. **Staging (Silver)**: Snowflake SQL (`06_silver_sales.sql`, `07_silver_customers.sql`) cleans and de-duplicates into `ETL_DB.STAGING`.
4. **Transformation (Gold)**: Snowflake SQL (`08_gold_fact_sales.sql`, `09_gold_aggregations.sql`) builds `ETL_DB.ANALYTICS` tables.
5. **Export**: `export_for_powerbi.py` reads Snowflake tables and writes CSV outputs for Power BI.
6. **Consumption**: Power BI dashboards load the CSV files or connect directly to Snowflake.

---

## 3. Tech Stack

- `Python`: pipeline orchestration and scripting.
- `pandas`: data frame operations and CSV output.
- `boto3`: AWS S3 upload/download.
- `snowflake-connector-python`: Snowflake connectivity and query execution.
- `python-dotenv`: environment configuration management.
- `Jupyter`: exploration and prototyping notebooks.
- `SQL`: Snowflake transformations and table definitions.
- `Power BI`: visualization and reporting.

---

## 4. Features

- ✅ Automated end-to-end ETL pipeline
- ✅ Silver + Gold layer architecture in Snowflake
- ✅ Idempotent `CREATE OR REPLACE` SQL statements
- ✅ Prebuilt aggregation reports (monthly revenue, product performance, etc.)
- ✅ Export to BI-friendly CSV for Power BI consumption
- ✅ Environment-driven configuration via `.env`

---

## 5. Folder Structure

```
etl-aws-snowflake-powerbi/
├── data/
│   ├── raw/
│   └── processed/
├── scripts/
│   ├── upload_to_s3.py
│   ├── download_from_s3.py
│   ├── run_transformations.py
│   └── export_for_powerbi.py
├── notebooks/
├── sql/
│   ├── 06_silver_sales.sql
│   ├── 07_silver_customers.sql
│   ├── 08_gold_fact_sales.sql
│   └── 09_gold_aggregations.sql
├── dashboard/
│   └── data/
├── models/
├── README.md
├── .gitignore
└── .env (ignored)
```

---

## 6. Setup Instructions

1. Clone the repo:
   ```bash
git clone <repo-url>
cd etl-aws-snowflake-powerbi
```
2. Create and activate a virtual environment:
   ```bash
python -m venv .venv
.venv\\Scripts\\activate      # Windows
source .venv/bin/activate      # macOS/Linux
```
3. Install dependencies:
   ```bash
pip install -r requirements.txt
```
4. Create `.env` file with credentials.

---

## 7. Environment Variables

In `.env`:

- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `AWS_REGION`
- `S3_BUCKET_NAME`
- `SNOWFLAKE_ACCOUNT` (e.g., `GGXTVCV-FP46453`)
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`

> Keep `.env` in `.gitignore` to avoid committing secrets.

---

## 8. Usage Instructions

1. Upload raw files to S3:
   ```bash
python scripts/upload_to_s3.py
```
2. Run Snowflake transformation pipeline:
   ```bash
python scripts/run_transformations.py
```
3. Export tables for Power BI:
   ```bash
python scripts/export_for_powerbi.py
```

---

## 9. Sample Output / Screenshots

- `dashboard/data/fact_sales.csv`
- `dashboard/data/monthly_revenue.csv`
- `dashboard/data/product_performance.csv`
- `dashboard/data/customer_segments.csv`
- `dashboard/data/regional_performance.csv`

_Add screenshot placeholders:_
- `assets/screenshots/powerbi_dashboard.png`
- `assets/screenshots/etl_pipeline_flow.png`

---

## 10. Best Practices

- ETL separation (raw → silver → gold)
- Idempotent table creation
- Data validation and summary checks
- Pushdown compute into Snowflake
- Environment variable secrets management

---

## 11. Challenges & Learnings

- Snowflake account format and connectivity nuance
- File encoding (`UTF-8`) for SQL ingestion
- Pandas column name normalization (uppercase/lowercase)
- Robust error handling in pipeline scripts

---

## 12. Future Enhancements

- Add CI/CD (GitHub Actions) for pipeline automation
- Implement incremental loads with Snowflake streams
- Add comprehensive unit/integration tests
- Add monitoring and alerting (failed loads, schema drift)

---

## 13. Author

- **Priyanka** (or your name)
- Data Engineer | Analytics Engineer
- LinkedIn: `https://www.linkedin.com/in/<your-profile>`

---

## 14. License

- MIT License (or choose your preferred license)

