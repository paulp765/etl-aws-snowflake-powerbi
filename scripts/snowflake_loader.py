import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# ── Connect to Snowflake ──────────────────────────────────────
def get_connection():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = "ETL_WH",
        database  = "ETL_DB",
        schema    = "RAW",
    )

# ── Run a query and return results ────────────────────────────
def run_query(sql, fetch=True):
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute(sql)
        if fetch:
            results = cur.fetchall()
            cols    = [desc[0] for desc in cur.description]
            return cols, results
        else:
            return None, f"{cur.rowcount} rows affected"
    finally:
        cur.close()
        conn.close()

# ── Validation checks ─────────────────────────────────────────
def validate_load():
    print("=" * 50)
    print("  Snowflake Load Validation")
    print("=" * 50)

    checks = [
        ("RAW_SALES row count",     "SELECT COUNT(*) FROM ETL_DB.RAW.RAW_SALES"),
        ("RAW_CUSTOMERS row count", "SELECT COUNT(*) FROM ETL_DB.RAW.RAW_CUSTOMERS"),
        ("NULL regions in sales",   "SELECT COUNT(*) FROM ETL_DB.RAW.RAW_SALES WHERE REGION IS NULL"),
        ("Returned transactions",   "SELECT COUNT(*) FROM ETL_DB.RAW.RAW_SALES WHERE STATUS = 'Returned'"),
        ("Premium customers",       "SELECT COUNT(*) FROM ETL_DB.RAW.RAW_CUSTOMERS WHERE SEGMENT = 'Premium'"),
    ]

    for label, sql in checks:
        cols, results = run_query(sql)
        print(f"  ✅ {label}: {results[0][0]}")

    print("\n🎉 Validation complete!")

if __name__ == "__main__":
    validate_load()