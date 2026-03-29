import snowflake.connector
import os, time
from dotenv import load_dotenv

load_dotenv()

# ── Connection ────────────────────────────────────────────────
def get_conn():
    acct = os.getenv("SNOWFLAKE_ACCOUNT")
    user = os.getenv("SNOWFLAKE_USER")
    pwd = os.getenv("SNOWFLAKE_PASSWORD")
    assert acct, "SNOWFLAKE_ACCOUNT missing"
    assert user, "SNOWFLAKE_USER missing"
    assert pwd, "SNOWFLAKE_PASSWORD missing"
    print("Connecting to", acct)
    return snowflake.connector.connect(
        account=acct, user=user, password=pwd,
        warehouse="ETL_WH", database="ETL_DB"
    )

# ── Run a SQL file ────────────────────────────────────────────
def run_sql_file(conn, filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        sql = f.read()

    # Split on semicolons, skip empty statements
    statements = [s.strip() for s in sql.split(";") if s.strip()]
    # print(statements)
    print(f"Executing {len(statements)} statements from {filepath}...")
    cur = conn.cursor()
    for stmt in statements:
        if stmt.upper().startswith("--"):
            continue
        # print(f"   Executing: {stmt}")
        cur.execute(stmt)
    cur.close()

# ── Transformation pipeline ───────────────────────────────────
def run_pipeline():
    print("=" * 55)
    print("  Running SQL Transformation Pipeline")
    print("=" * 55)

    steps = [
        ("Silver: Clean sales",         "sql/06_silver_sales.sql"),
        ("Silver: Clean customers",     "sql/07_silver_customers.sql"),
        ("Gold: Fact sales table",      "sql/08_gold_fact_sales.sql"),
        ("Gold: Aggregation tables",    "sql/09_gold_aggregations.sql"),
    ]

    conn = get_conn()
    for label, filepath in steps:
        start = time.time()
        print(f"\n⚙  Running: {label}")
        try:
            run_sql_file(conn, filepath)
            elapsed = round(time.time() - start, 2)
            print(f"   ✅ Done in {elapsed}s → {filepath}")
        except Exception as e:
            print(f"   ❌ Error in {filepath}: {e}")
            raise
    conn.close()

    print("\n" + "=" * 55)
    print("  ✅ All transformations complete!")
    print("  Tables ready in ANALYTICS schema for Power BI")
    print("=" * 55)

if __name__ == "__main__":
    run_pipeline()