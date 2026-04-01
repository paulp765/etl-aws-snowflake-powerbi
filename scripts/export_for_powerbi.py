import snowflake.connector
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return snowflake.connector.connect(
        account   = os.getenv("SNOWFLAKE_ACCOUNT"),
        user      = os.getenv("SNOWFLAKE_USER"),
        password  = os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse = "ETL_WH",
        database  = "ETL_DB",
        schema    = "ANALYTICS",
    )

def export_table(conn, table_name, output_path):
    print(f"⬇  Exporting {table_name}...")
    df = pd.read_sql(f"SELECT * FROM {table_name}", conn)
    # normalize to lower case column names for consistent pandas access
    df.columns = [c.lower() for c in df.columns]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"   ✅ {len(df)} rows → {output_path}")
    return df

if __name__ == "__main__":
    print("=" * 50)
    print("  Exporting Snowflake tables for Power BI")
    print("=" * 50)

    conn = get_conn()

    tables = {
        "FACT_SALES":               "dashboard/data/fact_sales.csv",
        "MONTHLY_REVENUE":          "dashboard/data/monthly_revenue.csv",
        "PRODUCT_PERFORMANCE":      "dashboard/data/product_performance.csv",
        "CUSTOMER_SEGMENT_SUMMARY": "dashboard/data/customer_segments.csv",
        "REGIONAL_PERFORMANCE":     "dashboard/data/regional_performance.csv",
    }

    dfs = {}
    for table, path in tables.items():
        dfs[table] = export_table(conn, table, path)

    conn.close()

    # Defensive column checks for FACT_SALES
    fs = dfs.get('FACT_SALES')
    if fs is None:
        raise RuntimeError('FACT_SALES export failed; cannot compute summary')

    if 'net_revenue' not in fs.columns:
        print('⚠️  net_revenue column missing in FACT_SALES, recomputing from unit_price, quantity, discount')
        if all(c in fs.columns for c in ['unit_price', 'quantity', 'discount']):
            fs['net_revenue'] = (fs['unit_price'] * fs['quantity'] * (1 - fs['discount'])).round(2)
        else:
            raise KeyError('FACT_SALES is missing net_revenue and cannot recompute because unit_price/quantity/discount are absent.')

    if 'is_returned' not in fs.columns:
        print('⚠️  is_returned column missing in FACT_SALES, setting all to False')
        fs['is_returned'] = False

    print('\n── Summary ────────────────────────────────────')
    print(f"  Total revenue:       ₹{fs['net_revenue'].sum():,.0f}")
    print(f"  Total transactions:  {len(fs):,}")
    print(f"  Avg order value:     ₹{fs['net_revenue'].mean():,.0f}")
    print(f"  Return rate:         "
          f"{(fs['is_returned'].sum() / len(fs) * 100):.1f}%")
    print('\n🎉 All tables exported to dashboard/data/')