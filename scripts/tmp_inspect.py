import snowflake.connector, os

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    warehouse='ETL_WH',
    database='ETL_DB',
    schema='ANALYTICS'
)
cur = conn.cursor()
cur.execute('SHOW COLUMNS IN TABLE ETL_DB.ANALYTICS.FACT_SALES')
all_col = cur.fetchall()
for r in all_col:
    print(r)
cur.close()
conn.close()
