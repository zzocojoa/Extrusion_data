import psycopg2
import pandas as pd

DB_PARAMS = {
    "host": "127.0.0.1",
    "port": 25432,
    "user": "postgres",
    "password": "postgres",
    "dbname": "postgres"
}

conn = psycopg2.connect(**DB_PARAMS)

print("=" * 80)
print("all_metrics 테이블 컬럼")
print("=" * 80)

query1 = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'all_metrics'
ORDER BY ordinal_position
"""
df1 = pd.read_sql(query1, conn)
print(df1.to_string(index=False))

print("\n" + "=" * 80)
print("tb_work_log 테이블 컬럼")
print("=" * 80)

query2 = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'tb_work_log'
ORDER BY ordinal_position
"""
df2 = pd.read_sql(query2, conn)
print(df2.to_string(index=False))

print("\n" + "=" * 80)
print("tb_cycle_log 테이블 컬럼")
print("=" * 80)

query3 = """
SELECT column_name, data_type, is_nullable
FROM information_schema.columns
WHERE table_name = 'tb_cycle_log'
ORDER BY ordinal_position
"""
df3 = pd.read_sql(query3, conn)
print(df3.to_string(index=False))

conn.close()
