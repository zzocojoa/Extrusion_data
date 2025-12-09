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

# Check 1분 동안의 데이터
query = """
SELECT device_id, timestamp 
FROM all_metrics 
WHERE timestamp BETWEEN '2025-01-23 15:01:00' AND '2025-01-23 15:02:00'
ORDER BY timestamp 
LIMIT 30
"""

df = pd.read_sql(query, conn)
print("=== 1분간 샘플 데이터 ===")
print(df.to_string())

print("\n=== 디바이스별 카운트 ===")
print(df['device_id'].value_counts())

conn.close()
