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

# Check which sensors have main_pressure data
query = """
SELECT 
    device_id, 
    COUNT(*) as total_rows,
    COUNT(main_pressure) as has_pressure,
    COUNT(CASE WHEN main_pressure IS NOT NULL THEN 1 END) as pressure_not_null,
    MIN(timestamp) as first_ts,
    MAX(timestamp) as last_ts
FROM all_metrics
GROUP BY device_id
"""

df = pd.read_sql(query, conn)
print("=== Device Data Summary ===")
print(df.to_string())

# Check a specific sample
print("\n=== Sample: Temperature Sensor Data ===")
query2 = """
SELECT device_id, timestamp, main_pressure, production_counter
FROM all_metrics
WHERE device_id = 'spot_temperature_sensor'
LIMIT 5
"""
df2 = pd.read_sql(query2, conn)
print(df2.to_string())

print("\n=== Sample: PLC Data ===")
query3 = """
SELECT device_id, timestamp, main_pressure, production_counter
FROM all_metrics
WHERE device_id = 'extruder_plc'
LIMIT 5
"""
df3 = pd.read_sql(query3, conn)
print(df3.to_string())

conn.close()
