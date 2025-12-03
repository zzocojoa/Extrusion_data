import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np

# DB Config
DB_PARAMS = {
    "host": "127.0.0.1",
    "port": 25432, # Correct port from supabase status
    "user": "postgres",
    "password": "postgres",
    "dbname": "postgres"
}

def import_csv():
    csv_path = "cycle_log_dump.csv"
    print(f"Reading {csv_path}...")
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print("CSV file not found.")
        return

    # Handle NaN for SQL
    df = df.replace({np.nan: None})
    
    print(f"Importing {len(df)} rows...")
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        # 1. Reload Schema Cache (Just in case)
        cur.execute("NOTIFY pgrst, 'reload schema';")
        
        # 2. Insert
        insert_query = """
            INSERT INTO tb_cycle_log 
            (machine_id, start_time, end_time, production_counter, duration_sec, max_pressure, is_valid, is_test_run)
            VALUES %s
        """
        
        data_tuples = [
            (
                row['machine_id'],
                row['start_time'],
                row['end_time'],
                row['production_counter'],
                row['duration_sec'],
                row['max_pressure'],
                row['is_valid'],
                row['is_test_run']
            )
            for _, row in df.iterrows()
        ]
        
        execute_values(cur, insert_query, data_tuples)
        conn.commit()
        print("Import successful!")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Import failed: {e}")

if __name__ == "__main__":
    import_csv()
