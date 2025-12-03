import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np

# DB Config
DB_PARAMS = {
    "host": "127.0.0.1",
    "port": 25432,
    "user": "postgres",
    "password": "postgres",
    "dbname": "postgres"
}

CREATE_TABLE_SQL = """
-- Create tb_cycle_log table to store segmented cycle metadata
CREATE TABLE IF NOT EXISTS "public"."tb_cycle_log" (
    "id" uuid DEFAULT uuid_generate_v4() NOT NULL PRIMARY KEY,
    "created_at" timestamp with time zone DEFAULT now(),
    "machine_id" text NOT NULL,
    "start_time" timestamp with time zone NOT NULL,
    "end_time" timestamp with time zone NOT NULL,
    "production_counter" bigint,
    "work_log_id" bigint REFERENCES "public"."tb_work_log"("id"),
    "duration_sec" double precision,
    "max_pressure" double precision,
    "is_valid" boolean DEFAULT false,
    "is_test_run" boolean DEFAULT false,
    "segmentation_method" text DEFAULT 'pressure_threshold_30bar'
);

-- Add indexes for performance
CREATE INDEX IF NOT EXISTS idx_cycle_log_machine_time ON "public"."tb_cycle_log" ("machine_id", "start_time");
CREATE INDEX IF NOT EXISTS idx_cycle_log_work_log_id ON "public"."tb_cycle_log" ("work_log_id");

-- Enable RLS (Optional for local but good practice)
ALTER TABLE "public"."tb_cycle_log" ENABLE ROW LEVEL SECURITY;

-- Policy: Allow read access to authenticated users
-- CREATE POLICY "Enable read access for all users" ON "public"."tb_cycle_log" FOR SELECT USING (true);
-- CREATE POLICY "Enable insert for authenticated users only" ON "public"."tb_cycle_log" FOR INSERT WITH CHECK (auth.role() = 'authenticated' OR auth.role() = 'service_role');
"""

def run():
    csv_path = "cycle_log_dump.csv"
    print(f"Reading {csv_path}...")
    try:
        df = pd.read_csv(csv_path)
    except FileNotFoundError:
        print("CSV file not found.")
        return

    df = df.replace({np.nan: None})
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        # 1. Create Table
        print("Creating table...")
        cur.execute(CREATE_TABLE_SQL)
        conn.commit()
        
        # 2. Reload Schema Cache
        cur.execute("NOTIFY pgrst, 'reload schema';")
        conn.commit()
        
        # 3. Insert Data
        print(f"Importing {len(df)} rows...")
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
        print(f"Operation failed: {e}")

if __name__ == "__main__":
    run()
