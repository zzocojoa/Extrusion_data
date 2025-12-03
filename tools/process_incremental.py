import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime, timedelta
import time
import sys

# DB Config
DB_PARAMS = {
    "host": "127.0.0.1",
    "port": 25432,
    "user": "postgres",
    "password": "postgres",
    "dbname": "postgres"
}

PRESSURE_THRESHOLD = 30.0
MIN_DURATION = 30.0
MIN_MAX_PRESSURE = 100.0

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def load_work_logs(start_from):
    conn = get_db_connection()
    # Load work logs that overlap with the new data period
    query = """
        SELECT id as work_log_id, machine_id, start_time, end_time, die_id
        FROM tb_work_log
        WHERE end_time >= %s OR end_time IS NULL
    """
    df = pd.read_sql(query, conn, params=(start_from,))
    conn.close()
    
    if df.empty:
        return df
        
    df['start_time'] = pd.to_datetime(df['start_time'], utc=True)
    df['end_time'] = pd.to_datetime(df['end_time'], utc=True)
    
    # Fill null end_time logic (same as full history)
    df = df.sort_values(['machine_id', 'start_time'])
    df['next_start'] = df.groupby('machine_id')['start_time'].shift(-1)
    df['end_time'] = df['end_time'].fillna(df['next_start'])
    df['end_time'] = df['end_time'].fillna(pd.Timestamp.now(tz='UTC'))
    
    return df

def process_chunk(metrics_df, work_log_df):
    # Reuse logic from process_full_history.py (simplified for brevity)
    # Ideally this should be a shared module, but for now duplicating for standalone execution
    metrics_df['is_active'] = metrics_df['main_pressure'] > PRESSURE_THRESHOLD
    metrics_df['active_change'] = metrics_df['is_active'].astype(int).diff()
    
    starts = metrics_df[metrics_df['active_change'] == 1].index
    ends = metrics_df[metrics_df['active_change'] == -1].index
    
    cycles = []
    s_ptr = 0
    e_ptr = 0
    
    while s_ptr < len(starts) and e_ptr < len(ends):
        s_idx = starts[s_ptr]
        while e_ptr < len(ends) and ends[e_ptr] < s_idx:
            e_ptr += 1
        if e_ptr >= len(ends): break
        e_idx = ends[e_ptr]
        
        start_time = metrics_df.loc[s_idx, 'timestamp']
        end_time = metrics_df.loc[e_idx, 'timestamp']
        duration = (end_time - start_time).total_seconds()
        
        cycle_slice = metrics_df.loc[s_idx:e_idx]
        max_p = cycle_slice['main_pressure'].max()
        cycle_counter = metrics_df.loc[e_idx, 'production_counter']
        
        is_valid = (duration >= MIN_DURATION) and (max_p >= MIN_MAX_PRESSURE)
        
        machine_id = '2호기(창녕)' 
        
        work_log_id = None
        if not work_log_df.empty:
            match = work_log_df[
                (work_log_df['machine_id'] == machine_id) &
                (work_log_df['start_time'] <= start_time) &
                (work_log_df['end_time'] >= start_time)
            ]
            if not match.empty:
                work_log_id = match.iloc[0]['work_log_id']
            
        cycles.append((
            machine_id,
            start_time,
            end_time,
            int(cycle_counter) if pd.notnull(cycle_counter) else None,
            int(work_log_id) if work_log_id is not None else None,
            float(duration),
            float(max_p),
            bool(is_valid),
            bool(not is_valid)
        ))
        
        s_ptr += 1
        e_ptr += 1
        
    return cycles

def run_incremental():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Get last processed time
    cur.execute("SELECT MAX(end_time) FROM tb_cycle_log")
    last_processed = cur.fetchone()[0]
    
    if last_processed is None:
        print("No existing cycles found. Please run process_full_history.py first.")
        return

    print(f"Last processed time: {last_processed}")
    
    # Buffer: Go back 1 minute to catch cycles straddling the boundary
    start_time = last_processed - timedelta(minutes=1)
    now = datetime.now().astimezone()
    
    print(f"Checking for new data from {start_time} to {now}...")
    
    # 2. Load relevant work logs
    work_log_df = load_work_logs(start_time)
    
    # 3. Fetch new metrics
    query = """
        SELECT timestamp, main_pressure, production_counter
        FROM all_metrics
        WHERE timestamp >= %s
        ORDER BY timestamp ASC
    """
    metrics_df = pd.read_sql(query, conn, params=(start_time,))
    
    if metrics_df.empty:
        print("No new data found.")
        return
        
    metrics_df['timestamp'] = pd.to_datetime(metrics_df['timestamp'], utc=True)
    print(f"Fetched {len(metrics_df)} new rows.")
    
    # 4. Process
    cycles = process_chunk(metrics_df, work_log_df)
    
    # 5. Insert (Avoid duplicates)
    # We might re-detect the last cycle. Use ON CONFLICT DO NOTHING or check existence.
    # Since we don't have a unique constraint on (machine, start_time) yet (only index),
    # we should check manually or add constraint.
    # For now, let's filter out cycles that end before last_processed.
    
    new_cycles = [c for c in cycles if c[2] > last_processed] # c[2] is end_time
    
    if new_cycles:
        print(f"Found {len(new_cycles)} new cycles.")
        insert_query = """
            INSERT INTO tb_cycle_log 
            (machine_id, start_time, end_time, production_counter, work_log_id, duration_sec, max_pressure, is_valid, is_test_run)
            VALUES %s
        """
        execute_values(cur, insert_query, new_cycles)
        conn.commit()
        print("Incremental update complete!")
    else:
        print("No new complete cycles found.")
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_incremental()
