import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import numpy as np
from datetime import datetime, timedelta
import time

# DB Config
DB_PARAMS = {
    "host": "127.0.0.1",
    "port": 25432,
    "user": "postgres",
    "password": "postgres",
    "dbname": "postgres"
}

PRESSURE_THRESHOLD = 30.0 # bar
MIN_DURATION = 30.0 # seconds
MIN_MAX_PRESSURE = 100.0 # bar
CHUNK_SIZE = 500000 # Process 500k rows at a time

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def load_work_logs():
    print("Loading Work Logs...")
    conn = get_db_connection()
    query = """
        SELECT id as work_log_id, machine_id, start_time, end_time, die_id
        FROM tb_work_log
        WHERE start_time IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()
    
    # Ensure timestamps are timezone-aware (UTC)
    df['start_time'] = pd.to_datetime(df['start_time'], utc=True)
    df['end_time'] = pd.to_datetime(df['end_time'], utc=True)
    
    # Handle missing end_time: assume valid until next start_time for same machine
    # Or just use a far future date if it's the last one?
    # For now, let's just use what we have. If end_time is null, we can't link effectively unless we infer.
    # Let's fill NaT end_time with 'now' or next row's start time.
    
    df = df.sort_values(['machine_id', 'start_time'])
    df['next_start'] = df.groupby('machine_id')['start_time'].shift(-1)
    df['end_time'] = df['end_time'].fillna(df['next_start'])
    df['end_time'] = df['end_time'].fillna(pd.Timestamp.now(tz='UTC')) # Last one valid until now
    
    print(f"Loaded {len(df)} work logs.")
    return df

def process_chunk(metrics_df, work_log_df):
    # 1. Segmentation
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
        
        # Get counter at end (plus buffer)
        # Simple approach: take counter at end_time
        cycle_counter = metrics_df.loc[e_idx, 'production_counter']
        
        # Validation
        is_valid = (duration >= MIN_DURATION) and (max_p >= MIN_MAX_PRESSURE)
        
        # 2. Linking (Find Work Log)
        # Filter work logs for this machine
        # machine_id is not in metrics? 
        # Wait, all_metrics has device_id, not machine_id.
        # Assuming single machine '2호기(창녕)' for now as per previous context.
        # Or we need to map device_id to machine_id.
        # Let's assume '2호기(창녕)' for all 'extruder_plc' device data.
        
        machine_id = '2호기(창녕)' 
        
        # Find matching work log
        # Cycle start must be within [wl.start, wl.end]
        match = work_log_df[
            (work_log_df['machine_id'] == machine_id) &
            (work_log_df['start_time'] <= start_time) &
            (work_log_df['end_time'] >= start_time)
        ]
        
        work_log_id = None
        if not match.empty:
            work_log_id = match.iloc[0]['work_log_id']
            
        cycles.append((
            machine_id,
            start_time,
            end_time,
            int(cycle_counter) if pd.notnull(cycle_counter) else None,
            int(work_log_id) if work_log_id is not None else None,
            float(duration), # Ensure float
            float(max_p),    # Ensure float
            bool(is_valid),  # Ensure bool
            bool(not is_valid) # is_test_run
        ))
        
        s_ptr += 1
        e_ptr += 1
        
    return cycles

def run_etl():
    work_log_df = load_work_logs()
    
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get min/max timestamp to iterate
    cur.execute("SELECT min(timestamp), max(timestamp) FROM all_metrics")
    min_ts, max_ts = cur.fetchone()
    print(f"Data Range: {min_ts} ~ {max_ts}")
    
    current_ts = min_ts
    total_inserted = 0
    
    while current_ts < max_ts:
        next_ts = current_ts + timedelta(hours=1) # Process 1 hour at a time
        if next_ts > max_ts: next_ts = max_ts + timedelta(seconds=1)
        
        print(f"Processing {current_ts} ~ {next_ts}...")
        
        # Fetch chunk
        query = """
            SELECT timestamp, main_pressure, production_counter
            FROM all_metrics
            WHERE timestamp >= %s AND timestamp < %s
            ORDER BY timestamp ASC
        """
        chunk_df = pd.read_sql(query, conn, params=(current_ts, next_ts))
        
        if not chunk_df.empty:
            chunk_df['timestamp'] = pd.to_datetime(chunk_df['timestamp'], utc=True)
            
            cycles = process_chunk(chunk_df, work_log_df)
            
            if cycles:
                insert_query = """
                    INSERT INTO tb_cycle_log 
                    (machine_id, start_time, end_time, production_counter, work_log_id, duration_sec, max_pressure, is_valid, is_test_run)
                    VALUES %s
                """
                execute_values(cur, insert_query, cycles)
                conn.commit()
                total_inserted += len(cycles)
                print(f"  -> Found {len(cycles)} cycles. Total: {total_inserted}")
        
        current_ts = next_ts
        
    print("Full History Processing Complete!")
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_etl()
