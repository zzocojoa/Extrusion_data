import httpx
import pandas as pd
import numpy as np
from datetime import timedelta

SUPABASE_URL = "http://127.0.0.1:54321"
ANON_KEY = "sb_secret_N7UND0UgjKTVK-Uodkm0Hg_xSvEMPvz"

headers = {
    "apikey": ANON_KEY,
    "Authorization": f"Bearer {ANON_KEY}",
    "Content-Type": "application/json"
}

PRESSURE_THRESHOLD = 30.0 # bar
MIN_DURATION = 30.0 # seconds
MIN_MAX_PRESSURE = 100.0 # bar (to be valid)

def fetch_metrics(limit=10000):
    print(f"데이터 {limit}건 조회 중 (최신순)...")
    url = f"{SUPABASE_URL}/rest/v1/all_metrics?select=timestamp,main_pressure,production_counter,device_id&order=timestamp.desc"
    
    # Supabase requires Range header for > 1000 rows usually, or we can just make multiple requests.
    # For simplicity in this script, let's just try to get the default page (1000) but sorted DESC to see recent data.
    # If we want more, we need a loop.
    
    all_data = []
    batch_size = 1000
    
    try:
        for i in range(0, limit, batch_size):
            headers["Range"] = f"{i}-{i+batch_size-1}"
            r = httpx.get(url, headers=headers, timeout=60.0)
            r.raise_for_status()
            data = r.json()
            if not data: break
            all_data.extend(data)
            print(f"  - {len(all_data)}건 로드됨...")
            
        return pd.DataFrame(all_data)
    except Exception as e:
        print(f"오류: {e}")
        return None

def run_segmentation():
    df = fetch_metrics(limit=100000) # Fetch enough data
    if df is None or df.empty:
        print("데이터가 없습니다.")
        return

    # Preprocessing
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Filter for PLC data if needed (assuming main_pressure is only from PLC)
    # df = df[df['device_id'] == 'extruder_plc'] 
    
    print(f"분석 대상: {len(df)}행 ({df['timestamp'].min()} ~ {df['timestamp'].max()})")

    # Detect Cycles
    # Logic: 
    # 1. Find continuous regions where Pressure > 30
    # 2. Start = First point > 30
    # 3. End = First point < 30 after Start
    
    df['is_active'] = df['main_pressure'] > PRESSURE_THRESHOLD
    df['active_change'] = df['is_active'].astype(int).diff()
    
    starts = df[df['active_change'] == 1].index
    ends = df[df['active_change'] == -1].index
    
    cycles = []
    
    # Align starts and ends
    # We need to handle edge cases (starts before ends, ends before starts)
    
    s_ptr = 0
    e_ptr = 0
    
    while s_ptr < len(starts) and e_ptr < len(ends):
        # Find first start
        s_idx = starts[s_ptr]
        
        # Find first end after this start
        while e_ptr < len(ends) and ends[e_ptr] < s_idx:
            e_ptr += 1
            
        if e_ptr >= len(ends):
            break
            
        e_idx = ends[e_ptr]
        
        # Extract Cycle Data
        cycle_slice = df.iloc[s_idx:e_idx+1] # Include end point?
        
        start_time = df.iloc[s_idx]['timestamp']
        end_time = df.iloc[e_idx]['timestamp'] # Time when it dropped below 30
        duration = (end_time - start_time).total_seconds()
        
        max_p = cycle_slice['main_pressure'].max()
        
        # Determine Cycle ID (Counter)
        # We take the counter value at the END of the cycle (or slightly after?)
        # Analysis showed counter increments at the END (during dead cycle).
        # So we should look at the counter value *after* the pressure drop?
        # Let's look at counter at e_idx + small buffer?
        # Or just take the max counter in the slice?
        # If counter increments 22 -> 23 during dead cycle, then the cycle that just finished was 22?
        # Wait, if 22 -> 23 happens *after* pressure drop, then the cycle that just finished produced billet #23?
        # No, usually counter increments when "Done". So if it becomes 23, it means #23 is done.
        # So the cycle that just finished IS #23.
        # So we should look for the counter value *after* the drop.
        
        # Let's look at 10 seconds after end_time
        # But we only have indices.
        # Let's take the counter value at e_idx + 5 (if exists)
        check_idx = min(len(df)-1, e_idx + 5)
        cycle_counter = df.iloc[check_idx]['production_counter']
        
        # Validation
        is_valid = (duration >= MIN_DURATION) and (max_p >= MIN_MAX_PRESSURE)
        
        cycles.append({
            'machine_id': '2호기(창녕)', # Hardcoded for now or derive from data
            'start_time': start_time.isoformat(),
            'end_time': end_time.isoformat(),
            'production_counter': int(cycle_counter) if pd.notnull(cycle_counter) else None,
            'duration_sec': duration,
            'max_pressure': max_p,
            'is_valid': is_valid,
            'is_test_run': not is_valid # Simple logic for now
        })
        
        s_ptr += 1
        e_ptr += 1

    print(f"감지된 사이클: {len(cycles)}건")
    
    if not cycles:
        return

    # Insert into DB (Direct SQL via psycopg2)
    import psycopg2
    from psycopg2.extras import execute_values
    
    DB_PARAMS = {
        "host": "127.0.0.1",
        "port": 54322,
        "user": "postgres",
        "password": "your-super-secret-and-long-postgres-password", # Default local supabase password
        "dbname": "postgres"
    }
    
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        
        # 1. Reload Schema Cache (Fix for API)
        cur.execute("NOTIFY pgrst, 'reload schema';")
        conn.commit()
        print("Schema Cache Reloaded.")
        
        # 2. Insert Data
        insert_query = """
            INSERT INTO tb_cycle_log 
            (machine_id, start_time, end_time, production_counter, duration_sec, max_pressure, is_valid, is_test_run)
            VALUES %s
        """
        
        data_tuples = [
            (
                c['machine_id'],
                c['start_time'],
                c['end_time'],
                c['production_counter'],
                c['duration_sec'],
                c['max_pressure'],
                c['is_valid'],
                c['is_test_run']
            )
            for c in cycles
        ]
        
        execute_values(cur, insert_query, data_tuples)
        conn.commit()
        print(f"총 {len(data_tuples)}건 저장 완료 (Direct SQL).")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"DB 저장 실패: {e}")
        # Fallback to CSV
        csv_path = "cycle_log_dump.csv"
        pd.DataFrame(cycles).to_csv(csv_path, index=False)
        print(f"데이터를 {csv_path}에 임시 저장했습니다.")
        pass

if __name__ == "__main__":
    run_segmentation()
