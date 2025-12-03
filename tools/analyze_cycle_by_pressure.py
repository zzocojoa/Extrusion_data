import httpx
import pandas as pd
import numpy as np

SUPABASE_URL = "http://127.0.0.1:54321"
ANON_KEY = "sb_secret_N7UND0UgjKTVK-Uodkm0Hg_xSvEMPvz"

headers = {
    "apikey": ANON_KEY,
    "Authorization": f"Bearer {ANON_KEY}",
    "Content-Type": "application/json"
}

def fetch_data(limit=50000):
    print(f"데이터 {limit}건 조회 중 (Pressure 기반 분석)...")
    url = f"{SUPABASE_URL}/rest/v1/all_metrics?select=timestamp,main_pressure,current_speed,billet_length,production_counter&order=timestamp.desc&limit={limit}"
    try:
        r = httpx.get(url, headers=headers, timeout=60.0)
        r.raise_for_status()
        data = r.json()
        if not data: return None
        return pd.DataFrame(data)
    except Exception as e:
        print(f"오류: {e}")
        return None

def analyze_pressure_cycles(df):
    # Preprocessing
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # 1. Determine Thresholds
    # Let's look at the distribution of pressure
    # High pressure = Extrusion, Low pressure = Idle
    # We'll use a simple threshold of 50 bar for now, or derive it.
    p_mean = df['main_pressure'].mean()
    p_max = df['main_pressure'].max()
    threshold = 30.0 # Conservative threshold for "Active"
    
    print(f"\n[1. 압력 분포]")
    print(f"최대 압력: {p_max:.1f} bar")
    print(f"평균 압력: {p_mean:.1f} bar")
    print(f"설정 임계값: {threshold} bar (이보다 높으면 '가동 중'으로 간주)")

    # 2. Detect Cycles
    # Create a boolean series for "Active"
    df['is_active'] = df['main_pressure'] > threshold
    
    # Find transitions (False -> True = Start, True -> False = End)
    df['active_change'] = df['is_active'].astype(int).diff()
    
    starts = df[df['active_change'] == 1].index
    ends = df[df['active_change'] == -1].index
    
    # Align starts and ends
    cycles = []
    if len(starts) > 0 and len(ends) > 0:
        # Ensure we start with a Start
        if ends[0] < starts[0]:
            ends = ends[1:]
        
        # Pair them up
        min_len = min(len(starts), len(ends))
        for i in range(min_len):
            s_idx = starts[i]
            e_idx = ends[i]
            
            # Calculate duration
            start_time = df.iloc[s_idx]['timestamp']
            end_time = df.iloc[e_idx]['timestamp']
            duration = (end_time - start_time).total_seconds()
            
            # Get max pressure in this cycle
            cycle_max_p = df.iloc[s_idx:e_idx]['main_pressure'].max()
            
            # Get counter change in this cycle?
            cnt_start = df.iloc[s_idx]['production_counter']
            cnt_end = df.iloc[e_idx]['production_counter']
            
            cycles.append({
                'start_idx': s_idx,
                'end_idx': e_idx,
                'start_time': start_time,
                'end_time': end_time,
                'duration_sec': duration,
                'max_pressure': cycle_max_p,
                'counter_change': cnt_end - cnt_start
            })
            
    cycles_df = pd.DataFrame(cycles)
    
    print(f"\n[2. 감지된 물리적 사이클 (압력 기준)]")
    print(f"총 감지된 구간 수: {len(cycles_df)}")
    
    if len(cycles_df) > 0:
        # Filter out noise (too short cycles, e.g. < 10 seconds)
        valid_cycles = cycles_df[cycles_df['duration_sec'] > 30]
        print(f"유효 사이클 수 (30초 이상): {len(valid_cycles)}")
        
        print("\n[3. 사이클 통계 (유효 사이클 기준)]")
        print(valid_cycles[['duration_sec', 'max_pressure']].describe().to_string())
        
        print("\n[4. Counter와의 일치 여부]")
        # Check if production_counter actually changed during these "pressure cycles"
        matched = valid_cycles[valid_cycles['counter_change'] > 0]
        print(f"Counter가 변경된 사이클: {len(matched)} / {len(valid_cycles)}")
        
        if len(matched) < len(valid_cycles):
            print("주의: 압력은 있었으나 Counter가 증가하지 않은 구간이 있습니다. (시운전, 예열, 또는 데이터 누락 가능성)")
            
        print("\n[5. 샘플 사이클 데이터]")
        print(valid_cycles.head().to_string())

if __name__ == "__main__":
    df = fetch_data(limit=50000)
    if df is not None:
        analyze_pressure_cycles(df)
