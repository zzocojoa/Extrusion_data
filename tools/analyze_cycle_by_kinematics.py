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
    print(f"데이터 {limit}건 조회 중 (Kinematics 기반 분석)...")
    url = f"{SUPABASE_URL}/rest/v1/all_metrics?select=timestamp,current_speed,billet_length,main_pressure&order=timestamp.desc&limit={limit}"
    try:
        r = httpx.get(url, headers=headers, timeout=60.0)
        r.raise_for_status()
        data = r.json()
        if not data: return None
        return pd.DataFrame(data)
    except Exception as e:
        print(f"오류: {e}")
        return None

def analyze_kinematics(df):
    # Preprocessing
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    # Calculate time delta in seconds
    df['dt'] = df['timestamp'].diff().dt.total_seconds().fillna(0)
    
    # 1. Speed Analysis
    # Speed usually has less noise than pressure during idle?
    # Let's see how many zero-speed points exist
    zero_speed_count = (df['current_speed'] == 0).sum()
    print(f"\n[1. 속도(Speed) 데이터 특성]")
    print(f"전체 데이터 중 속도 0인 비율: {zero_speed_count / len(df) * 100:.1f}%")
    print(f"최대 속도: {df['current_speed'].max()}")
    
    # Define "Moving" state
    speed_threshold = 0.5 # mm/s?
    df['is_moving'] = df['current_speed'] > speed_threshold
    
    # 2. Billet Length Analysis
    # Calculate rate of change
    df['d_length'] = df['billet_length'].diff().fillna(0)
    
    # Detect "Loading" events (Length increases significantly)
    # If length goes 0 -> 500, d_length will be +500
    loading_events = df[df['d_length'] > 100] # Arbitrary large jump
    
    print(f"\n[2. 빌렛 장전(Loading) 이벤트 감지]")
    print(f"총 장전 횟수: {len(loading_events)}")
    if len(loading_events) > 0:
        print("장전 시점 샘플:")
        print(loading_events[['timestamp', 'billet_length', 'd_length', 'main_pressure']].head().to_string())

    # 3. Compare Speed Cycles vs Pressure Cycles
    # We'll detect cycles based on Speed > 0.5
    df['speed_change'] = df['is_moving'].astype(int).diff()
    starts = df[df['speed_change'] == 1].index
    ends = df[df['speed_change'] == -1].index
    
    # Filter short movements
    valid_speed_cycles = 0
    for i in range(min(len(starts), len(ends))):
        if ends[i] < starts[i]: continue
        duration = (df.iloc[ends[i]]['timestamp'] - df.iloc[starts[i]]['timestamp']).total_seconds()
        if duration > 30:
            valid_speed_cycles += 1
            
    print(f"\n[3. 속도 기반 사이클 분석]")
    print(f"속도 기반 유효 사이클 수(>30s): {valid_speed_cycles}")
    
    # 4. Cross Verification
    # Do Loading Events happen exactly during Zero Speed?
    # Check speed at loading events
    if len(loading_events) > 0:
        speeds_at_loading = df.loc[loading_events.index, 'current_speed']
        print(f"\n[4. 장전 시점의 설비 상태]")
        print(f"장전 중 평균 속도: {speeds_at_loading.mean():.2f}")
        print(f"장전 중 평균 압력: {df.loc[loading_events.index, 'main_pressure'].mean():.1f}")
        
        if speeds_at_loading.mean() < 1.0:
             print("=> 결론: 빌렛 장전은 설비가 멈춰있을 때(Speed~0) 발생합니다.")
        else:
             print("=> 결론: 설비가 움직이는 도중에 장전 신호가 잡힙니다 (특이사항).")

if __name__ == "__main__":
    df = fetch_data(limit=50000)
    if df is not None:
        analyze_kinematics(df)
