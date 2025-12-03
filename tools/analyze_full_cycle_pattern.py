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

def fetch_all_data(limit=10000):
    print(f"데이터 {limit}건 조회 중...")
    # Select all relevant columns to find hidden correlations
    url = f"{SUPABASE_URL}/rest/v1/all_metrics?select=*&order=timestamp.desc&limit={limit}"
    
    try:
        r = httpx.get(url, headers=headers, timeout=60.0)
        r.raise_for_status()
        data = r.json()
        if not data:
            print("데이터가 없습니다.")
            return None
        return pd.DataFrame(data)
    except Exception as e:
        print(f"데이터 조회 실패: {e}")
        return None

def analyze_patterns(df):
    # Preprocessing
    df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
    df = df.sort_values('timestamp').reset_index(drop=True)
    
    print(f"\n[데이터 개요]")
    print(f"기간: {df['timestamp'].min()} ~ {df['timestamp'].max()}")
    print(f"총 데이터 수: {len(df)}행")
    print(f"컬럼 목록: {list(df.columns)}")

    # 1. Production Counter Analysis
    df['counter_diff'] = df['production_counter'].diff()
    cycle_changes = df[df['counter_diff'] > 0].index
    
    print(f"\n[1. 사이클 변경 분석]")
    print(f"총 사이클 변경 횟수: {len(cycle_changes)}회")
    
    if len(cycle_changes) == 0:
        print("사이클 변경이 감지되지 않았습니다. 더 넓은 범위의 데이터가 필요할 수 있습니다.")
        return

    # Analyze metrics around change points
    stats = []
    window = 10 # seconds before/after
    
    for idx in cycle_changes:
        start_idx = max(0, idx - window)
        end_idx = min(len(df), idx + window)
        subset = df.iloc[start_idx:end_idx]
        
        # Check pressure drop
        min_pressure = subset['main_pressure'].min()
        max_pressure = subset['main_pressure'].max()
        
        # Check billet length behavior
        start_len = subset.iloc[0]['billet_length']
        end_len = subset.iloc[-1]['billet_length']
        
        stats.append({
            'timestamp': df.iloc[idx]['timestamp'],
            'counter_from': df.iloc[idx-1]['production_counter'],
            'counter_to': df.iloc[idx]['production_counter'],
            'min_pressure_in_window': min_pressure,
            'max_pressure_in_window': max_pressure,
            'billet_len_start': start_len,
            'billet_len_end': end_len
        })
    
    stats_df = pd.DataFrame(stats)
    print("\n[2. 사이클 변경 시점의 주요 지표 통계]")
    print(stats_df.describe().to_string())
    
    print("\n[3. 패턴 상세 확인 (처음 5개 사이클)]")
    print(stats_df.head().to_string())

    # 4. Correlation Analysis (Entire Dataset)
    print("\n[4. 전체 데이터 상관관계 분석]")
    # Select numeric columns only
    numeric_df = df.select_dtypes(include=[np.number])
    corr = numeric_df.corr()['production_counter'].sort_values(ascending=False)
    print("Production Counter와 가장 연관성이 높은 변수:")
    print(corr.to_string())

if __name__ == "__main__":
    df = fetch_all_data(limit=50000) # Fetch a large chunk
    if df is not None:
        analyze_patterns(df)
