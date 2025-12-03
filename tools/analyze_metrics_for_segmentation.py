import httpx
import pandas as pd

SUPABASE_URL = "http://127.0.0.1:54321"
ANON_KEY = "sb_secret_N7UND0UgjKTVK-Uodkm0Hg_xSvEMPvz"

headers = {
    "apikey": ANON_KEY,
    "Authorization": f"Bearer {ANON_KEY}",
    "Content-Type": "application/json"
}

def analyze_metrics():
    # Fetch a chunk of metrics
    # We want enough data to see a counter change. 
    # Assuming 1 second interval, 1000 rows is ~16 mins. Might be enough for one cycle?
    # Let's try fetching 2000 rows.
    url = f"{SUPABASE_URL}/rest/v1/all_metrics?select=timestamp,production_counter,billet_length,main_pressure,current_speed&order=timestamp.desc&limit=2000"
    
    try:
        print("데이터 조회 중... (최대 2000건)")
        r = httpx.get(url, headers=headers, timeout=30.0)
        r.raise_for_status()
        data = r.json()
        
        if not data:
            print("데이터가 없습니다.")
            return

        df = pd.DataFrame(data)
        # Sort by time ascending to analyze flow
        # Use mixed format to handle potential variations (e.g. with/without microseconds)
        df['timestamp'] = pd.to_datetime(df['timestamp'], format='mixed')
        df = df.sort_values('timestamp')
        
        print(f"총 {len(df)}건 데이터 로드 완료.")
        print(f"시간 범위: {df['timestamp'].min()} ~ {df['timestamp'].max()}")
        
        # Check for counter changes
        df['prev_counter'] = df['production_counter'].shift(1)
        changes = df[df['production_counter'] != df['prev_counter']].dropna()
        
        if changes.empty:
            print("\n[!] 조회된 범위 내에서 production_counter 변화가 없습니다.")
            print("데이터 샘플 (처음 5행):")
            print(df[['timestamp', 'production_counter', 'billet_length', 'main_pressure']].head().to_string(index=False))
            print("\n데이터 샘플 (마지막 5행):")
            print(df[['timestamp', 'production_counter', 'billet_length', 'main_pressure']].tail().to_string(index=False))
            return

        print(f"\n[!] 총 {len(changes)}번의 사이클 변경(Counter Change) 감지됨.")
        
        for idx, row in changes.head(3).iterrows():
            print(f"\n--- 변경 시점 #{idx} ---")
            # Show context around this change
            # Find integer index
            loc = df.index.get_loc(idx)
            start_loc = max(0, loc - 5)
            end_loc = min(len(df), loc + 5)
            
            context = df.iloc[start_loc:end_loc]
            print(context[['timestamp', 'production_counter', 'billet_length', 'main_pressure', 'current_speed']].to_string())

    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    analyze_metrics()
