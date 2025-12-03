import httpx
import pandas as pd

SUPABASE_URL = "http://127.0.0.1:54321"
ANON_KEY = "sb_secret_N7UND0UgjKTVK-Uodkm0Hg_xSvEMPvz"

headers = {
    "apikey": ANON_KEY,
    "Authorization": f"Bearer {ANON_KEY}",
    "Content-Type": "application/json"
}

def fetch_specific_logs():
    # Fetch IDs 40 and 41 with ALL columns
    ids = "40,41"
    url = f"{SUPABASE_URL}/rest/v1/tb_work_log?id=in.({ids})&select=*"
    try:
        r = httpx.get(url, headers=headers, timeout=10.0)
        r.raise_for_status()
        data = r.json()
        
        if not data:
            print(f"ID {ids}에 해당하는 데이터가 없습니다.")
            return

        df = pd.DataFrame(data)
        print(f"--- ID {ids} 전체 데이터 상세 비교 ---")
        
        # Transpose to show all columns clearly
        for idx, row in df.iterrows():
            print(f"\n[ID: {row['id']}]")
            for col in df.columns:
                print(f"{col:<20}: {row[col]}")
        
    except Exception as e:
        print(f"오류 발생: {e}")

if __name__ == "__main__":
    fetch_specific_logs()
