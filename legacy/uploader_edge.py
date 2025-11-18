import os
import sys
import pandas as pd
import numpy as np
import httpx
from dotenv import load_dotenv

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

LOG_FILE = "processed_files.log"
RESUME_FILE = 'upload_resume.json'

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_ANON_KEY = os.getenv("SUPABASE_ANON_KEY")
EDGE_FUNCTION_URL = os.getenv("EDGE_FUNCTION_URL", "").strip() or (
    (SUPABASE_URL.rstrip("/") + "/functions/v1/upload-metrics") if SUPABASE_URL else ""
)

if not (SUPABASE_ANON_KEY and EDGE_FUNCTION_URL):
    print("Edge Function settings missing. Ensure SUPABASE_URL and SUPABASE_ANON_KEY are set.")
    sys.exit(1)


def load_processed_files(log_file: str) -> set:
    if not os.path.exists(log_file):
        return set()
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    except UnicodeDecodeError:
        with open(log_file, 'r', encoding='cp949', errors='ignore') as f:
            return set(line.strip() for line in f if line.strip())


def load_resume() -> dict:
    try:
        import json
        if os.path.exists(RESUME_FILE):
            with open(RESUME_FILE, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def save_resume(data: dict):
    try:
        import json
        tmp = RESUME_FILE + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, RESUME_FILE)
    except Exception:
        pass


def set_resume_offset(key: str, offset: int):
    data = load_resume()
    if offset <= 0:
        if key in data:
            del data[key]
    else:
        data[key] = int(offset)
    save_resume(data)


def get_resume_offset(key: str) -> int:
    data = load_resume()
    return int(data.get(key, 0))

def upload_via_edge_resume(df: pd.DataFrame, resume_key: str | None = None, start_index: int = 0) -> bool:
    if df.empty:
        print("    - 처리할 데이터가 없어 건너뜁니다")
        return True
    records = df.replace({np.nan: None}).to_dict(orient='records')
    headers = {"Authorization": f"Bearer {SUPABASE_ANON_KEY}", "Content-Type": "application/json"}
    total = len(records)
    start = max(0, min(start_index, total))
    if start > 0:
        print(f"    - 파일 재개 지점: {start}/{total}")
    for i in range(start, total, 500):
        batch = records[i:i+500]
        try:
            r = httpx.post(EDGE_FUNCTION_URL, json=batch, headers=headers, timeout=30.0)
            if r.status_code >= 300:
                print(f"    업로드 실패 ({r.status_code}): {r.text[:200]}")
                return False
        except Exception as e:
            print(f"    업로드 예외: {e}")
            return False
        if resume_key:
            set_resume_offset(resume_key, min(i + len(batch), total))
    print(f"    {len(records)}건 업로드 완료(Edge)")
    return True


def log_processed_file(log_file: str, folder: str, filename: str):
    key = f"{folder}/{filename}"
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(key + '\n')


def is_processed(processed: set, folder: str, filename: str) -> bool:
    return (f"{folder}/{filename}" in processed) or (filename in processed)


def process_plc_data(file_path: str, filename: str) -> pd.DataFrame:
    try:
        date_str = f"20{filename[0:2]}-{filename[2:4]}-{filename[4:6]}"
        # Try utf-8, fallback to cp949 for Korean CSVs
        try:
            df = pd.read_csv(file_path)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='cp949')

        # Column names may be localized; adjust as needed in your data
        # Placeholder expected columns (adjust to actual headers)
        required_cols = [
            "시간", "메인압력", "빌렛길이", "콘테이너온도 앞쪽", "콘테이너온도 뒤쪽", "생산카운트", "현재속도"
        ]
        df = df[required_cols]
        df['timestamp'] = df['시간'].apply(lambda t: f"{date_str}T{t}+09:00")
        df.rename(columns={
            "메인압력": "main_pressure",
            "빌렛길이": "billet_length",
            "콘테이너온도 앞쪽": "container_temp_front",
            "콘테이너온도 뒤쪽": "container_temp_rear",
            "생산카운트": "production_counter",
            "현재속도": "current_speed"
        }, inplace=True)
        df['device_id'] = 'extruder_plc'
        final_cols = ['timestamp', 'device_id', 'main_pressure', 'billet_length', 'container_temp_front', 'container_temp_rear', 'production_counter', 'current_speed']
        return df[final_cols]
    except Exception as e:
        print(f"    PLC 파일 처리 오류: {filename} - {e}")
        return pd.DataFrame()


def process_temperature_data(file_path: str, filename: str) -> pd.DataFrame:
    try:
        try:
            df = pd.read_csv(file_path, header=0)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, header=0, encoding='cp949')
        df.columns = df.columns.str.strip().str.replace(r'\[|\]', '', regex=True)
        df = df[['Date', 'Time', 'Temperature']]
        df.replace('-', np.nan, inplace=True)
        df.dropna(subset=['Date', 'Time', 'Temperature'], inplace=True)
        time_parts = df['Time'].str.rsplit(':', n=1, expand=True)
        df['timestamp'] = df['Date'] + 'T' + time_parts[0] + '.' + time_parts[1] + '+09:00'
        df.rename(columns={"Temperature": "temperature"}, inplace=True)
        df['device_id'] = 'spot_temperature_sensor'
        final_cols = ['timestamp', 'device_id', 'temperature']
        return df[final_cols]
    except Exception as e:
        print(f"    온도 파일 처리 오류: {filename} - {e}")
        return pd.DataFrame()


def upload_via_edge(df: pd.DataFrame) -> bool:
    if df.empty:
        print("    - 처리할 데이터가 없어 건너뜁니다")
        return True
    records = df.replace({np.nan: None}).to_dict(orient='records')
    headers = {"Authorization": f"Bearer {SUPABASE_ANON_KEY}", "Content-Type": "application/json"}
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        try:
            r = httpx.post(EDGE_FUNCTION_URL, json=batch, headers=headers, timeout=30.0)
            if r.status_code >= 300:
                print(f"    업로드 실패 ({r.status_code}): {r.text[:200]}")
                return False
        except Exception as e:
            print(f"    업로드 예외: {e}")
            return False
    print(f"    {len(records)}건 업로드 완료(Edge)")
    return True


if __name__ == "__main__":
    print("===== 데이터 처리 및 업로드(Edge) 시작 =====")
    processed = load_processed_files(LOG_FILE)
    print(f"📖 이전 처리 파일 {len(processed)}개")

    # PLC
    plc_folder = "PLC_data"
    print(f"\n🔄 '{plc_folder}' 폴더 처리...")
    for filename in sorted(os.listdir(plc_folder)):
        if not filename.endswith('.csv'):
            continue
        if is_processed(processed, plc_folder, filename):
            continue
        print(f"  - 업로드 대상: {filename}")
        df = process_plc_data(os.path.join(plc_folder, filename), filename)
        key = f"{plc_folder}/{filename}"
        start_idx = get_resume_offset(key)
        if upload_via_edge_resume(df, resume_key=key, start_index=start_idx):
            log_processed_file(LOG_FILE, plc_folder, filename)
            set_resume_offset(key, 0)

    # Temperature
    temp_folder = "Temperature_data"
    print(f"\n🔄 '{temp_folder}' 폴더 처리...")
    for filename in sorted(os.listdir(temp_folder)):
        if not filename.lower().endswith('.csv'):
            continue
        if is_processed(processed, temp_folder, filename):
            continue
        print(f"  - 업로드 대상: {filename}")
        df = process_temperature_data(os.path.join(temp_folder, filename), filename)
        key = f"{temp_folder}/{filename}"
        start_idx = get_resume_offset(key)
        if upload_via_edge_resume(df, resume_key=key, start_index=start_idx):
            log_processed_file(LOG_FILE, temp_folder, filename)
            set_resume_offset(key, 0)

    print("\n===== 모든 작업 완료 =====")
