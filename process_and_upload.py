import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv
import numpy as np
from datetime import datetime

# --- 설정 ---
LOG_FILE = "processed_files.log"

# .env 파일에서 환경 변수를 로드합니다.
load_dotenv()

# Supabase 클라이언트를 초기화합니다.
try:
    url: str = os.environ.get("SUPABASE_URL")
    key: str = os.environ.get("SUPABASE_SERVICE_KEY")
    if not url or not key:
        raise ValueError("Supabase URL 또는 Service Key가 .env 파일에 설정되지 않았습니다.")
    supabase: Client = create_client(url, key)
    print("✅ Supabase 클라이언트를 성공적으로 초기화했습니다.")
except Exception as e:
    print(f"🔥 Supabase 클라이언트 초기화 실패: {e}")
    exit()

def load_processed_files(log_file: str) -> set:
    """
    처리 완료된 파일 목록을 로그 파일에서 읽어옵니다.
    """
    if not os.path.exists(log_file):
        return set()
    with open(log_file, 'r') as f:
        return set(line.strip() for line in f)

def log_processed_file(log_file: str, filename: str):
    """
    처리 완료된 파일 이름을 로그 파일에 추가합니다.
    """
    with open(log_file, 'a') as f:
        f.write(filename + '\n')

def process_plc_data(file_path: str, filename: str) -> pd.DataFrame:
    """
    단일 PLC CSV 파일을 읽고 표준 형식의 DataFrame으로 변환합니다.
    """
    try:
        # 파일명에서 날짜 추출 (YYMMDD 형식 가정)
        date_str = f"20{filename[0:2]}-{filename[2:4]}-{filename[4:6]}"
        df = pd.read_csv(file_path)

        required_cols = ["시간", "메인압력", "빌렛길이", "콘테이너온도 앞쪽", "콘테이너온도 뒷쪽", "생산카운터", "현재속도"]
        df = df[required_cols]

        df['timestamp'] = df['시간'].apply(lambda time_str: f"{date_str}T{time_str}+09:00")

        df.rename(columns={
            "메인압력": "main_pressure",
            "빌렛길이": "billet_length",
            "콘테이너온도 앞쪽": "container_temp_front",
            "콘테이너온도 뒷쪽": "container_temp_rear",
            "생산카운터": "production_counter",
            "현재속도": "current_speed"
        }, inplace=True)

        df['device_id'] = 'extruder_plc'
        
        final_cols = ['timestamp', 'device_id', 'main_pressure', 'billet_length', 'container_temp_front', 'container_temp_rear', 'production_counter', 'current_speed']
        return df[final_cols]

    except Exception as e:
        print(f"    🔥 파일 처리 중 오류 발생: {filename} - {e}")
        return pd.DataFrame()

def process_temperature_data(file_path: str, filename: str) -> pd.DataFrame:
    """
    단일 온도 CSV 파일을 읽고 표준 형식의 DataFrame으로 변환합니다.
    """
    try:
        df = pd.read_csv(file_path, header=0)
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
        print(f"    🔥 파일 처리 중 오류 발생: {filename} - {e}")
        return pd.DataFrame()

def upload_to_supabase(df: pd.DataFrame, table_name: str = "all_metrics") -> bool:
    """
    DataFrame을 Supabase 테이블에 업로드합니다. 성공 시 True, 실패 시 False를 반환합니다.
    """
    if df.empty:
        print("    - 처리할 데이터가 없어 건너뜁니다.")
        return True # 내용이 없는 파일도 성공으로 간주

    df = df.replace({np.nan: None})
    records = df.to_dict(orient='records')
    
    batch_size = 500
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table(table_name).insert(batch).execute()
        except Exception as e:
            if 'duplicate key value' in str(e):
                print(f"    ⚠️ 데이터 중복 오류 발생. (예상된 동작)")
                # 중복은 실패가 아니므로 계속 진행
            else:
                print(f"    🔥 Supabase 업로드 중 예상치 못한 오류 발생: {e}")
                return False # 예상치 못한 오류는 실패로 간주
    
    print(f"    ✅ {len(records)}개 레코드 업로드 시도 완료.")
    return True

if __name__ == "__main__":
    print("===== 데이터 처리 및 업로드 스크립트 시작 =====")
    
    processed_files = load_processed_files(LOG_FILE)
    print(f"📖 이전에 처리된 파일 {len(processed_files)}개를 로그에서 불러왔습니다.")

    # --- PLC 데이터 처리 ---
    plc_folder = "PLC_data"
    print(f"\n🔄 '{plc_folder}' 폴더 처리를 시작합니다...")
    for filename in sorted(os.listdir(plc_folder)):
        if not filename.endswith('.csv'):
            continue
        
        if filename in processed_files:
            continue

        print(f"  - 새로운 파일 처리: {filename}")
        file_path = os.path.join(plc_folder, filename)
        plc_df = process_plc_data(file_path, filename)
        
        if not plc_df.empty:
            if upload_to_supabase(plc_df):
                log_processed_file(LOG_FILE, filename)
                print(f"    ➡️ {filename} 처리 완료 및 로그 기록")
        else:
            # 내용이 없거나 오류가 발생한 파일도 처리한 것으로 기록
            log_processed_file(LOG_FILE, filename)

    # --- 온도 데이터 처리 ---
    temp_folder = "Temperature_data"
    print(f"\n🔄 '{temp_folder}' 폴더 처리를 시작합니다...")
    for filename in sorted(os.listdir(temp_folder)):
        if not filename.endswith('.csv') or not filename.startswith('LandData'):
            continue

        if filename in processed_files:
            continue
            
        print(f"  - 새로운 파일 처리: {filename}")
        file_path = os.path.join(temp_folder, filename)
        temp_df = process_temperature_data(file_path, filename)

        if not temp_df.empty:
            if upload_to_supabase(temp_df):
                log_processed_file(LOG_FILE, filename)
                print(f"    ➡️ {filename} 처리 완료 및 로그 기록")
        else:
            log_processed_file(LOG_FILE, filename)

    print("\n===== 모든 작업이 완료되었습니다. =====")