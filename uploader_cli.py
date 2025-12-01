import os
import sys
import re
import argparse
import json
from datetime import datetime, timedelta, timezone
import configparser

import pandas as pd
import numpy as np
import httpx

from core.config import get_data_dir, load_config
from core.transform import build_records_plc as core_build_records_plc, build_records_temp as core_build_records_temp
from core import state as core_state
from core import files as core_files
from core import upload as core_upload

KST = timezone(timedelta(hours=9))

# Data directory for logs/state (AppData)
DATA_DIR = get_data_dir()
LOG_PATH = os.path.join(DATA_DIR, 'processed_files.log')
RESUME_PATH = os.path.join(DATA_DIR, 'upload_resume.json')


def migrate_legacy_state():
    """Wrapper calling shared core.state.migrate_legacy_state."""
    core_state.migrate_legacy_state(os.path.dirname(os.path.abspath(__file__)))


def kst_now() -> datetime:
    return datetime.now(KST)


def resolve_config_paths():
    # Kept for backward compatibility; core.config.load_config now handles paths.
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_cfg = os.path.join(script_dir, 'config.ini')
    app_cfg = os.path.join(get_data_dir(), 'config.ini')
    return script_cfg, app_cfg


def load_config(path: str | None = None) -> tuple[dict, str]:
    # Delegate to shared core.config implementation
    return load_config(path)


def save_resume(data: dict):
    core_state.save_resume(data, RESUME_PATH)


def load_resume() -> dict:
    return core_state.load_resume(RESUME_PATH)


def set_resume_offset(key: str, offset: int):
    core_state.set_resume_offset(key, offset, RESUME_PATH)


def get_resume_offset(key: str) -> int:
    return core_state.get_resume_offset(key, RESUME_PATH)


def load_processed(log_file: str = LOG_PATH) -> set:
    return core_state.load_processed(log_file)


def log_processed(folder: str, filename: str):
    core_state.log_processed(folder, filename, LOG_PATH)


def is_locked(path: str) -> bool:
    return core_files.is_locked(path)


def file_mtime_kst(path: str) -> datetime:
    return core_files.file_mtime_kst(path)


def parse_plc_date_from_filename(name: str) -> datetime | None:
    return core_files.parse_plc_date_from_filename(name)


def parse_temp_end_date_from_filename(name: str) -> datetime | None:
    return core_files.parse_temp_end_date_from_filename(name)


def within_cutoff(file_date: datetime, cutoff_date: datetime) -> bool:
    return core_files.within_cutoff(file_date, cutoff_date)


def stable_enough(path: str, lag_minutes: int) -> bool:
    return core_files.stable_enough(path, lag_minutes)


def compute_cutoff(mode: str, custom_date: str) -> datetime:
    return core_files.compute_cutoff(mode, custom_date)


def build_records_plc(file_path: str, filename: str) -> pd.DataFrame:
    try:
        try:
            df = pd.read_csv(file_path)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='cp949')
        candidates = {
            'time': ['시간', '시각', 'Time'],
            'main_pressure': ['메인압력', '메인 압력'],
            'billet_length': ['빌렛길이', '빌렛 길이'],
            'container_temp_front': ['콘테이너온도 앞쪽', '콘테이너 온도 앞쪽'],
            'container_temp_rear': ['콘테이너온도 뒤쪽', '콘테이너 온도 뒤쪽'],
            'production_counter': ['생산카운트', '생산 카운트'],
            'current_speed': ['현재속도', '현재 속도'],
        }
        colmap = {}
        for key, names in candidates.items():
            for n in names:
                if n in df.columns:
                    colmap[key] = n
                    break
        # Fallback heuristics for columns that often differ by one word
        cols = list(df.columns)
        if 'container_temp_rear' not in colmap:
            # 1) 이름 기반: "뒤쪽"/"후면" 포함 컬럼
            for cname in cols:
                # e.g. "콘테이너온도 뒤쪽", "콘테이너 온도 뒤쪽"
                if '뒤쪽' in cname or '후면' in cname:
                    colmap['container_temp_rear'] = cname
                    break
        if 'container_temp_rear' not in colmap and 'container_temp_front' in colmap:
            # 2) 순서 기반: 앞쪽 바로 다음에 오는 숫자형 컬럼을 뒤쪽으로 간주
            try:
                front_idx = cols.index(colmap['container_temp_front'])
            except ValueError:
                front_idx = -1
            if front_idx >= 0:
                used = set(colmap.values())
                for cname in cols[front_idx + 1:]:
                    if cname in used:
                        continue
                    s = df[cname]
                    # 숫자형 열만 후보로
                    if getattr(s.dtype, 'kind', None) in ('i', 'u', 'f', 'c'):
                        colmap['container_temp_rear'] = cname
                        break
        if 'production_counter' not in colmap:
            for cname in cols:
                # e.g. "생산카운트", "생산카운터"
                if '생산' in cname and ('카운트' in cname or '카운터' in cname):
                    colmap['production_counter'] = cname
                    break
        if 'time' not in colmap:
            raise ValueError('필수 컬럼 누락(시간)')
        date_str = f"20{filename[0:2]}-{filename[2:4]}-{filename[4:6]}"
        df['timestamp'] = df[colmap['time']].apply(lambda t: f"{date_str}T{t}+09:00")
        out = pd.DataFrame()
        out['timestamp'] = df['timestamp']
        out['device_id'] = 'extruder_plc'
        for key in ['main_pressure','billet_length','container_temp_front','container_temp_rear','production_counter','current_speed']:
            if key in colmap:
                out[key] = df[colmap[key]]
        return out
    except Exception:
        return pd.DataFrame()


def build_records_temp(file_path: str, filename: str) -> pd.DataFrame:
    try:
        try:
            df = pd.read_csv(file_path, header=0)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, header=0, encoding='cp949')
        df.columns = df.columns.str.strip().str.replace(r'\[|\]', '', regex=True)
        lower_map = {c.lower(): c for c in df.columns}

        def pick(*cands: str) -> str | None:
            for c in cands:
                key = c.lower()
                if key in lower_map:
                    return lower_map[key]
            return None

        dt_col = pick('datetime', 'date_time', '날짜시간', '일시')
        date_col = pick('date', '날짜', '일자')
        time_col = pick('time', '시간', '시각')
        temp_main = pick('temperature', '온도', 'temp')
        if temp_main is None:
            raise ValueError('온도(Temperature) 컬럼을 찾을 수 없습니다')

        if dt_col is not None:
            dt = pd.to_datetime(df[dt_col], errors='coerce')
        elif date_col is not None and time_col is not None:
            tstr = df[time_col].astype(str)
            has_ms = tstr.str.count(':') >= 3
            tconv = tstr
            if has_ms.any():
                parts = tstr.str.rsplit(':', n=1, expand=True)
                tconv = parts[0] + '.' + parts[1]
            dt = pd.to_datetime(df[date_col].astype(str) + ' ' + tconv, errors='coerce')
        else:
            raise ValueError('날짜/시간 컬럼을 찾을 수 없습니다')

        out = pd.DataFrame()
        out['timestamp'] = dt.dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str.rstrip('0').str.rstrip('.') + '+09:00'
        out['device_id'] = 'spot_temperature_sensor'
        temp_series = df[temp_main].replace('-', np.nan)
        out['temperature'] = pd.to_numeric(temp_series, errors='coerce')
        out.dropna(subset=['timestamp', 'temperature'], inplace=True)
        return out[['timestamp', 'device_id', 'temperature']]
    except Exception:
        return pd.DataFrame()


def list_candidates(plc_dir: str, temp_dir: str, cutoff: datetime, lag_min: int, include_today: bool, check_lock: bool, quick: bool) -> list[tuple[str, str, str, str]]:
    # For CLI we still honor "quick" by optionally filtering with content checks.
    base_items = core_files.list_candidates(plc_dir, temp_dir, cutoff, lag_min, include_today, check_lock, quick=True)
    if quick:
        return base_items
    filtered: list[tuple[str, str, str, str]] = []
    for folder, fn, path, kind in base_items:
        df = build_records_plc(path, fn) if kind == 'plc' else build_records_temp(path, fn)
        if not df.empty:
            filtered.append((folder, fn, path, kind))
    return filtered


def main():
    # Migrate any legacy state from script directory to AppData
    migrate_legacy_state()
    ap = argparse.ArgumentParser(description='Extrusion Uploader (CLI, Edge)')
    ap.add_argument('--range', dest='range_mode', choices=['today','yesterday','twodays','custom'], default=None)
    ap.add_argument('--custom-date', dest='custom_date', default=None)
    ap.add_argument('--lag', dest='lag', type=int, default=None, help='minutes for stability check (default 15)')
    ap.add_argument('--check-lock', dest='check_lock', action='store_true')
    ap.add_argument('--no-check-lock', dest='check_lock', action='store_false')
    ap.set_defaults(check_lock=True)
    ap.add_argument('--quick', action='store_true', help='preview-style quick candidate selection (no content scan)')
    ap.add_argument('--plc-dir', dest='plc_dir', default=None)
    ap.add_argument('--temp-dir', dest='temp_dir', default=None)
    ap.add_argument('--config', dest='config_path', default=None)
    args = ap.parse_args()

    cfg, cfg_path = load_config(args.config_path)
    supabase_url = cfg['SUPABASE_URL']
    anon_key = cfg['SUPABASE_ANON_KEY']
    edge_url = (cfg['EDGE_FUNCTION_URL'] or (supabase_url.rstrip('/') + '/functions/v1/upload-metrics')) if supabase_url else ''
    if not (supabase_url and anon_key and edge_url):
        print('환경 설정 누락: SUPABASE_URL / SUPABASE_ANON_KEY / EDGE_FUNCTION_URL', file=sys.stderr)
        return 2

    plc_dir = args.plc_dir or cfg['PLC_DIR']
    temp_dir = args.temp_dir or cfg['TEMP_DIR']
    mode = args.range_mode or cfg['RANGE_MODE']
    custom = (args.custom_date if args.custom_date is not None else cfg['CUSTOM_DATE'])
    lag = (args.lag if args.lag is not None else int(str(cfg['MTIME_LAG_MIN'])))
    check_lock = args.check_lock if args.check_lock is not None else (str(cfg['CHECK_LOCK']).lower() == 'true')

    cutoff = compute_cutoff(mode, custom)
    include_today = (mode == 'today')

    print('===== 업로드 시작 (CLI) =====')
    print('Config:', cfg_path)
    print('범위:', mode, custom or '')
    print('폴더:', plc_dir, '|', temp_dir)

    items = list_candidates(plc_dir, temp_dir, cutoff, lag, include_today, check_lock, quick=args.quick)                                                                                      
    print(f'대상 파일: {len(items)}개')                                                                                                                                                       
    if not items:                                                                                                                                                                             
        return 0                                                                                                                                                                              
                                                                                                                                                                                            
    ok_all = True                                                                                                                                                                             
    done = 0                                                                                                                                                                                  
    for folder, fn, path, kind in items:                                                                                                                                                      
        print(f'- 업로드 {folder}/{fn}')                                                                                                                                                      
        ok = core_upload.upload_item(                                                                                                                                                         
            edge_url,                                                                                                                                                                         
            anon_key,                                                                                                                                                                         
            folder,                                                                                                                                                                           
            fn,                                                                                                                                                                               
            path,                                                                                                                                                                             
            kind,                                                                                                                                                                             
            build_plc=build_records_plc,                                                                                                                                                      
            build_temp=build_records_temp,                                                                                                                                                    
            get_resume_offset=get_resume_offset,                                                                                                                                              
            set_resume_offset_fn=set_resume_offset,                                                                                                                                           
            log_processed_fn=log_processed,                                                                                                                                                   
            log=print,                                                                                                                                                                        
            batch_size=500,                                                                                                                                                                   
            progress_cb=None,                                                                                                                                                                 
        )                                                                                                                                                                                     
        if ok:                                                                                                                                                                                
            done += 1                                                                                                                                                                         
        else:                                                                                                                                                                                 
            ok_all = False                                                                                                                                                                    
                                                                                                                                                                                            
    print(f'완료: {done}/{len(items)}개')                                                                                                                                                     
    return 0 if ok_all else 1      


# Override local implementations with shared core.transform versions
def build_records_plc(file_path: str, filename: str) -> pd.DataFrame:
    return core_build_records_plc(file_path, filename)


def build_records_temp(file_path: str, filename: str) -> pd.DataFrame:
    return core_build_records_temp(file_path, filename)


if __name__ == '__main__':
    raise SystemExit(main())
