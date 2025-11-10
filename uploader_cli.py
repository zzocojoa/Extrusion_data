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

KST = timezone(timedelta(hours=9))

LOG_FILE = 'processed_files.log'
RESUME_FILE = 'upload_resume.json'


def kst_now() -> datetime:
    return datetime.now(KST)


def resolve_config_paths():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    script_cfg = os.path.join(script_dir, 'config.ini')
    appdata = os.getenv('APPDATA') or os.path.expanduser('~')
    app_dir = os.path.join(appdata, 'ExtrusionUploader')
    try:
        os.makedirs(app_dir, exist_ok=True)
    except Exception:
        pass
    app_cfg = os.path.join(app_dir, 'config.ini')
    return script_cfg, app_cfg


def load_config(path: str | None = None) -> tuple[dict, str]:
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    defaults = {
        'SUPABASE_URL': os.environ.get('SUPABASE_URL', ''),
        'SUPABASE_ANON_KEY': os.environ.get('SUPABASE_ANON_KEY', ''),
        'EDGE_FUNCTION_URL': os.environ.get('EDGE_FUNCTION_URL', ''),
        'PLC_DIR': 'PLC_data',
        'TEMP_DIR': 'Temperature_data',
        'RANGE_MODE': 'yesterday',
        'CUSTOM_DATE': '',
        'MTIME_LAG_MIN': '15',
        'CHECK_LOCK': 'true',
    }
    script_cfg, app_cfg = resolve_config_paths()
    chosen = path or (script_cfg if os.path.exists(script_cfg) else (app_cfg if os.path.exists(app_cfg) else app_cfg))
    if os.path.exists(chosen):
        try:
            cfg.read(chosen, encoding='utf-8-sig')
        except Exception:
            with open(chosen, 'r', encoding='cp949', errors='ignore') as f:
                content = f.read()
            cfg.read_string(content if content.strip().startswith('[') else '[app]\n' + content)
        if 'app' in cfg:
            for k, v in cfg['app'].items():
                defaults[k.upper()] = v
    return defaults, chosen


def save_resume(data: dict):
    tmp = RESUME_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, RESUME_FILE)


def load_resume() -> dict:
    if os.path.exists(RESUME_FILE):
        try:
            with open(RESUME_FILE, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
        except Exception:
            return {}
    return {}


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


def load_processed(log_file: str = LOG_FILE) -> set:
    if not os.path.exists(log_file):
        return set()
    try:
        with open(log_file, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    except UnicodeDecodeError:
        with open(log_file, 'r', encoding='cp949', errors='ignore') as f:
            return set(line.strip() for line in f if line.strip())


def log_processed(folder: str, filename: str):
    key = f"{folder}/{filename}"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(key + '\n')


def is_locked(path: str) -> bool:
    try:
        if os.name == 'nt':
            import msvcrt
            with open(path, 'rb') as fh:
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                    return False
                except OSError:
                    return True
        return False
    except Exception:
        return True


def file_mtime_kst(path: str) -> datetime:
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(KST)


def parse_plc_date_from_filename(name: str) -> datetime | None:
    m = re.match(r'^(\d{2})(\d{2})(\d{2})', name)
    if not m:
        return None
    y, mo, d = m.groups()
    try:
        return datetime(int('20' + y), int(mo), int(d), tzinfo=KST)
    except Exception:
        return None


def parse_temp_end_date_from_filename(name: str) -> datetime | None:
    m = re.search(r'__([0-9]{4}-[0-9]{2}-[0-9]{2})', name)
    if m:
        date_str = m.group(1)
    else:
        matches = list(re.finditer(r'([0-9]{4}-[0-9]{2}-[0-9]{2})', name))
        if not matches:
            return None
        date_str = matches[-1].group(1)
    try:
        y, mo, d = map(int, date_str.split('-'))
        return datetime(y, mo, d, tzinfo=KST)
    except Exception:
        return None


def within_cutoff(file_date: datetime, cutoff_date: datetime) -> bool:
    return file_date.date() <= cutoff_date.date()


def stable_enough(path: str, lag_minutes: int) -> bool:
    last = file_mtime_kst(path)
    return last <= (kst_now() - timedelta(minutes=lag_minutes))


def compute_cutoff(mode: str, custom_date: str) -> datetime:
    today = kst_now().date()
    if mode == 'today':
        return datetime(today.year, today.month, today.day, tzinfo=KST)
    if mode == 'twodays':
        d = today - timedelta(days=2)
        return datetime(d.year, d.month, d.day, tzinfo=KST)
    if mode == 'custom' and custom_date:
        y, m, d = map(int, custom_date.split('-'))
        return datetime(y, m, d, tzinfo=KST)
    d = today - timedelta(days=1)
    return datetime(d.year, d.month, d.day, tzinfo=KST)


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


def edge_upload(edge_url: str, anon_key: str, df: pd.DataFrame, resume_key: str | None = None, start_index: int = 0, log=print) -> bool:
    if df.empty:
        log('    - 유효 데이터 없음(건너뜀)')
        return True
    records = df.replace({np.nan: None}).to_dict(orient='records')
    headers = {"Authorization": f"Bearer {anon_key}", "Content-Type": "application/json"}
    total = len(records)
    start = max(0, min(start_index, total))
    if start > 0:
        log(f"    - 파일 재개 지점: {start}/{total}")
    for i in range(start, total, 500):
        batch = records[i:i+500]
        try:
            r = httpx.post(edge_url, json=batch, headers=headers, timeout=30.0)
            if r.status_code >= 300:
                log(f"    업로드 실패 ({r.status_code}): {r.text[:200]}")
                return False
        except Exception as e:
            log(f"    업로드 예외: {e}")
            return False
        if resume_key:
            set_resume_offset(resume_key, min(i + len(batch), total))
    log(f"    {len(records)}건 업로드 완료(Edge)")
    return True


def list_candidates(plc_dir: str, temp_dir: str, cutoff: datetime, lag_min: int, include_today: bool, check_lock: bool, quick: bool) -> list[tuple[str, str, str, str]]:
    items = []
    processed = load_processed()
    # PLC
    if os.path.isdir(plc_dir):
        for fn in sorted(os.listdir(plc_dir)):
            if not fn.lower().endswith('.csv'):
                continue
            fdate = parse_plc_date_from_filename(fn)
            if not fdate or not within_cutoff(fdate, cutoff):
                continue
            path = os.path.join(plc_dir, fn)
            if f"{plc_dir}/{fn}" in processed or fn in processed:
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(path, lag_min):
                    continue
                if check_lock and is_locked(path):
                    continue
            if quick:
                items.append((plc_dir, fn, path, 'plc'))
            else:
                if not build_records_plc(path, fn).empty:
                    items.append((plc_dir, fn, path, 'plc'))
    # Temperature
    if os.path.isdir(temp_dir):
        for fn in sorted(os.listdir(temp_dir)):
            if not fn.lower().endswith('.csv'):
                continue
            fdate = parse_temp_end_date_from_filename(fn)
            if not fdate:
                try:
                    fdate = file_mtime_kst(os.path.join(temp_dir, fn))
                except Exception:
                    fdate = None
            if not fdate or not within_cutoff(fdate, cutoff):
                continue
            path = os.path.join(temp_dir, fn)
            if f"{temp_dir}/{fn}" in processed or fn in processed:
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(path, lag_min):
                    continue
                if check_lock and is_locked(path):
                    continue
            if quick:
                items.append((temp_dir, fn, path, 'temp'))
            else:
                if not build_records_temp(path, fn).empty:
                    items.append((temp_dir, fn, path, 'temp'))
    return items


def main():
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
        key = f'{folder}/{fn}'
        start_idx = get_resume_offset(key)
        print(f'- 업로드: {key} (재개 {start_idx})')
        df = build_records_plc(path, fn) if kind == 'plc' else build_records_temp(path, fn)
        if edge_upload(edge_url, anon_key, df, resume_key=key, start_index=start_idx, log=print):
            log_processed(folder, fn)
            set_resume_offset(key, 0)
            done += 1
        else:
            ok_all = False
    print(f'완료: {done}/{len(items)}')
    return 0 if ok_all else 1


if __name__ == '__main__':
    raise SystemExit(main())

