import os
import sys
import re
from datetime import datetime, timedelta, timezone
import configparser
import threading

import pandas as pd
import numpy as np
import httpx
import PySimpleGUI as sg

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

KST = timezone(timedelta(hours=9))
LOG_FILE = 'processed_files.log'
DEFAULT_PLC_DIR = 'PLC_data'
DEFAULT_TEMP_DIR = 'Temperature_data'


def kst_now() -> datetime:
    return datetime.now(KST)


def load_processed() -> set:
    if not os.path.exists(LOG_FILE):
        return set()
    with open(LOG_FILE, 'r', encoding='utf-8') as f:
        return set(line.strip() for line in f if line.strip())


def log_processed(folder: str, filename: str):
    key = f"{folder}/{filename}"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(key + '\n')


def is_locked(path: str) -> bool:
    try:
        if os.name == 'nt':
            # Best-effort lock check on Windows
            import msvcrt
            with open(path, 'rb') as fh:
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                    return False
                except OSError:
                    return True
        else:
            # Non-Windows: rely on mtime stability
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
    # Example: LandData-2025-01-23_07-36-55__2025-01-24_07-36-41.csv
    m = re.search(r'__([0-9]{4}-[0-9]{2}-[0-9]{2})', name)
    if not m:
        m2 = re.search(r'LandData-([0-9]{4}-[0-9]{2}-[0-9]{2})', name)
        if not m2:
            return None
        date_str = m2.group(1)
    else:
        date_str = m.group(1)
    try:
        y, mo, d = map(int, date_str.split('-'))
        return datetime(y, mo, d, tzinfo=KST)
    except Exception:
        return None


def within_cutoff(file_date: datetime, cutoff_date: datetime) -> bool:
    # Include files with date <= cutoff (date-only comparison)
    return file_date.date() <= cutoff_date.date()


def stable_enough(path: str, lag_minutes: int) -> bool:
    last = file_mtime_kst(path)
    return last <= (kst_now() - timedelta(minutes=lag_minutes))


def load_config(path: str = 'config.ini') -> dict:
    cfg = configparser.ConfigParser()
    defaults = {
        'SUPABASE_URL': os.environ.get('SUPABASE_URL', ''),
        'SUPABASE_ANON_KEY': os.environ.get('SUPABASE_ANON_KEY', ''),
        'EDGE_FUNCTION_URL': os.environ.get('EDGE_FUNCTION_URL', ''),
        'PLC_DIR': DEFAULT_PLC_DIR,
        'TEMP_DIR': DEFAULT_TEMP_DIR,
        'RANGE_MODE': 'yesterday',  # today|yesterday|twodays|custom
        'CUSTOM_DATE': '',
        'MTIME_LAG_MIN': '15',
        'CHECK_LOCK': 'true',
    }
    if os.path.exists(path):
        cfg.read(path, encoding='utf-8')
        if 'app' in cfg:
            defaults.update(cfg['app'])
    return defaults


def save_config(values: dict, path: str = 'config.ini'):
    cfg = configparser.ConfigParser()
    cfg['app'] = {k: str(v) for k, v in values.items()}
    with open(path, 'w', encoding='utf-8') as f:
        cfg.write(f)


def build_records_plc(file_path: str, filename: str) -> pd.DataFrame:
    try:
        try:
            df = pd.read_csv(file_path)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding='cp949')
        # Adjust these column names to your actual CSV headers
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
        required = ['time']
        if not all(k in colmap for k in required):
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
    except Exception as e:
        print(f"PLC 변환 오류: {filename} - {e}")
        return pd.DataFrame()


def build_records_temp(file_path: str, filename: str) -> pd.DataFrame:
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
        out = pd.DataFrame({'timestamp': df['timestamp'], 'device_id': 'spot_temperature_sensor', 'temperature': df['Temperature']})
        return out
    except Exception as e:
        print(f"온도 변환 오류: {filename} - {e}")
        return pd.DataFrame()


def edge_upload(url: str, anon_key: str, df: pd.DataFrame) -> bool:
    if df.empty:
        return True
    records = df.replace({np.nan: None}).to_dict(orient='records')
    headers = {"Authorization": f"Bearer {anon_key}", "Content-Type": "application/json"}
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        r = httpx.post(url, json=batch, headers=headers, timeout=30.0)
        if r.status_code >= 300:
            print('Edge error:', r.status_code, r.text[:200])
            return False
    return True


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
    # default yesterday
    d = today - timedelta(days=1)
    return datetime(d.year, d.month, d.day, tzinfo=KST)


def list_candidates(plc_dir: str, temp_dir: str, cutoff: datetime, lag_min: int, include_today: bool, check_lock: bool):
    items = []  # list of (folder, filename, path)
    processed = load_processed()

    # PLC
    if os.path.isdir(plc_dir):
        for fn in sorted(os.listdir(plc_dir)):
            if not fn.endswith('.csv'):
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
            items.append((plc_dir, fn, path, 'plc'))

    # Temperature
    if os.path.isdir(temp_dir):
        for fn in sorted(os.listdir(temp_dir)):
            if not (fn.endswith('.csv') and fn.startswith('LandData')):
                continue
            fdate = parse_temp_end_date_from_filename(fn)
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
            items.append((temp_dir, fn, path, 'temp'))

    return items


def process_file(kind: str, path: str, filename: str) -> pd.DataFrame:
    if kind == 'plc':
        return build_records_plc(path, filename)
    return build_records_temp(path, filename)


def run_upload(values: dict, window: sg.Window):
    url = values['SUPABASE_URL'].strip()
    anon = values['SUPABASE_ANON_KEY'].strip()
    edge = values['EDGE_FUNCTION_URL'].strip() or (url.rstrip('/') + '/functions/v1/upload-metrics')
    plc_dir = values['PLC_DIR']
    temp_dir = values['TEMP_DIR']
    mode = values['RANGE_MODE']
    custom = values.get('CUSTOM_DATE', '')
    lag = int(values.get('MTIME_LAG_MIN', '15'))
    check_lock = str(values.get('CHECK_LOCK', 'true')).lower() == 'true'

    cutoff = compute_cutoff(mode, custom)
    include_today = (mode == 'today')
    items = list_candidates(plc_dir, temp_dir, cutoff, lag, include_today, check_lock)
    window['-LOG-'].print(f'대상 파일: {len(items)}개')
    if not items:
        return

    ok = True
    count_files = 0
    for folder, fn, path, kind in items:
        window['-LOG-'].print(f'처리: {folder}/{fn}')
        df = process_file(kind, path, fn)
        if edge_upload(edge, anon, df):
            log_processed(folder, fn)
            count_files += 1
            window['-PROG-'].update_bar(count_files, len(items))
        else:
            window['-LOG-'].print(f'  업로드 실패: {fn}', text_color='red')
            ok = False
    if ok:
        window['-LOG-'].print(f'완료: {count_files}개 파일 업로드')
    else:
        window['-LOG-'].print('일부 실패가 발생했습니다.', text_color='yellow')


def main():
    cfg = load_config()

    sg.theme('SystemDefault')
    layout = [
        [sg.Text('Supabase URL'), sg.Input(cfg['SUPABASE_URL'], key='SUPABASE_URL', size=(50,1))],
        [sg.Text('Anon Key'), sg.Input(cfg['SUPABASE_ANON_KEY'], key='SUPABASE_ANON_KEY', size=(50,1), password_char='*')],
        [sg.Text('Edge Function URL'), sg.Input(cfg['EDGE_FUNCTION_URL'], key='EDGE_FUNCTION_URL', size=(50,1))],
        [sg.Text('PLC 폴더'), sg.Input(cfg['PLC_DIR'], key='PLC_DIR', size=(40,1)), sg.FolderBrowse('찾기')],
        [sg.Text('온도 폴더'), sg.Input(cfg['TEMP_DIR'], key='TEMP_DIR', size=(40,1)), sg.FolderBrowse('찾기')],
        [sg.Text('업로드 범위'),
         sg.Radio('오늘까지(안정 N분 필요)', 'RANGE', key='RM_TODAY', default=(cfg['RANGE_MODE']=='today')),
         sg.Radio('어제까지', 'RANGE', key='RM_YDAY', default=(cfg['RANGE_MODE']=='yesterday')),
         sg.Radio('이틀 전까지', 'RANGE', key='RM_2DAY', default=(cfg['RANGE_MODE']=='twodays')),
         sg.Radio('사용자 지정', 'RANGE', key='RM_CUSTOM', default=(cfg['RANGE_MODE']=='custom'))],
        [sg.Text('사용자 지정(YYYY-MM-DD)'), sg.Input(cfg['CUSTOM_DATE'], key='CUSTOM_DATE', size=(15,1))],
        [sg.Text('안정성(마지막 수정 후 분)'), sg.Input(cfg['MTIME_LAG_MIN'], key='MTIME_LAG_MIN', size=(6,1)), sg.Checkbox('잠금 파일 제외(가능 시)', key='CHECK_LOCK', default=(cfg['CHECK_LOCK']=='true'))],
        [sg.Button('설정 저장'), sg.Button('미리보기'), sg.Button('업로드 시작'), sg.Button('종료')],
        [sg.ProgressBar(100, orientation='h', size=(50, 20), key='-PROG-')],
        [sg.Multiline(size=(100,20), key='-LOG-', autoscroll=True, reroute_stdout=True, reroute_stderr=True)],
    ]

    window = sg.Window('Extrusion Uploader (Edge)', layout, finalize=True)

    def get_values_from_gui():
        mode = 'yesterday'
        if window['RM_TODAY'].get():
            mode = 'today'
        elif window['RM_2DAY'].get():
            mode = 'twodays'
        elif window['RM_CUSTOM'].get():
            mode = 'custom'
        vals = {
            'SUPABASE_URL': window['SUPABASE_URL'].get(),
            'SUPABASE_ANON_KEY': window['SUPABASE_ANON_KEY'].get(),
            'EDGE_FUNCTION_URL': window['EDGE_FUNCTION_URL'].get(),
            'PLC_DIR': window['PLC_DIR'].get() or DEFAULT_PLC_DIR,
            'TEMP_DIR': window['TEMP_DIR'].get() or DEFAULT_TEMP_DIR,
            'RANGE_MODE': mode,
            'CUSTOM_DATE': window['CUSTOM_DATE'].get(),
            'MTIME_LAG_MIN': window['MTIME_LAG_MIN'].get() or '15',
            'CHECK_LOCK': 'true' if window['CHECK_LOCK'].get() else 'false',
        }
        return vals

    while True:
        event, _ = window.read()
        if event in (sg.WINDOW_CLOSED, '종료'):
            break
        if event == '설정 저장':
            vals = get_values_from_gui()
            save_config(vals)
            window['-LOG-'].print('설정 저장 완료')
        if event == '미리보기':
            vals = get_values_from_gui()
            cutoff = compute_cutoff(vals['RANGE_MODE'], vals['CUSTOM_DATE'])
            include_today = (vals['RANGE_MODE']=='today')
            try:
                lag = int(vals['MTIME_LAG_MIN'])
            except Exception:
                lag = 15
            items = list_candidates(vals['PLC_DIR'], vals['TEMP_DIR'], cutoff, lag, include_today, vals['CHECK_LOCK']=='true')
            window['-LOG-'].print('미리보기 목록:')
            for folder, fn, _, _ in items[:200]:
                window['-LOG-'].print(f'  - {folder}/{fn}')
            window['-LOG-'].print(f'총 {len(items)}개')
        if event == '업로드 시작':
            vals = get_values_from_gui()
            window['-PROG-'].update_bar(0, 1)
            threading.Thread(target=run_upload, args=(vals, window), daemon=True).start()

    window.close()


if __name__ == '__main__':
    main()

