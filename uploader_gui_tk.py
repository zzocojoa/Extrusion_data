import os
import sys
import re
import threading
from datetime import datetime, timedelta, timezone
import configparser

import pandas as pd
import numpy as np
import httpx
import subprocess

try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except Exception:
    pass

# Tkinter UI
import tkinter as tk
from tkinter import ttk, filedialog

KST = timezone(timedelta(hours=9))

# Data directory (AppData) for persistent state
def _get_data_dir() -> str:
    appdata = os.getenv('APPDATA') or os.path.expanduser('~')
    d = os.path.join(appdata, 'ExtrusionUploader')
    os.makedirs(d, exist_ok=True)
    return d

DATA_DIR = _get_data_dir()
LOG_PATH = os.path.join(DATA_DIR, 'processed_files.log')
RESUME_PATH = os.path.join(DATA_DIR, 'upload_resume.json')


def _migrate_legacy_state_gui():
    """GUI-side migration of legacy state files into AppData.
    Safe union/merge like CLI.
    """
    legacy_dir = os.path.dirname(os.path.abspath(__file__))
    leg_log = os.path.join(legacy_dir, 'processed_files.log')
    leg_res = os.path.join(legacy_dir, 'upload_resume.json')
    # Merge logs
    try:
        legacy_set = set()
        if os.path.exists(leg_log):
            try:
                with open(leg_log, 'r', encoding='utf-8') as f:
                    legacy_set = {line.strip() for line in f if line.strip()}
            except UnicodeDecodeError:
                with open(leg_log, 'r', encoding='cp949', errors='ignore') as f:
                    legacy_set = {line.strip() for line in f if line.strip()}
        app_set = set()
        if os.path.exists(LOG_PATH):
            try:
                with open(LOG_PATH, 'r', encoding='utf-8') as f:
                    app_set = {line.strip() for line in f if line.strip()}
            except UnicodeDecodeError:
                with open(LOG_PATH, 'r', encoding='cp949', errors='ignore') as f:
                    app_set = {line.strip() for line in f if line.strip()}
        merged = app_set | legacy_set
        if merged and merged != app_set:
            with open(LOG_PATH, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sorted(merged)) + '\n')
    except Exception:
        pass
    # Merge resume
    try:
        import json
        leg = {}
        if os.path.exists(leg_res):
            try:
                with open(leg_res, 'r', encoding='utf-8') as f:
                    leg = json.load(f) or {}
            except Exception:
                leg = {}
        app = {}
        if os.path.exists(RESUME_PATH):
            try:
                with open(RESUME_PATH, 'r', encoding='utf-8') as f:
                    app = json.load(f) or {}
            except Exception:
                app = {}
        merged = dict(app)
        for k, v in leg.items():
            try:
                lv = int(v)
            except Exception:
                lv = 0
            try:
                av = int(merged.get(k, 0))
            except Exception:
                av = 0
            if lv > av:
                merged[k] = lv
        if merged != app:
            tmp = RESUME_PATH + '.tmp'
            with open(tmp, 'w', encoding='utf-8') as f:
                import json as _json
                _json.dump(merged, f, ensure_ascii=False)
            os.replace(tmp, RESUME_PATH)
    except Exception:
        pass

# Config path resolution (script-dir or %APPDATA%/ExtrusionUploader)
_CONFIG_PATH = None

def _resolve_config_paths():
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


def kst_now() -> datetime:
    return datetime.now(KST)


def load_processed() -> set:
    if not os.path.exists(LOG_PATH):
        return set()
    # Be tolerant of legacy encodings (cp949, ansi)
    try:
        with open(LOG_PATH, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f if line.strip())
    except UnicodeDecodeError:
        with open(LOG_PATH, 'r', encoding='cp949', errors='ignore') as f:
            return set(line.strip() for line in f if line.strip())


def log_processed(folder: str, filename: str):
    key = f"{folder}/{filename}"
    with open(LOG_PATH, 'a', encoding='utf-8') as f:
        f.write(key + '\n')


# --- Resume state (파일별 마지막 배치 오프셋) ---
def load_resume() -> dict:
    try:
        import json
        if os.path.exists(RESUME_PATH):
            with open(RESUME_PATH, 'r', encoding='utf-8') as f:
                return json.load(f) or {}
    except Exception:
        pass
    return {}


def save_resume(data: dict):
    try:
        import json
        tmp = RESUME_PATH + '.tmp'
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, RESUME_PATH)
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
    # Prefer explicit end-date marker (__YYYY-MM-DD), else last YYYY-MM-DD in name
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


def load_config(path: str | None = None) -> dict:
    global _CONFIG_PATH
    cfg = configparser.ConfigParser()
    # Preserve option case
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
    script_cfg, app_cfg = _resolve_config_paths()
    # Always use AppData config; if missing and script config exists, migrate it once
    chosen = path or app_cfg
    if not os.path.exists(chosen) and os.path.exists(script_cfg):
        try:
            import shutil
            shutil.copyfile(script_cfg, chosen)
        except Exception:
            pass
    if os.path.exists(chosen):
        # Tolerate BOM (utf-8-sig) and legacy encodings
        try:
            cfg.read(chosen, encoding='utf-8-sig')
        except Exception:
            with open(chosen, 'r', encoding='cp949', errors='ignore') as f:
                content = f.read()
            cfg.read_string(content if content.strip().startswith('[') else '[app]\n' + content)
        if 'app' in cfg:
            # Accept both lower/upper keys by normalizing to uppercase
            for k, v in cfg['app'].items():
                defaults[k.upper()] = v
    _CONFIG_PATH = chosen
    return defaults


def save_config(values: dict, path: str | None = None):
    global _CONFIG_PATH
    cfg = configparser.ConfigParser()
    cfg.optionxform = str
    cfg['app'] = {k: str(v) for k, v in values.items()}
    # Determine save path (use previously chosen path or AppData fallback)
    _, app_cfg = _resolve_config_paths()
    path = path or app_cfg
    _CONFIG_PATH = path
    # Use utf-8-sig for BOM tolerance across editors
    with open(path, 'w', encoding='utf-8-sig') as f:
        cfg.write(f)


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
    except Exception as e:
        print(f"PLC 변환 오류: {filename} - {e}")
        return pd.DataFrame()


def build_records_temp(file_path: str, filename: str) -> pd.DataFrame:
    try:
        try:
            df = pd.read_csv(file_path, header=0)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, header=0, encoding='cp949')
        # normalize columns (strip spaces, remove bracket chars), build lower-case map
        df.columns = df.columns.str.strip().str.replace(r'\[|\]', '', regex=True)
        lower_map = {c.lower(): c for c in df.columns}

        def pick(*cands: str) -> str | None:
            for c in cands:
                key = c.lower()
                if key in lower_map:
                    return lower_map[key]
            return None

        # Try to resolve columns flexibly (KR/EN)
        dt_col = pick('datetime', 'date_time', '날짜시간', '일시')
        date_col = pick('date', '날짜', '일자')
        time_col = pick('time', '시간', '시각')
        temp_main = pick('temperature', '온도', 'temp')

        if temp_main is None:
            raise ValueError('온도(Temperature) 컬럼을 찾을 수 없습니다')

        # Build timestamp
        if dt_col is not None:
            dt = pd.to_datetime(df[dt_col], errors='coerce')
        elif date_col is not None and time_col is not None:
            # Handle time like HH:MM:SS:ms by converting last ':' to '.'
            tstr = df[time_col].astype(str)
            # if time has 3 colons, replace last ':' with '.'
            has_ms = tstr.str.count(':') >= 3
            tconv = tstr
            if has_ms.any():
                parts = tstr.str.rsplit(':', n=1, expand=True)
                tconv = parts[0] + '.' + parts[1]
            dt = pd.to_datetime(df[date_col].astype(str) + ' ' + tconv, errors='coerce')
        else:
            raise ValueError('날짜/시간 컬럼을 찾을 수 없습니다')

        # Drop invalid rows
        out = pd.DataFrame()
        out['timestamp'] = dt.dt.strftime('%Y-%m-%dT%H:%M:%S.%f').str.rstrip('0').str.rstrip('.') + '+09:00'
        out['device_id'] = 'spot_temperature_sensor'
        # Build temperature strictly from Temperature column only
        temp_series = df[temp_main].replace('-', np.nan)
        out['temperature'] = pd.to_numeric(temp_series, errors='coerce')
        out.dropna(subset=['timestamp', 'temperature'], inplace=True)
        return out[['timestamp', 'device_id', 'temperature']]
    except Exception as e:
        print(f"온도 변환 오류: {filename} - {e}")
        return pd.DataFrame()


def edge_upload(url: str, anon_key: str, df: pd.DataFrame, logfn, progress_cb=None, start_index: int = 0, resume_key: str | None = None) -> bool:
    if df.empty:
        return True
    records = df.replace({np.nan: None}).to_dict(orient='records')
    headers = {"Authorization": f"Bearer {anon_key}", "Content-Type": "application/json"}
    total = len(records)
    processed = max(0, min(start_index, total))
    if processed > 0 and progress_cb:
        try:
            progress_cb(processed, total)
        except Exception:
            pass
    for i in range(processed, total, 500):
        batch = records[i:i+500]
        r = httpx.post(url, json=batch, headers=headers, timeout=30.0)
        if r.status_code >= 300:
            logfn(f'Edge error: {r.status_code} {r.text[:200]}')
            return False
        processed = min(i + len(batch), total)
        if resume_key:
            set_resume_offset(resume_key, processed)
        if progress_cb:
            try:
                progress_cb(processed, total)
            except Exception:
                pass
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
    d = today - timedelta(days=1)
    return datetime(d.year, d.month, d.day, tzinfo=KST)


def list_candidates(plc_dir: str, temp_dir: str, cutoff: datetime, lag_min: int, include_today: bool, check_lock: bool):
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
            items.append((temp_dir, fn, path, 'temp'))
    return items


def process_file(kind: str, path: str, filename: str) -> pd.DataFrame:
    try:
        if kind == 'plc':
            return build_records_plc(path, filename)
        elif kind == 'temp':
            return build_records_temp(path, filename)
    except Exception:
        pass
    return pd.DataFrame()


def preview_diagnostics(plc_dir: str, temp_dir: str, cutoff: datetime, lag_min: int, include_today: bool, check_lock: bool):
    included = []  # (folder, filename, path, kind)
    excluded = []  # (folder, filename, reason)
    processed = load_processed()

    # Helper to validate content
    def has_data(kind: str, path: str, filename: str) -> bool:
        df = process_file(kind, path, filename)
        return not df.empty

    # PLC
    if os.path.isdir(plc_dir):
        for fn in sorted(os.listdir(plc_dir)):
            full = os.path.join(plc_dir, fn)
            if not fn.lower().endswith('.csv'):
                excluded.append((plc_dir, fn, 'CSV 아님'))
                continue
            fdate = parse_plc_date_from_filename(fn)
            if not fdate or not within_cutoff(fdate, cutoff):
                excluded.append((plc_dir, fn, '컷오프 범위 밖'))
                continue
            if f"{plc_dir}/{fn}" in processed or fn in processed:
                excluded.append((plc_dir, fn, '이미 처리됨'))
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(full, lag_min):
                    excluded.append((plc_dir, fn, f'오늘 파일 미안정({lag_min}분 이내 변경)'))
                    continue
                if check_lock and is_locked(full):
                    excluded.append((plc_dir, fn, '파일 잠금'))
                    continue
            # content check
            if has_data('plc', full, fn):
                included.append((plc_dir, fn, full, 'plc'))
            else:
                excluded.append((plc_dir, fn, '데이터 없음'))

    # Temperature
    if os.path.isdir(temp_dir):
        for fn in sorted(os.listdir(temp_dir)):
            full = os.path.join(temp_dir, fn)
            if not fn.lower().endswith('.csv'):
                excluded.append((temp_dir, fn, 'CSV 아님'))
                continue
            fdate = parse_temp_end_date_from_filename(fn)
            if not fdate:
                try:
                    fdate = file_mtime_kst(full)
                except Exception:
                    fdate = None
            if not fdate or not within_cutoff(fdate, cutoff):
                excluded.append((temp_dir, fn, '컷오프 범위 밖'))
                continue
            if f"{temp_dir}/{fn}" in processed or fn in processed:
                excluded.append((temp_dir, fn, '이미 처리됨'))
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(full, lag_min):
                    excluded.append((temp_dir, fn, f'오늘 파일 미안정({lag_min}분 이내 변경)'))
                    continue
                if check_lock and is_locked(full):
                    excluded.append((temp_dir, fn, '파일 잠금'))
                    continue
            if has_data('temp', full, fn):
                included.append((temp_dir, fn, full, 'temp'))
            else:
                excluded.append((temp_dir, fn, '데이터 없음'))

    return included, excluded


class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title('Extrusion Uploader (Edge / Tk)')
        self.geometry('900x600')
        self.resizable(True, True)
        self.cfg = load_config()
        self.create_widgets()

    def create_widgets(self):
        pad = {'padx': 6, 'pady': 4}
        frm = ttk.Frame(self)
        frm.pack(fill='both', expand=True)

        # Settings variables
        self.var_url = tk.StringVar(value=self.cfg['SUPABASE_URL'])
        self.var_anon = tk.StringVar(value=self.cfg['SUPABASE_ANON_KEY'])
        self.var_edge = tk.StringVar(value=self.cfg['EDGE_FUNCTION_URL'])
        self.var_plc = tk.StringVar(value=self.cfg['PLC_DIR'])
        self.var_temp = tk.StringVar(value=self.cfg['TEMP_DIR'])

        # Collapsible Settings Frame
        self.settings_frame = ttk.LabelFrame(frm, text='설정')
        # Connection group
        conn = ttk.LabelFrame(self.settings_frame, text='연결 설정')
        conn.grid(row=0, column=0, columnspan=3, sticky='we', padx=6, pady=(6, 0))
        ttk.Label(conn, text='Supabase URL').grid(row=0, column=0, sticky='w', padx=4, pady=4)
        ttk.Entry(conn, textvariable=self.var_url, width=60).grid(row=0, column=1, columnspan=2, sticky='we', padx=4, pady=4)
        ttk.Label(conn, text='Anon Key').grid(row=1, column=0, sticky='w', padx=4, pady=4)
        self.entry_anon = ttk.Entry(conn, textvariable=self.var_anon, width=60, show='*')
        self.entry_anon.grid(row=1, column=1, sticky='we', padx=4, pady=4)
        self.var_show_anon = tk.BooleanVar(value=False)
        ttk.Checkbutton(conn, text='키 보기', variable=self.var_show_anon, command=self._toggle_anon_visibility).grid(row=1, column=2, sticky='w', padx=4, pady=4)
        ttk.Label(conn, text='Edge Function URL').grid(row=2, column=0, sticky='w', padx=4, pady=4)
        ttk.Entry(conn, textvariable=self.var_edge, width=60).grid(row=2, column=1, columnspan=2, sticky='we', padx=4, pady=4)
        conn.columnconfigure(1, weight=1)

        # Folders group
        fgrp = ttk.LabelFrame(self.settings_frame, text='데이터 폴더')
        fgrp.grid(row=1, column=0, columnspan=3, sticky='we', padx=6, pady=(6, 6))
        ttk.Label(fgrp, text='PLC 폴더').grid(row=0, column=0, sticky='w', padx=4, pady=4)
        ttk.Entry(fgrp, textvariable=self.var_plc, width=50).grid(row=0, column=1, sticky='we', padx=4, pady=4)
        ttk.Button(fgrp, text='찾기', command=self.pick_plc).grid(row=0, column=2, padx=4, pady=4)
        ttk.Label(fgrp, text='온도 폴더').grid(row=1, column=0, sticky='w', padx=4, pady=4)
        ttk.Entry(fgrp, textvariable=self.var_temp, width=50).grid(row=1, column=1, sticky='we', padx=4, pady=4)
        ttk.Button(fgrp, text='찾기', command=self.pick_temp).grid(row=1, column=2, padx=4, pady=4)
        fgrp.columnconfigure(1, weight=1)
        self.settings_frame.grid(row=0, column=0, columnspan=4, sticky='we', **pad)

        # Row 3: Range
        ttk.Label(frm, text='업로드 범위').grid(row=5, column=0, sticky='w', **pad)
        self.var_range = tk.StringVar(value=self.cfg['RANGE_MODE'])
        rfrm = ttk.Frame(frm)
        rfrm.grid(row=5, column=1, columnspan=3, sticky='w')
        for text, val in [('오늘까지(안정 N분 필요)', 'today'), ('어제까지', 'yesterday'), ('이틀 전까지', 'twodays'), ('사용자 지정', 'custom')]:
            ttk.Radiobutton(rfrm, text=text, value=val, variable=self.var_range).pack(side='left', padx=6)

        ttk.Label(frm, text='사용자 지정(YYYY-MM-DD)').grid(row=6, column=0, sticky='w', **pad)
        self.var_custom = tk.StringVar(value=self.cfg['CUSTOM_DATE'])
        ttk.Entry(frm, textvariable=self.var_custom, width=16).grid(row=6, column=1, sticky='w', **pad)

        ttk.Label(frm, text='안정성(마지막 수정 후 분)').grid(row=7, column=0, sticky='w', **pad)
        self.var_lag = tk.StringVar(value=str(self.cfg['MTIME_LAG_MIN']))
        ttk.Entry(frm, textvariable=self.var_lag, width=8).grid(row=7, column=1, sticky='w', **pad)
        self.var_lock = tk.BooleanVar(value=(str(self.cfg['CHECK_LOCK']).lower()=='true'))
        ttk.Checkbutton(frm, text='잠금 파일 제외(가능 시)', variable=self.var_lock).grid(row=7, column=2, sticky='w', **pad)

        # Buttons
        btnfrm = ttk.Frame(frm)
        btnfrm.grid(row=1, column=0, columnspan=4, sticky='w', **pad)
        self.btn_settings = ttk.Button(btnfrm, text='설정 닫기', command=self.toggle_settings)
        self.btn_settings.pack(side='left', padx=6)
        ttk.Button(btnfrm, text='설정 저장', command=self.on_save).pack(side='left', padx=6)
        ttk.Button(btnfrm, text='미리보기', command=self.on_preview).pack(side='left', padx=6)
        ttk.Button(btnfrm, text='업로드 시작', command=self.on_start).pack(side='left', padx=6)
        ttk.Button(btnfrm, text='종료', command=self.destroy).pack(side='left', padx=6)
        # Quick preview toggle (count-only)
        self.var_quick_preview = tk.BooleanVar(value=True)
        ttk.Checkbutton(btnfrm, text='빠른 미리보기(파일 수만)', variable=self.var_quick_preview).pack(side='left', padx=12)

        # Progress + labels + Log
        self.prog = ttk.Progressbar(frm, orient='horizontal', length=600, mode='determinate')
        self.prog.grid(row=2, column=0, columnspan=4, sticky='we', **pad)
        self.lbl_prog = ttk.Label(frm, text='진행률: 0.0% (0/0) | 현재 파일: 0.00%')
        self.lbl_prog.grid(row=3, column=0, columnspan=4, sticky='w', **pad)
        self.txt = tk.Text(frm, height=16)
        self.txt.grid(row=4, column=0, columnspan=4, sticky='nsew', **pad)

        frm.rowconfigure(4, weight=1)
        frm.columnconfigure(3, weight=1)

        # Start with settings collapsed by default for a cleaner workspace
        self.settings_frame.grid_remove()
        self.btn_settings.config(text='설정 열기')

        # Migrate legacy state once UI builds
        _migrate_legacy_state_gui()

        # Scheduler (Task Scheduler) controls
        sfrm = ttk.LabelFrame(frm, text='자동 실행 (작업 스케줄러)')
        sfrm.grid(row=5, column=0, columnspan=4, sticky='we', **pad)

        # Defaults
        self.var_sched_mode = tk.StringVar(value='Daily')  # Daily | OnLogon
        self.var_sched_time = tk.StringVar(value='01:00')
        self.var_sched_delay = tk.StringVar(value='1')
        self.var_task_name = tk.StringVar(value='Extrusion Uploader Daily')
        self.var_cli_path = tk.StringVar(value=self._find_cli_default())

        ttk.Label(sfrm, text='모드').grid(row=0, column=0, sticky='w', padx=4, pady=4)
        self.cmb_mode = ttk.Combobox(sfrm, values=['Daily','OnLogon'], textvariable=self.var_sched_mode, width=10, state='readonly')
        self.cmb_mode.grid(row=0, column=1, sticky='w', padx=4, pady=4)
        ttk.Label(sfrm, text='시작 시간(일일)').grid(row=0, column=2, sticky='e', padx=4, pady=4)
        ttk.Entry(sfrm, textvariable=self.var_sched_time, width=8).grid(row=0, column=3, sticky='w', padx=4, pady=4)
        ttk.Label(sfrm, text='지연(분, 로그온)').grid(row=0, column=4, sticky='e', padx=4, pady=4)
        ttk.Entry(sfrm, textvariable=self.var_sched_delay, width=6).grid(row=0, column=5, sticky='w', padx=4, pady=4)

        ttk.Label(sfrm, text='작업 이름').grid(row=1, column=0, sticky='w', padx=4, pady=4)
        ttk.Entry(sfrm, textvariable=self.var_task_name, width=30).grid(row=1, column=1, columnspan=2, sticky='we', padx=4, pady=4)

        ttk.Label(sfrm, text='CLI 경로').grid(row=1, column=3, sticky='e', padx=4, pady=4)
        ttk.Entry(sfrm, textvariable=self.var_cli_path, width=40).grid(row=1, column=4, sticky='we', padx=4, pady=4)
        ttk.Button(sfrm, text='찾기', command=self._pick_cli).grid(row=1, column=5, padx=4, pady=4)

        sbtn = ttk.Frame(sfrm)
        sbtn.grid(row=2, column=0, columnspan=6, sticky='w', padx=4, pady=6)
        ttk.Button(sbtn, text='등록/업데이트', command=self.on_sched_register).pack(side='left', padx=6)
        ttk.Button(sbtn, text='해지', command=self.on_sched_unregister).pack(side='left', padx=6)
        ttk.Button(sbtn, text='상태 확인', command=self.on_sched_status).pack(side='left', padx=6)

        sfrm.columnconfigure(1, weight=1)
        sfrm.columnconfigure(4, weight=1)

    def toggle_settings(self):
        if self.settings_frame.winfo_viewable():
            self.settings_frame.grid_remove()
            self.btn_settings.config(text='설정 열기')
        else:
            self.settings_frame.grid()
            self.btn_settings.config(text='설정 닫기')

    def _toggle_anon_visibility(self):
        self.entry_anon.configure(show='' if self.var_show_anon.get() else '*')

    # --- Scheduler helpers ---
    def _find_cli_default(self) -> str:
        try:
            base = os.path.dirname(os.path.abspath(__file__))
            cand = [
                os.path.join(base, 'ExtrusionUploaderCli.exe'),
                os.path.join(base, 'dist', 'ExtrusionUploaderCli.exe')
            ]
            for p in cand:
                if os.path.exists(p):
                    return p
        except Exception:
            pass
        return ''

    def _pick_cli(self):
        from tkinter import filedialog
        path = filedialog.askopenfilename(title='CLI 실행 파일 선택', filetypes=[('Executable','*.exe'),('All','*.*')])
        if path:
            self.var_cli_path.set(path)

    def _run_schtasks(self, args: list[str]) -> tuple[int,str,str]:
        try:
            proc = subprocess.run(['schtasks'] + args, capture_output=True, text=True)
            return proc.returncode, proc.stdout, proc.stderr
        except Exception as e:
            return 1, '', str(e)

    def on_sched_register(self):
        exe = self.var_cli_path.get().strip('"')
        if not exe or not os.path.exists(exe):
            self.log('CLI 경로가 유효하지 않습니다.')
            return
        name = self.var_task_name.get().strip() or 'Extrusion Uploader'
        mode = self.var_sched_mode.get()
        if mode == 'Daily':
            st = self.var_sched_time.get().strip() or '01:00'
            code, out, err = self._run_schtasks(['/Create','/TN',name,'/TR',exe,'/SC','DAILY','/ST',st,'/F'])
        else:
            try:
                delay_min = int(self.var_sched_delay.get().strip() or '1')
            except Exception:
                delay_min = 1
            delay = f"{delay_min:04d}:00"  # mm:ss with zero pad
            code, out, err = self._run_schtasks(['/Create','/TN',name,'/TR',exe,'/SC','ONLOGON','/DELAY',delay,'/F'])
        if code == 0:
            self.log(f"스케줄 등록/업데이트 완료: {name}")
        else:
            self.log(f"스케줄 등록 실패: {name} | {err or out}")

    def on_sched_unregister(self):
        name = self.var_task_name.get().strip() or 'Extrusion Uploader'
        code, out, err = self._run_schtasks(['/Delete','/TN',name,'/F'])
        if code == 0:
            self.log(f"스케줄 해지 완료: {name}")
        else:
            self.log(f"스케줄 해지 실패: {name} | {err or out}")

    def on_sched_status(self):
        name = self.var_task_name.get().strip() or 'Extrusion Uploader'
        code, out, err = self._run_schtasks(['/Query','/TN',name])
        if code == 0:
            self.log(f"스케줄 상태 OK: {name}")
            if out:
                self.log(out.splitlines()[0])
        else:
            self.log(f"스케줄 존재하지 않음 또는 오류: {name} | {err or out}")

    def pick_plc(self):
        d = filedialog.askdirectory()
        if d:
            self.var_plc.set(d)

    def pick_temp(self):
        d = filedialog.askdirectory()
        if d:
            self.var_temp.set(d)

    def log(self, msg: str, color=None):
        self.txt.insert('end', msg + '\n')
        self.txt.see('end')

    def get_values(self) -> dict:
        return {
            'SUPABASE_URL': self.var_url.get(),
            'SUPABASE_ANON_KEY': self.var_anon.get(),
            'EDGE_FUNCTION_URL': self.var_edge.get(),
            'PLC_DIR': self.var_plc.get() or 'PLC_data',
            'TEMP_DIR': self.var_temp.get() or 'Temperature_data',
            'RANGE_MODE': self.var_range.get(),
            'CUSTOM_DATE': self.var_custom.get(),
            'MTIME_LAG_MIN': self.var_lag.get() or '15',
            'CHECK_LOCK': 'true' if self.var_lock.get() else 'false',
        }

    def on_save(self):
        save_config(self.get_values())
        self.log('설정 저장 완료')

    def on_preview(self):
        vals = self.get_values()
        cutoff = compute_cutoff(vals['RANGE_MODE'], vals['CUSTOM_DATE'])
        include_today = (vals['RANGE_MODE'] == 'today')
        try:
            lag = int(vals['MTIME_LAG_MIN'])
        except Exception:
            lag = 15
        if self.var_quick_preview.get():
            items = list_candidates(vals['PLC_DIR'], vals['TEMP_DIR'], cutoff, lag, include_today, vals['CHECK_LOCK']=='true')
            self.log('미리보기(빠른): 업로드 예정 파일 목록')
            for folder, fn, _, _ in items[:200]:
                self.log(f'  - {folder}/{fn}')
            self.log(f'업로드 예정: {len(items)}개')
        else:
            inc, exc = preview_diagnostics(vals['PLC_DIR'], vals['TEMP_DIR'], cutoff, lag, include_today, vals['CHECK_LOCK']=='true')
            self.log('미리보기: 업로드 예정 파일 목록')
            for folder, fn, _, _ in inc[:200]:
                self.log(f'  - {folder}/{fn}')
            self.log(f'업로드 예정: {len(inc)}개')
            if exc:
                # summarize reasons
                summary = {}
                for folder, fn, reason in exc:
                    summary[reason] = summary.get(reason, 0) + 1
                self.log('제외 파일 요약:')
                for reason, cnt in summary.items():
                    self.log(f'  - {reason}: {cnt}개')
                self.log('예시(최대 10개):')
                for folder, fn, reason in exc[:10]:
                    self.log(f'  - {folder}/{fn} ({reason})')

    def on_start(self):
        vals = self.get_values()
        self.prog['value'] = 0
        self.txt.delete('1.0', 'end')
        threading.Thread(target=self._run_upload, args=(vals,), daemon=True).start()

    def _run_upload(self, vals: dict):
        url = vals['SUPABASE_URL'].strip()
        anon = vals['SUPABASE_ANON_KEY'].strip()
        edge = vals['EDGE_FUNCTION_URL'].strip() or (url.rstrip('/') + '/functions/v1/upload-metrics')
        cutoff = compute_cutoff(vals['RANGE_MODE'], vals['CUSTOM_DATE'])
        include_today = (vals['RANGE_MODE'] == 'today')
        try:
            lag = int(vals['MTIME_LAG_MIN'])
        except Exception:
            lag = 15
        check_lock = (vals['CHECK_LOCK'] == 'true')

        items = list_candidates(vals['PLC_DIR'], vals['TEMP_DIR'], cutoff, lag, include_today, check_lock)
        self.log(f'대상 파일: {len(items)}개')
        if not items:
            return

        total_files = len(items)
        self.prog['maximum'] = total_files
        count = 0
        for folder, fn, path, kind in items:
            key = f'{folder}/{fn}'
            start_idx = get_resume_offset(key)
            if start_idx > 0:
                self.log(f'업로드 재개: {folder}/{fn} (재개 지점 {start_idx}행)')
            else:
                self.log(f'업로드 시작: {folder}/{fn}')
            df = build_records_plc(path, fn) if kind == 'plc' else build_records_temp(path, fn)
            def per_file_cb(done, total):
                pct = (done/total*100.0) if total else 100.0
                overall_pct = ((count + (done/total if total else 0))/total_files*100.0) if total_files else 100.0
                self.lbl_prog.config(text=f'진행률: {overall_pct:0.1f}% ({count}/{total_files}) | 현재 파일: {pct:0.2f}%')
                self.update_idletasks()
            ok = edge_upload(edge, anon, df, self.log, progress_cb=per_file_cb, start_index=start_idx, resume_key=key)
            if ok:
                log_processed(folder, fn)
                # 성공 시 재개 지점 제거
                set_resume_offset(key, 0)
                count += 1
                self.prog['value'] = count
                overall_pct = (count/total_files*100.0) if total_files else 100.0
                self.lbl_prog.config(text=f'진행률: {overall_pct:0.1f}% ({count}/{total_files}) | 현재 파일: 100.00%')
                self.update_idletasks()
            else:
                self.log(f'업로드 실패: {folder}/{fn}')
        self.log(f'완료: {count}개 파일 업로드')


if __name__ == '__main__':
    App().mainloop()
