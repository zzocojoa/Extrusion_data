import os
import sys
import re
import argparse
import json
from datetime import datetime, timedelta, timezone
import httpx
import pandas as pd

from core.config import (
    get_data_dir,
    load_config,
    compute_edge_url,
    validate_config,
)
from core.transform import (
    build_records_plc as core_build_records_plc,
    build_records_temp as core_build_records_temp,
)
from core import state as core_state
from core import files as core_files
from core import upload as core_upload

KST = timezone(timedelta(hours=9))

# Data directory for logs/state (AppData)
DATA_DIR = get_data_dir()
LOG_PATH = os.path.join(DATA_DIR, 'processed_files.log')
RESUME_PATH = os.path.join(DATA_DIR, 'upload_resume.json')


def log(msg: str, level: str = "INFO") -> None:
    ts = kst_now().isoformat(timespec="seconds")
    print(f"[{level.upper()} {ts}] {msg}")


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
    ok_cfg, missing_keys = validate_config(cfg)
    if not ok_cfg:
        log(f"환경 설정 누락/불완전: {', '.join(missing_keys)}", level="ERROR")
        return 2
    supabase_url = cfg.get('SUPABASE_URL')
    anon_key = cfg.get('SUPABASE_ANON_KEY')
    edge_url = compute_edge_url(cfg)

    plc_dir = args.plc_dir or cfg['PLC_DIR']
    temp_dir = args.temp_dir or cfg['TEMP_DIR']
    mode = args.range_mode or cfg['RANGE_MODE']
    custom = (args.custom_date if args.custom_date is not None else cfg['CUSTOM_DATE'])
    lag = (args.lag if args.lag is not None else int(str(cfg['MTIME_LAG_MIN'])))
    check_lock = args.check_lock if args.check_lock is not None else (str(cfg['CHECK_LOCK']).lower() == 'true')

    cutoff = compute_cutoff(mode, custom)
    include_today = (mode == 'today')

    log('===== 업로드 시작 (CLI) =====')
    log(f'Config: {cfg_path}')
    log(f'범위: {mode} {custom or ""}')
    log(f'폴더: {plc_dir} | {temp_dir}')

    items = list_candidates(plc_dir, temp_dir, cutoff, lag, include_today, check_lock, quick=args.quick)
    log(f'대상 파일: {len(items)}개')
    if not items:
        return 0

    ok_all = True
    done = 0
    for folder, fn, path, kind in items:
        log(f'- 업로드 {folder}/{fn}')
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

    log(f'완료: {done}/{len(items)}개')
    return 0 if ok_all else 1


# Override local implementations with shared core.transform versions
def build_records_plc(file_path: str, filename: str) -> pd.DataFrame:
    return core_build_records_plc(file_path, filename)


def build_records_temp(file_path: str, filename: str) -> pd.DataFrame:
    return core_build_records_temp(file_path, filename)


if __name__ == '__main__':
    raise SystemExit(main())
