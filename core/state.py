import json
import os
import threading
from typing import Dict, Set

from .config import get_data_dir


LOG_FILENAME = "processed_files.log"
RESUME_FILENAME = "upload_resume.json"

# Global lock for file operations
_file_lock = threading.Lock()


def get_log_path(path: str | None = None) -> str:
    """
    Return path to processed_files.log (AppData by default).
    """
    if path:
        return path
    data_dir = get_data_dir()
    return os.path.join(data_dir, LOG_FILENAME)


def get_resume_path(path: str | None = None) -> str:
    """
    Return path to upload_resume.json (AppData by default).
    """
    if path:
        return path
    data_dir = get_data_dir()
    return os.path.join(data_dir, RESUME_FILENAME)


def load_processed(path: str | None = None) -> Set[str]:
    """
    Load processed file keys from log file.
    """
    log_path = get_log_path(path)
    if not os.path.exists(log_path):
        return set()
    
    # Read is safe enough without lock usually, but for strict consistency we can lock
    # However, to avoid contention on heavy reads, we might skip lock for read-only if append-only is atomic enough.
    # But let's lock to be safe.
    with _file_lock:
        try:
            with open(log_path, "r", encoding="utf-8") as f:
                return {line.strip() for line in f if line.strip()}
        except UnicodeDecodeError:
            with open(log_path, "r", encoding="cp949", errors="ignore") as f:
                return {line.strip() for line in f if line.strip()}
        except Exception:
            return set()


def log_processed(folder: str, filename: str, path: str | None = None) -> None:
    """
    Append a processed file key ("folder/filename") to the log.
    Thread-safe.
    """
    log_path = get_log_path(path)
    key = f"{folder}/{filename}"
    
    with _file_lock:
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(key + "\n")


def load_resume(path: str | None = None) -> Dict[str, int]:
    """
    Load resume offsets from JSON file.
    """
    resume_path = get_resume_path(path)
    if not os.path.exists(resume_path):
        return {}
    
    with _file_lock:
        try:
            with open(resume_path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
        except Exception:
            return {}
            
    # Normalize to int values
    out: Dict[str, int] = {}
    for k, v in data.items():
        try:
            out[k] = int(v)
        except Exception:
            continue
    return out


def save_resume(data: Dict[str, int], path: str | None = None) -> None:
    """
    Atomically save resume offsets to JSON file.
    Thread-safe.
    """
    resume_path = get_resume_path(path)
    
    with _file_lock:
        os.makedirs(os.path.dirname(resume_path), exist_ok=True)
        tmp = resume_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, resume_path)


def set_resume_offset(key: str, offset: int, path: str | None = None) -> None:
    # Note: This read-modify-write cycle needs to be atomic.
    # Since load_resume and save_resume are locked individually, we need a lock around the whole operation here.
    # But re-entrant lock (RLock) would be needed if we reuse the same lock.
    # Or we just implement the logic inside the lock here.
    
    resume_path = get_resume_path(path)
    
    with _file_lock:
        # Load inside lock
        if os.path.exists(resume_path):
            try:
                with open(resume_path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
            except Exception:
                data = {}
        else:
            data = {}

        # Modify
        if offset <= 0:
            if key in data:
                del data[key]
        else:
            data[key] = int(offset)
            
        # Save inside lock
        os.makedirs(os.path.dirname(resume_path), exist_ok=True)
        tmp = resume_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False)
        os.replace(tmp, resume_path)


def get_resume_offset(key: str, path: str | None = None) -> int:
    data = load_resume(path)
    try:
        return int(data.get(key, 0))
    except Exception:
        return 0


def migrate_legacy_state(script_dir: str | None = None) -> None:
    """
    Merge legacy state files from script directory into AppData paths.
    - processed_files.log: union of lines
    - upload_resume.json: union of keys, taking max offset per key
    """
    if script_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = get_data_dir()

    leg_log = os.path.join(script_dir, LOG_FILENAME)
    leg_res = os.path.join(script_dir, RESUME_FILENAME)
    app_log = get_log_path()
    app_res = get_resume_path()

    # Merge logs
    try:
        legacy_set: Set[str] = set()
        if os.path.exists(leg_log):
            try:
                with open(leg_log, "r", encoding="utf-8") as f:
                    legacy_set = {line.strip() for line in f if line.strip()}
            except UnicodeDecodeError:
                with open(leg_log, "r", encoding="cp949", errors="ignore") as f:
                    legacy_set = {line.strip() for line in f if line.strip()}
        app_set: Set[str] = load_processed(app_log)
        merged = app_set | legacy_set
        if merged and merged != app_set:
            os.makedirs(os.path.dirname(app_log), exist_ok=True)
            with open(app_log, "w", encoding="utf-8") as f:
                f.write("\n".join(sorted(merged)) + "\n")
    except Exception:
        pass

    # Merge resume
    try:
        leg = load_resume(leg_res)
        app = load_resume(app_res)
        merged_dict: Dict[str, int] = dict(app)
        for k, v in leg.items():
            try:
                lv = int(v)
            except Exception:
                lv = 0
            try:
                av = int(merged_dict.get(k, 0))
            except Exception:
                av = 0
            if lv > av:
                merged_dict[k] = lv
        if merged_dict != app:
            save_resume(merged_dict, app_res)
    except Exception:
        pass

