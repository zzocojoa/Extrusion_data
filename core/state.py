import json
import os
import threading
from typing import Any, Dict, Set, TypedDict

from .config import get_data_dir


MANIFEST_FILENAME = "state_manifest.json"
LOG_FILENAME = "processed_files.log"
RESUME_FILENAME = "upload_resume.json"
MANIFEST_VERSION = 1

_file_lock = threading.RLock()


class StateManifest(TypedDict):
    version: int
    processed: list[str]
    resume: dict[str, int]


def build_legacy_file_key(folder: str, filename: str) -> str:
    return f"{folder}/{filename}"


def build_file_state_key(folder: str, filename: str, file_path: str) -> str:
    legacy_key = build_legacy_file_key(folder, filename)
    if file_path.strip() == "":
        return legacy_key
    try:
        stat_result = os.stat(file_path)
    except OSError:
        return legacy_key
    return f"{legacy_key}|size={stat_result.st_size}|mtime_ns={stat_result.st_mtime_ns}"


def build_file_state_lookup_keys(folder: str, filename: str, file_path: str) -> tuple[str, ...]:
    current_key = build_file_state_key(folder, filename, file_path)
    legacy_key = build_legacy_file_key(folder, filename)
    if current_key == legacy_key:
        return current_key, filename
    return current_key, legacy_key, filename


def _default_manifest() -> StateManifest:
    return {
        "version": MANIFEST_VERSION,
        "processed": [],
        "resume": {},
    }


def _normalize_legacy_key(key: str) -> str:
    if "|" not in key:
        return key
    return key.split("|", 1)[0]


def _expand_processed_key(key: str) -> Set[str]:
    normalized = key.strip()
    if normalized == "":
        return set()

    expanded: Set[str] = {normalized}
    legacy_key = _normalize_legacy_key(normalized)
    if legacy_key != normalized:
        expanded.add(legacy_key)
        expanded.add(os.path.basename(legacy_key))
    else:
        expanded.add(os.path.basename(normalized))
    return expanded


def _normalize_processed_set(values: Any) -> Set[str]:
    if not isinstance(values, (list, set, tuple)):
        return set()

    processed: Set[str] = set()
    for value in values:
        key = str(value).strip()
        if key != "":
            processed.add(key)
    return processed


def _normalize_resume_map(values: Any) -> Dict[str, int]:
    if not isinstance(values, dict):
        return {}

    resume: Dict[str, int] = {}
    for raw_key, raw_value in values.items():
        key = str(raw_key).strip()
        if key == "":
            continue
        try:
            offset = int(raw_value)
        except Exception:
            continue
        if offset > 0:
            resume[key] = offset
    return resume


def _merge_resume_maps(base: Dict[str, int], extra: Dict[str, int]) -> Dict[str, int]:
    merged: Dict[str, int] = dict(base)
    for key, value in extra.items():
        current_value = merged.get(key, 0)
        if value > current_value:
            merged[key] = value
    return merged


def _load_json_object(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as file_handle:
            raw_value = json.load(file_handle)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    if isinstance(raw_value, dict):
        return raw_value
    return {}


def _load_legacy_processed_keys(path: str) -> Set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as file_handle:
            return {line.strip() for line in file_handle if line.strip()}
    except UnicodeDecodeError:
        with open(path, "r", encoding="cp949", errors="ignore") as file_handle:
            return {line.strip() for line in file_handle if line.strip()}
    except Exception:
        return set()


def _load_legacy_resume_map(path: str) -> Dict[str, int]:
    raw_data = _load_json_object(path)
    return _normalize_resume_map(raw_data)


def _normalize_manifest(raw_manifest: Any) -> StateManifest:
    manifest = _default_manifest()
    if not isinstance(raw_manifest, dict):
        return manifest

    version_value = raw_manifest.get("version", MANIFEST_VERSION)
    try:
        manifest["version"] = int(version_value)
    except Exception:
        manifest["version"] = MANIFEST_VERSION

    manifest["processed"] = sorted(_normalize_processed_set(raw_manifest.get("processed", [])))
    manifest["resume"] = _normalize_resume_map(raw_manifest.get("resume", {}))
    return manifest


def _atomic_write_text(path: str, content: str) -> None:
    parent_dir = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(parent_dir, exist_ok=True)
    temp_path = path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as file_handle:
        file_handle.write(content)
    os.replace(temp_path, path)


def _atomic_write_json(path: str, data: dict[str, Any]) -> None:
    _atomic_write_text(path, json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True))


def _write_legacy_processed_keys(path: str, keys: Set[str]) -> None:
    processed_text = "\n".join(sorted(keys))
    if processed_text != "":
        processed_text += "\n"
    _atomic_write_text(path, processed_text)


def _save_state_snapshot(
    manifest_path: str,
    legacy_log_path: str,
    legacy_resume_path: str,
    manifest: StateManifest,
) -> None:
    normalized_manifest = _normalize_manifest(manifest)
    _atomic_write_json(manifest_path, normalized_manifest)
    _write_legacy_processed_keys(legacy_log_path, set(normalized_manifest["processed"]))
    _atomic_write_json(legacy_resume_path, normalized_manifest["resume"])


def _load_manifest_only(manifest_path: str) -> StateManifest:
    if not os.path.exists(manifest_path):
        return _default_manifest()
    return _normalize_manifest(_load_json_object(manifest_path))


def _materialize_manifest_from_legacy(
    manifest_path: str,
    legacy_log_path: str,
    legacy_resume_path: str,
) -> StateManifest:
    if os.path.exists(manifest_path):
        return _load_manifest_only(manifest_path)

    manifest = _default_manifest()
    processed_keys = _load_legacy_processed_keys(legacy_log_path)
    resume_map = _load_legacy_resume_map(legacy_resume_path)
    manifest["processed"] = sorted(processed_keys)
    manifest["resume"] = resume_map

    if processed_keys or resume_map:
        _save_state_snapshot(manifest_path, legacy_log_path, legacy_resume_path, manifest)
    return manifest


def _load_state_snapshot(
    manifest_path: str,
    legacy_log_path: str,
    legacy_resume_path: str,
) -> StateManifest:
    return _materialize_manifest_from_legacy(manifest_path, legacy_log_path, legacy_resume_path)


def get_manifest_path(path: str | None = None) -> str:
    """
    상태 manifest 파일 경로를 반환합니다.
    """
    if path:
        return os.path.join(os.path.dirname(os.path.abspath(path)), MANIFEST_FILENAME)
    return os.path.join(get_data_dir(), MANIFEST_FILENAME)


def get_log_path(path: str | None = None) -> str:
    """
    processed_files.log 호환 경로를 반환합니다.
    """
    if path:
        return path
    return os.path.join(get_data_dir(), LOG_FILENAME)


def get_resume_path(path: str | None = None) -> str:
    """
    upload_resume.json 호환 경로를 반환합니다.
    """
    if path:
        return path
    return os.path.join(get_data_dir(), RESUME_FILENAME)


def load_processed(path: str | None = None) -> Set[str]:
    """
    처리 완료 파일 키를 읽습니다.
    """
    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _file_lock:
        manifest = _load_state_snapshot(manifest_path, log_path, resume_path)
        processed_keys = set(manifest["processed"])
        for key in list(processed_keys):
            processed_keys.update(_expand_processed_key(key))
        return processed_keys


def log_processed(folder: str, filename: str, file_path: str, path: str | None = None) -> None:
    """
    처리 완료 파일 키를 저장합니다.
    """
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)
    manifest_path = get_manifest_path(path)
    key = build_file_state_key(folder, filename, file_path)

    with _file_lock:
        manifest = _load_state_snapshot(manifest_path, log_path, resume_path)
        processed_keys = set(manifest["processed"])
        processed_keys.add(key)
        manifest["processed"] = sorted(processed_keys)
        _save_state_snapshot(manifest_path, log_path, resume_path, manifest)


def load_resume(path: str | None = None) -> Dict[str, int]:
    """
    재개 오프셋을 읽습니다.
    """
    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _file_lock:
        manifest = _load_state_snapshot(manifest_path, log_path, resume_path)
        return dict(manifest["resume"])


def save_resume(data: Dict[str, int], path: str | None = None) -> None:
    """
    재개 오프셋 전체를 저장합니다.
    """
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)
    manifest_path = get_manifest_path(path)

    with _file_lock:
        manifest = _load_state_snapshot(manifest_path, log_path, resume_path)
        manifest["resume"] = _normalize_resume_map(data)
        _save_state_snapshot(manifest_path, log_path, resume_path, manifest)


def set_resume_offset(key: str, offset: int, path: str | None = None) -> None:
    """
    단일 재개 오프셋을 저장합니다.
    """
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)
    manifest_path = get_manifest_path(path)

    with _file_lock:
        manifest = _load_state_snapshot(manifest_path, log_path, resume_path)
        resume_map = dict(manifest["resume"])
        legacy_key = _normalize_legacy_key(key)
        if offset <= 0:
            resume_map.pop(key, None)
            resume_map.pop(legacy_key, None)
        else:
            resume_map[key] = int(offset)
            if legacy_key != key:
                resume_map.pop(legacy_key, None)
        manifest["resume"] = resume_map
        _save_state_snapshot(manifest_path, log_path, resume_path, manifest)


def get_resume_offset(key: str, path: str | None = None) -> int:
    """
    저장된 재개 오프셋을 반환합니다.
    """
    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _file_lock:
        manifest = _load_state_snapshot(manifest_path, log_path, resume_path)
        resume_map = manifest["resume"]
        legacy_key = _normalize_legacy_key(key)
        candidates = [key]
        if legacy_key not in candidates:
            candidates.append(legacy_key)
        basename_key = os.path.basename(legacy_key)
        if basename_key not in candidates:
            candidates.append(basename_key)

        for candidate in candidates:
            try:
                value = int(resume_map.get(candidate, 0))
            except Exception:
                value = 0
            if value > 0:
                return value
        return 0


def migrate_legacy_state(script_dir: str | None = None) -> None:
    """
    기존 상태 파일을 manifest 기준 구조로 이관합니다.
    """
    if script_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))

    manifest_path = get_manifest_path()
    app_log_path = get_log_path()
    app_resume_path = get_resume_path()
    legacy_log_path = os.path.join(script_dir, LOG_FILENAME)
    legacy_resume_path = os.path.join(script_dir, RESUME_FILENAME)

    with _file_lock:
        manifest = _load_manifest_only(manifest_path)
        if not manifest["processed"] and not manifest["resume"]:
            manifest = _default_manifest()

        processed_keys = set(manifest["processed"])
        processed_keys.update(_load_legacy_processed_keys(app_log_path))
        processed_keys.update(_load_legacy_processed_keys(legacy_log_path))

        resume_map = dict(manifest["resume"])
        resume_map = _merge_resume_maps(resume_map, _load_legacy_resume_map(app_resume_path))
        resume_map = _merge_resume_maps(resume_map, _load_legacy_resume_map(legacy_resume_path))

        manifest["processed"] = sorted(processed_keys)
        manifest["resume"] = resume_map
        _save_state_snapshot(manifest_path, app_log_path, app_resume_path, manifest)
