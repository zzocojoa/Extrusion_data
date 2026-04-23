import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Iterator
from typing import Any, Dict, NotRequired, Set, TypedDict, cast

from .config import get_data_dir
from . import state_db


MANIFEST_FILENAME = "state_manifest.json"
LOG_FILENAME = "processed_files.log"
RESUME_FILENAME = "upload_resume.json"
MANIFEST_VERSION = 1
STATE_LOCK_SUFFIX = ".lock"
STATE_LOCK_TIMEOUT_SECONDS = 10.0
STATE_LOCK_POLL_SECONDS = 0.05
STATE_LOCK_RELEASE_TIMEOUT_SECONDS = 1.0
SQLITE_READ_MODE_ENV_NAME = "EXTRUSION_STATE_DB_READ_MODE"

_file_lock = threading.RLock()

StateDbError = state_db.StateDbError
StateDbCorruptionError = state_db.StateDbCorruptionError
StateDbParityError = state_db.StateDbParityError
StateDbImportError = state_db.StateDbImportError


class StateManifest(TypedDict):
    version: int
    processed: list[str]
    resume: dict[str, int]
    recent_successful_upload_profile: NotRequired["RecentSuccessfulUploadProfile"]
    last_failed_retry_state: NotRequired["LastFailedRetryState"]


class RecentSuccessfulUploadProfile(TypedDict):
    profile_name: str
    applied_at: NotRequired[float]
    values: NotRequired[dict[str, str]]


class LastFailedRetryState(TypedDict):
    file_key: str
    offset: int
    retry_count: NotRequired[int]
    failed_at: NotRequired[float]
    error_message: NotRequired[str]


class FailedRetryEntry(TypedDict):
    file_key: str
    folder: str
    filename: str
    legacy_key: str
    resume_offset: int
    retry_count: int
    failed_at: float
    error_message: NotRequired[str]


class StateManifestEnvelope(TypedDict):
    manifest: StateManifest
    extra_fields: dict[str, Any]


class UploadDashboardStateSnapshot(TypedDict):
    resume: dict[str, int]
    recent_successful_upload_profile: RecentSuccessfulUploadProfile | None
    failed_retry_set: tuple[FailedRetryEntry, ...]


class StateHealthSnapshot(TypedDict):
    state: str
    read_mode: str
    can_start_upload: bool
    pending_resume_count: int
    failed_retry_count: int
    recovery_action_required: bool
    summary_code: str
    detail_codes: tuple[str, ...]
    error_message: NotRequired[str]
    backup_dir: NotRequired[str]
    maintenance_source: NotRequired[str]


def build_legacy_file_key(folder: str, filename: str) -> str:
    return f"{folder}/{filename}"


def _split_legacy_key(legacy_key: str) -> tuple[str, str]:
    normalized = legacy_key.replace("\\", "/")
    if "/" not in normalized:
        return "", normalized
    folder, filename = normalized.rsplit("/", 1)
    return folder, filename


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
        return (current_key,)
    return current_key, legacy_key


def _default_manifest() -> StateManifest:
    return {
        "version": MANIFEST_VERSION,
        "processed": [],
        "resume": {},
    }


def _manifest_known_keys() -> set[str]:
    return {
        "version",
        "processed",
        "resume",
        "recent_successful_upload_profile",
        "last_failed_retry_state",
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


def _normalize_non_empty_string(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if normalized == "":
        return None
    return normalized


def _normalize_positive_float(value: Any) -> float | None:
    try:
        normalized = float(value)
    except Exception:
        return None
    if normalized <= 0:
        return None
    return normalized


def _normalize_positive_int(value: Any) -> int | None:
    try:
        normalized = int(value)
    except Exception:
        return None
    if normalized <= 0:
        return None
    return normalized


def _normalize_string_map(values: Any) -> dict[str, str] | None:
    if not isinstance(values, dict):
        return None

    normalized_values: dict[str, str] = {}
    for raw_key, raw_value in values.items():
        key = _normalize_non_empty_string(raw_key)
        value = _normalize_non_empty_string(raw_value)
        if key is None or value is None:
            continue
        normalized_values[key] = value
    return normalized_values


def _clone_recent_successful_upload_profile(
    profile: RecentSuccessfulUploadProfile,
) -> RecentSuccessfulUploadProfile:
    cloned_profile: dict[str, Any] = dict(profile)
    values = profile.get("values")
    if isinstance(values, dict):
        cloned_profile["values"] = dict(values)
    return cast(RecentSuccessfulUploadProfile, cloned_profile)


def _clone_last_failed_retry_state(state: LastFailedRetryState) -> LastFailedRetryState:
    return cast(LastFailedRetryState, dict(state))


def _normalize_recent_successful_upload_profile(values: Any) -> RecentSuccessfulUploadProfile | None:
    if not isinstance(values, dict):
        return None

    profile_name = _normalize_non_empty_string(values.get("profile_name"))
    if profile_name is None:
        return None

    normalized_profile: dict[str, Any] = dict(values)
    normalized_profile["profile_name"] = profile_name

    applied_at = _normalize_positive_float(values.get("applied_at"))
    if applied_at is None:
        normalized_profile.pop("applied_at", None)
    else:
        normalized_profile["applied_at"] = applied_at

    profile_values = _normalize_string_map(values.get("values"))
    if profile_values is None:
        normalized_profile.pop("values", None)
    else:
        normalized_profile["values"] = profile_values

    return cast(RecentSuccessfulUploadProfile, normalized_profile)


def _normalize_last_failed_retry_state(values: Any) -> LastFailedRetryState | None:
    if not isinstance(values, dict):
        return None

    file_key = _normalize_non_empty_string(values.get("file_key"))
    offset = _normalize_positive_int(values.get("offset"))
    if file_key is None or offset is None:
        return None

    normalized_state: dict[str, Any] = dict(values)
    normalized_state["file_key"] = file_key
    normalized_state["offset"] = offset

    retry_count = _normalize_positive_int(values.get("retry_count"))
    if retry_count is None:
        normalized_state.pop("retry_count", None)
    else:
        normalized_state["retry_count"] = retry_count

    failed_at = _normalize_positive_float(values.get("failed_at"))
    if failed_at is None:
        normalized_state.pop("failed_at", None)
    else:
        normalized_state["failed_at"] = failed_at

    error_message = _normalize_non_empty_string(values.get("error_message"))
    if error_message is None:
        normalized_state.pop("error_message", None)
    else:
        normalized_state["error_message"] = error_message

    return cast(LastFailedRetryState, normalized_state)


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


def _normalize_manifest_data(raw_manifest: Any) -> StateManifest:
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
    recent_successful_upload_profile = _normalize_recent_successful_upload_profile(
        raw_manifest.get("recent_successful_upload_profile")
    )
    if recent_successful_upload_profile is not None:
        manifest["recent_successful_upload_profile"] = recent_successful_upload_profile

    last_failed_retry_state = _normalize_last_failed_retry_state(raw_manifest.get("last_failed_retry_state"))
    if last_failed_retry_state is not None:
        manifest["last_failed_retry_state"] = last_failed_retry_state
    return manifest


def _extract_manifest_extra_fields(raw_manifest: dict[str, Any]) -> dict[str, Any]:
    known_keys = _manifest_known_keys()
    extra_fields: dict[str, Any] = {}
    for key, value in raw_manifest.items():
        if key in known_keys:
            continue
        extra_fields[key] = value
    return extra_fields


def _normalize_manifest_extra_fields(raw_extra_fields: Any) -> dict[str, Any]:
    if not isinstance(raw_extra_fields, dict):
        return {}
    return _extract_manifest_extra_fields(raw_extra_fields)


def _normalize_manifest(raw_manifest: Any) -> StateManifestEnvelope:
    if not isinstance(raw_manifest, dict):
        return {
            "manifest": _default_manifest(),
            "extra_fields": {},
        }
    return {
        "manifest": _normalize_manifest_data(raw_manifest),
        "extra_fields": _extract_manifest_extra_fields(raw_manifest),
    }


def _normalize_manifest_envelope(envelope: StateManifestEnvelope) -> StateManifestEnvelope:
    return {
        "manifest": _normalize_manifest_data(envelope["manifest"]),
        "extra_fields": _normalize_manifest_extra_fields(envelope["extra_fields"]),
    }


def _serialize_manifest(envelope: StateManifestEnvelope) -> dict[str, Any]:
    manifest = _normalize_manifest_data(envelope["manifest"])
    serialized_manifest: dict[str, Any] = dict(envelope["extra_fields"])
    serialized_manifest["version"] = manifest["version"]
    serialized_manifest["processed"] = list(manifest["processed"])
    serialized_manifest["resume"] = dict(manifest["resume"])

    recent_successful_upload_profile = manifest.get("recent_successful_upload_profile")
    if recent_successful_upload_profile is not None:
        serialized_manifest["recent_successful_upload_profile"] = _clone_recent_successful_upload_profile(
            recent_successful_upload_profile
        )

    last_failed_retry_state = manifest.get("last_failed_retry_state")
    if last_failed_retry_state is not None:
        serialized_manifest["last_failed_retry_state"] = _clone_last_failed_retry_state(last_failed_retry_state)

    return serialized_manifest


def _atomic_write_text(path: str, content: str) -> None:
    parent_dir = os.path.dirname(os.path.abspath(path)) or "."
    os.makedirs(parent_dir, exist_ok=True)
    temp_path = path + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as file_handle:
        file_handle.write(content)
    os.replace(temp_path, path)


def _is_pid_active(pid: int) -> bool:
    if pid <= 0:
        return False
    if os.name == "nt":
        import ctypes

        ERROR_ACCESS_DENIED = 5
        ERROR_INVALID_PARAMETER = 87
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        SYNCHRONIZE = 0x00100000
        WAIT_OBJECT_0 = 0x00000000
        WAIT_TIMEOUT = 0x00000102

        kernel32 = ctypes.windll.kernel32
        kernel32.OpenProcess.argtypes = [ctypes.c_uint32, ctypes.c_int, ctypes.c_uint32]
        kernel32.OpenProcess.restype = ctypes.c_void_p
        kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel32.WaitForSingleObject.restype = ctypes.c_uint32
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_int

        process_handle = kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION | SYNCHRONIZE,
            0,
            pid,
        )
        if process_handle in {None, 0}:
            error_code = ctypes.GetLastError()
            if error_code == ERROR_ACCESS_DENIED:
                return True
            if error_code == ERROR_INVALID_PARAMETER:
                return False
            return False
        try:
            wait_result = kernel32.WaitForSingleObject(process_handle, 0)
            if wait_result == WAIT_TIMEOUT:
                return True
            if wait_result == WAIT_OBJECT_0:
                return False
            return True
        finally:
            kernel32.CloseHandle(process_handle)
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return True
    return True


def _read_lock_pid(lock_path: str) -> int | None:
    try:
        with open(lock_path, "r", encoding="utf-8") as file_handle:
            raw_value = file_handle.read().strip()
    except FileNotFoundError:
        return None
    except OSError:
        return None
    if raw_value == "":
        return None
    try:
        return int(raw_value)
    except ValueError:
        return None


def _remove_lock_file(lock_path: str) -> None:
    deadline = time.monotonic() + STATE_LOCK_RELEASE_TIMEOUT_SECONDS
    while True:
        try:
            os.remove(lock_path)
            return
        except FileNotFoundError:
            return
        except PermissionError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(STATE_LOCK_POLL_SECONDS)
        except OSError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(STATE_LOCK_POLL_SECONDS)


@contextmanager
def _process_file_lock(lock_path: str) -> Iterator[None]:
    parent_dir = os.path.dirname(os.path.abspath(lock_path)) or "."
    os.makedirs(parent_dir, exist_ok=True)
    deadline = time.monotonic() + STATE_LOCK_TIMEOUT_SECONDS
    file_descriptor: int | None = None

    while file_descriptor is None:
        try:
            file_descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        except FileExistsError:
            lock_pid = _read_lock_pid(lock_path)
            if lock_pid is not None and not _is_pid_active(lock_pid):
                try:
                    _remove_lock_file(lock_path)
                    continue
                except FileNotFoundError:
                    continue
                except OSError:
                    pass
            if time.monotonic() >= deadline:
                raise TimeoutError(f"State lock timed out: {lock_path}")
            time.sleep(STATE_LOCK_POLL_SECONDS)
        except OSError as error:
            raise OSError(f"Failed to acquire state lock: {lock_path}") from error

    try:
        os.write(file_descriptor, str(os.getpid()).encode("utf-8"))
        yield
    finally:
        os.close(file_descriptor)
        _remove_lock_file(lock_path)


@contextmanager
def _state_guard(manifest_path: str) -> Iterator[None]:
    lock_path = manifest_path + STATE_LOCK_SUFFIX
    with _file_lock:
        with _process_file_lock(lock_path):
            yield


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
    envelope: StateManifestEnvelope,
) -> None:
    normalized_envelope = _normalize_manifest_envelope(envelope)
    normalized_manifest = normalized_envelope["manifest"]
    _atomic_write_json(manifest_path, _serialize_manifest(normalized_envelope))
    _write_legacy_processed_keys(legacy_log_path, set(normalized_manifest["processed"]))
    _atomic_write_json(legacy_resume_path, normalized_manifest["resume"])


def _save_resume_snapshot(
    manifest_path: str,
    legacy_resume_path: str,
    envelope: StateManifestEnvelope,
) -> None:
    normalized_envelope = _normalize_manifest_envelope(envelope)
    normalized_manifest = normalized_envelope["manifest"]
    _atomic_write_json(manifest_path, _serialize_manifest(normalized_envelope))
    _atomic_write_json(legacy_resume_path, normalized_manifest["resume"])


def _load_manifest_only(manifest_path: str) -> StateManifestEnvelope:
    if not os.path.exists(manifest_path):
        return {
            "manifest": _default_manifest(),
            "extra_fields": {},
        }
    return _normalize_manifest(_load_json_object(manifest_path))


def _materialize_manifest_from_legacy(
    manifest_path: str,
    legacy_log_path: str,
    legacy_resume_path: str,
    include_processed_snapshot: bool,
) -> StateManifestEnvelope:
    if os.path.exists(manifest_path):
        return _load_manifest_only(manifest_path)

    envelope: StateManifestEnvelope = {
        "manifest": _default_manifest(),
        "extra_fields": {},
    }
    manifest = envelope["manifest"]
    processed_keys = _load_legacy_processed_keys(legacy_log_path)
    resume_map = _load_legacy_resume_map(legacy_resume_path)
    manifest["processed"] = sorted(processed_keys)
    manifest["resume"] = resume_map

    if processed_keys or resume_map:
        if include_processed_snapshot:
            _save_state_snapshot(manifest_path, legacy_log_path, legacy_resume_path, envelope)
        else:
            _save_resume_snapshot(manifest_path, legacy_resume_path, envelope)
    return envelope


def _load_state_snapshot(
    manifest_path: str,
    legacy_log_path: str,
    legacy_resume_path: str,
) -> StateManifestEnvelope:
    return _materialize_manifest_from_legacy(
        manifest_path,
        legacy_log_path,
        legacy_resume_path,
        include_processed_snapshot=True,
    )


def _load_resume_state_snapshot(
    manifest_path: str,
    legacy_log_path: str,
    legacy_resume_path: str,
) -> StateManifestEnvelope:
    return _materialize_manifest_from_legacy(
        manifest_path,
        legacy_log_path,
        legacy_resume_path,
        include_processed_snapshot=False,
    )


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


def get_db_path(path: str | None = None) -> str:
    if path:
        return os.path.join(os.path.dirname(os.path.abspath(path)), state_db.DB_FILENAME)
    return os.path.join(get_data_dir(), state_db.DB_FILENAME)


def _get_state_dir(path: str | None) -> str:
    if path:
        return os.path.dirname(os.path.abspath(path))
    return get_data_dir()


def _get_manifest_store_path(path: str | None) -> str:
    return os.path.join(_get_state_dir(path), MANIFEST_FILENAME)


def _get_log_store_path(path: str | None) -> str:
    return os.path.join(_get_state_dir(path), LOG_FILENAME)


def _get_resume_store_path(path: str | None) -> str:
    return os.path.join(_get_state_dir(path), RESUME_FILENAME)


def _get_migration_backup_dir(path: str | None) -> str:
    return os.path.join(_get_state_dir(path), state_db.MIGRATION_BACKUP_DIRNAME)


def _get_sqlite_read_mode() -> str:
    mode = os.getenv(SQLITE_READ_MODE_ENV_NAME, "sqlite").strip().lower()
    if mode in {"legacy", "sqlite"}:
        return mode
    return "sqlite"


def _legacy_source_exists(*paths: str) -> bool:
    return any(os.path.exists(path) for path in paths)


def _load_legacy_source_fingerprint(manifest_path: str, log_path: str, resume_path: str) -> str | None:
    if not _legacy_source_exists(manifest_path, log_path, resume_path):
        return None
    bundle = state_db.build_legacy_snapshot(manifest_path, log_path, resume_path)
    return bundle["parity_snapshot"]["source_fingerprint"]


def _resolve_legacy_import_paths(path: str | None, script_dir: str | None) -> tuple[str, str, str] | None:
    app_manifest_path = _get_manifest_store_path(path)
    app_log_path = _get_log_store_path(path)
    app_resume_path = _get_resume_store_path(path)
    script_manifest_path = os.path.join(script_dir, MANIFEST_FILENAME) if script_dir is not None else ""
    script_log_path = os.path.join(script_dir, LOG_FILENAME) if script_dir is not None else ""
    script_resume_path = os.path.join(script_dir, RESUME_FILENAME) if script_dir is not None else ""
    app_exists = _legacy_source_exists(app_manifest_path, app_log_path, app_resume_path)
    script_exists = _legacy_source_exists(script_manifest_path, script_log_path, script_resume_path)
    if not app_exists and not script_exists:
        return None
    if app_exists and not script_exists:
        return app_manifest_path, app_log_path, app_resume_path
    if script_exists and not app_exists:
        return script_manifest_path, script_log_path, script_resume_path
    app_fingerprint = _load_legacy_source_fingerprint(app_manifest_path, app_log_path, app_resume_path)
    script_fingerprint = _load_legacy_source_fingerprint(script_manifest_path, script_log_path, script_resume_path)
    if app_fingerprint != script_fingerprint:
        raise StateDbImportError(
            "Conflicting legacy state sources detected. Restore from migration_backups or remove EXTRUSION_STATE_DB_READ_MODE=legacy before uploading."
        )
    return app_manifest_path, app_log_path, app_resume_path


def _ensure_sqlite_state_store(path: str | None) -> str:
    db_path = get_db_path(path)
    if os.path.exists(db_path):
        state_db.ensure_bootstrap_database(db_path)
        return db_path
    legacy_paths = _resolve_legacy_import_paths(path, None)
    if legacy_paths is not None:
        raise StateDbImportError(
            "SQLite state database is missing while legacy state files are still present. Restore uploader_state.db from migration_backups or run explicit migrate_legacy_state() before uploading."
        )
    state_db.ensure_bootstrap_database(db_path)
    return db_path


def _load_sqlite_snapshot(path: str | None) -> state_db.StateParitySnapshot:
    db_path = _ensure_sqlite_state_store(path)
    return state_db.load_sqlite_snapshot(db_path)


def load_processed(path: str | None = None) -> Set[str]:
    """
    처리 완료 파일 키를 읽습니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        sqlite_snapshot = _load_sqlite_snapshot(path)
        return set(sqlite_snapshot["processed_lookup_keys"])

    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _state_guard(manifest_path):
        envelope = _load_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
        processed_keys = set(manifest["processed"])
        for key in list(processed_keys):
            processed_keys.update(_expand_processed_key(key))
        return processed_keys


def log_processed(folder: str, filename: str, file_path: str, path: str | None = None) -> None:
    """
    처리 완료 파일 키를 저장합니다.
    """
    db_path = _ensure_sqlite_state_store(path)
    state_db.mark_file_completed(db_path, folder, filename, file_path, None)


def load_resume(path: str | None = None) -> Dict[str, int]:
    """
    재개 오프셋을 읽습니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        sqlite_snapshot = _load_sqlite_snapshot(path)
        return dict(sqlite_snapshot["resume"])

    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
        return dict(manifest["resume"])


def save_resume(data: Dict[str, int], path: str | None = None) -> None:
    """
    재개 오프셋 전체를 저장합니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    normalized_resume = _normalize_resume_map(data)
    if sqlite_read_mode != "legacy":
        db_path = _ensure_sqlite_state_store(path)
        current_resume = load_resume(path)
        candidate_keys = set(current_resume.keys()) | set(normalized_resume.keys())
        for candidate_key in sorted(candidate_keys):
            state_db.set_resume_offset(db_path, candidate_key, normalized_resume.get(candidate_key, 0))
        return

    log_path = get_log_path(path)
    resume_path = get_resume_path(path)
    manifest_path = get_manifest_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
        manifest["resume"] = normalized_resume
        _save_resume_snapshot(manifest_path, resume_path, envelope)


def set_resume_offset(key: str, offset: int, path: str | None = None) -> None:
    """
    단일 재개 오프셋을 저장합니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        db_path = _ensure_sqlite_state_store(path)
        state_db.set_resume_offset(db_path, key, offset)
        return

    log_path = get_log_path(path)
    resume_path = get_resume_path(path)
    manifest_path = get_manifest_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
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
        _save_resume_snapshot(manifest_path, resume_path, envelope)


def get_resume_offset(key: str, path: str | None = None) -> int:
    """
    저장된 재개 오프셋을 반환합니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        sqlite_snapshot = _load_sqlite_snapshot(path)
        candidates = [key]
        legacy_key = _normalize_legacy_key(key)
        if legacy_key not in candidates:
            candidates.append(legacy_key)
        for candidate in candidates:
            value = int(sqlite_snapshot["resume_lookup"].get(candidate, 0))
            if value > 0:
                return value
        return 0

    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
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


def load_upload_dashboard_state(path: str | None = None) -> UploadDashboardStateSnapshot:
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        sqlite_snapshot = _load_sqlite_snapshot(path)
        return {
            "resume": dict(sqlite_snapshot["resume"]),
            "recent_successful_upload_profile": sqlite_snapshot["recent_successful_upload_profile"],
            "failed_retry_set": tuple(cast(tuple[FailedRetryEntry, ...], sqlite_snapshot["failed_retry_set"])),
        }

    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
        profile = manifest.get("recent_successful_upload_profile")
        failed_retry_state = manifest.get("last_failed_retry_state")
        return {
            "resume": dict(manifest["resume"]),
            "recent_successful_upload_profile": (
                None if profile is None else _clone_recent_successful_upload_profile(profile)
            ),
            "failed_retry_set": (),
        }


def _empty_upload_dashboard_state() -> UploadDashboardStateSnapshot:
    return {
        "resume": {},
        "recent_successful_upload_profile": None,
        "failed_retry_set": (),
    }


def load_state_health(path: str | None = None, verify_integrity: bool = False) -> StateHealthSnapshot:
    sqlite_read_mode = _get_sqlite_read_mode()
    backup_dir = _get_migration_backup_dir(path)
    if sqlite_read_mode == "legacy":
        return {
            "state": "blocked",
            "read_mode": "legacy",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "legacy_mode",
            "detail_codes": ("legacy_mode", "restore_sqlite"),
            "backup_dir": backup_dir,
        }
    try:
        db_path = _ensure_sqlite_state_store(path)
        if verify_integrity:
            state_db.quick_check(db_path)
        snapshot = _load_sqlite_snapshot(path)
    except StateDbCorruptionError as error:
        return {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "corruption",
            "detail_codes": ("restore_sqlite", "backup_dir"),
            "error_message": str(error),
            "backup_dir": backup_dir,
        }
    except StateDbError as error:
        return {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "unavailable",
            "detail_codes": ("restore_sqlite", "backup_dir"),
            "error_message": str(error),
            "backup_dir": backup_dir,
        }
    maintenance_block = state_db.load_upload_maintenance_block(db_path)
    if maintenance_block is not None:
        return {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": False,
            "summary_code": "maintenance_block",
            "detail_codes": ("maintenance_block",),
            "error_message": maintenance_block["reason"],
            "maintenance_source": maintenance_block["source"],
            "backup_dir": backup_dir,
        }
    pending_resume_count = sum(1 for offset in snapshot["resume"].values() if int(offset) > 0)
    failed_retry_count = len(snapshot["failed_retry_set"])
    detail_codes: list[str] = []
    state = "ready"
    summary_code = "ready"
    if failed_retry_count > 0 or pending_resume_count > 0:
        state = "attention"
        summary_code = "recovery_available"
        if failed_retry_count > 0:
            detail_codes.append("failed_retry_present")
        if pending_resume_count > 0:
            detail_codes.append("resume_present")
        detail_codes.append("can_resume")
    else:
        detail_codes.append("ready")
    return {
        "state": state,
        "read_mode": "sqlite",
        "can_start_upload": True,
        "pending_resume_count": pending_resume_count,
        "failed_retry_count": failed_retry_count,
        "recovery_action_required": failed_retry_count > 0 or pending_resume_count > 0,
        "summary_code": summary_code,
        "detail_codes": tuple(detail_codes),
        "backup_dir": backup_dir,
    }


def load_recent_successful_upload_profile(
    path: str | None = None,
) -> RecentSuccessfulUploadProfile | None:
    """
    최근 성공한 업로드 프로필 정보를 읽습니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        db_path = _ensure_sqlite_state_store(path)
        profile = state_db.load_recent_successful_upload_profile(db_path)
        if profile is None:
            return None
        return _clone_recent_successful_upload_profile(cast(RecentSuccessfulUploadProfile, profile))

    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        profile = envelope["manifest"].get("recent_successful_upload_profile")
        if profile is None:
            return None
        return _clone_recent_successful_upload_profile(profile)


def load_failed_retry_set(path: str | None = None) -> tuple[FailedRetryEntry, ...]:
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        db_path = _ensure_sqlite_state_store(path)
        failed_retry_set = state_db.load_failed_retry_set(db_path)
        return tuple(cast(tuple[FailedRetryEntry, ...], failed_retry_set))

    last_failed_retry_state = load_last_failed_retry_state(path)
    if last_failed_retry_state is None:
        return ()
    legacy_key = _normalize_legacy_key(last_failed_retry_state["file_key"])
    folder, filename = _split_legacy_key(legacy_key)
    failed_retry_entry: FailedRetryEntry = {
        "file_key": last_failed_retry_state["file_key"],
        "folder": folder,
        "filename": filename,
        "legacy_key": legacy_key,
        "resume_offset": last_failed_retry_state["offset"],
        "retry_count": int(last_failed_retry_state.get("retry_count", 0) or 0),
        "failed_at": float(last_failed_retry_state.get("failed_at", 0.0) or 0.0),
    }
    error_message = _normalize_non_empty_string(last_failed_retry_state.get("error_message"))
    if error_message is not None:
        failed_retry_entry["error_message"] = error_message
    return (failed_retry_entry,)


def save_recent_successful_upload_profile(
    profile: RecentSuccessfulUploadProfile | None,
    path: str | None = None,
) -> None:
    """
    최근 성공한 업로드 프로필 정보를 저장합니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        db_path = _ensure_sqlite_state_store(path)
        normalized_profile = _normalize_recent_successful_upload_profile(profile)
        state_db.save_recent_successful_upload_profile(
            db_path,
            cast(state_db.RecentSuccessfulUploadProfile | None, normalized_profile),
        )
        return

    log_path = get_log_path(path)
    resume_path = get_resume_path(path)
    manifest_path = get_manifest_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
        normalized_profile = _normalize_recent_successful_upload_profile(profile)
        if normalized_profile is None:
            manifest.pop("recent_successful_upload_profile", None)
        else:
            manifest["recent_successful_upload_profile"] = normalized_profile
        _save_resume_snapshot(manifest_path, resume_path, envelope)


def load_last_failed_retry_state(path: str | None = None) -> LastFailedRetryState | None:
    """
    마지막 실패 재시도 상태를 읽습니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        sqlite_snapshot = _load_sqlite_snapshot(path)
        failed_retry_set = sqlite_snapshot["failed_retry_set"]
        failed_state = sqlite_snapshot["last_failed_retry_state"] if "last_failed_retry_state" in sqlite_snapshot else None
        if failed_state is None and failed_retry_set != ():
            first_failed_entry = failed_retry_set[0]
            failed_state = {
                "file_key": first_failed_entry["file_key"],
                "offset": first_failed_entry["resume_offset"],
                "retry_count": first_failed_entry["retry_count"],
                "failed_at": first_failed_entry["failed_at"],
            }
            error_message = _normalize_non_empty_string(first_failed_entry.get("error_message"))
            if error_message is not None:
                failed_state["error_message"] = error_message
        if failed_state is None:
            return None
        return _clone_last_failed_retry_state(failed_state)

    manifest_path = get_manifest_path(path)
    log_path = get_log_path(path)
    resume_path = get_resume_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        last_failed_retry_state = envelope["manifest"].get("last_failed_retry_state")
        if last_failed_retry_state is None:
            return None
        return _clone_last_failed_retry_state(last_failed_retry_state)


def save_last_failed_retry_state(
    state: LastFailedRetryState | None,
    path: str | None = None,
) -> None:
    """
    마지막 실패 재시도 상태를 저장합니다.
    """
    sqlite_read_mode = _get_sqlite_read_mode()
    if sqlite_read_mode != "legacy":
        if state is None:
            raise StateDbError("save_last_failed_retry_state is not supported after SQLite cutover")
        normalized_state = _normalize_last_failed_retry_state(state)
        if normalized_state is None:
            raise StateDbError("Last failed retry state must include file_key and offset")
        file_key = normalized_state["file_key"]
        legacy_key = _normalize_legacy_key(file_key)
        folder, filename = _split_legacy_key(legacy_key)
        db_path = _ensure_sqlite_state_store(path)
        error_message = _normalize_non_empty_string(normalized_state.get("error_message"))
        state_db.record_file_failure(
            db_path,
            folder,
            filename,
            "",
            normalized_state["offset"],
            "" if error_message is None else error_message,
            None,
        )
        return

    log_path = get_log_path(path)
    resume_path = get_resume_path(path)
    manifest_path = get_manifest_path(path)

    with _state_guard(manifest_path):
        envelope = _load_resume_state_snapshot(manifest_path, log_path, resume_path)
        manifest = envelope["manifest"]
        normalized_state = _normalize_last_failed_retry_state(state)
        if normalized_state is None:
            manifest.pop("last_failed_retry_state", None)
        else:
            manifest["last_failed_retry_state"] = normalized_state
        _save_resume_snapshot(manifest_path, resume_path, envelope)


def start_upload_run(
    total_count: int,
    retry_failed_only: bool,
    config_values: dict[str, str],
    path: str | None = None,
) -> int:
    db_path = _ensure_sqlite_state_store(path)
    return state_db.start_upload_run(db_path, total_count, retry_failed_only, config_values)


def finish_upload_run(
    run_id: int,
    total_count: int,
    success_count: int,
    failure_count: int,
    warning_messages: tuple[str, ...],
    recent_successful_upload_profile: RecentSuccessfulUploadProfile | None,
    path: str | None = None,
) -> None:
    db_path = _ensure_sqlite_state_store(path)
    state_db.finish_upload_run(
        db_path,
        run_id,
        total_count,
        success_count,
        failure_count,
        warning_messages,
        cast(state_db.RecentSuccessfulUploadProfile | None, recent_successful_upload_profile),
    )


def set_upload_maintenance_block(
    source: str,
    reason: str,
    path: str | None = None,
) -> None:
    db_path = _ensure_sqlite_state_store(path)
    state_db.set_upload_maintenance_block(db_path, source, reason)


def clear_upload_maintenance_block(path: str | None = None) -> None:
    db_path = _ensure_sqlite_state_store(path)
    state_db.clear_upload_maintenance_block(db_path)


def mark_file_completed(
    folder: str,
    filename: str,
    file_path: str,
    run_id: int | None,
    path: str | None = None,
) -> None:
    db_path = _ensure_sqlite_state_store(path)
    state_db.mark_file_completed(db_path, folder, filename, file_path, run_id)


def record_file_failure(
    folder: str,
    filename: str,
    file_path: str,
    resume_offset: int,
    error_message: str,
    run_id: int | None,
    path: str | None = None,
) -> None:
    db_path = _ensure_sqlite_state_store(path)
    state_db.record_file_failure(
        db_path,
        folder,
        filename,
        file_path,
        resume_offset,
        error_message,
        run_id,
    )


def migrate_legacy_state(script_dir: str | None = None) -> None:
    """
    기존 상태 파일을 SQLite 기준 구조로 이관합니다.
    """
    if script_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
    db_path = get_db_path()
    if os.path.exists(db_path):
        state_db.ensure_bootstrap_database(db_path)
        return
    legacy_paths = _resolve_legacy_import_paths(None, script_dir)
    if legacy_paths is None:
        state_db.ensure_bootstrap_database(db_path)
        return
    state_db.ensure_sqlite_snapshot_from_legacy(
        legacy_paths[0],
        legacy_paths[1],
        legacy_paths[2],
        db_path,
        _get_migration_backup_dir(None),
    )
