import hashlib
import json
import os
import shutil
import sqlite3
import time
from typing import Any, Iterable, NotRequired, TypedDict


DB_FILENAME = "uploader_state.db"
DB_SCHEMA_VERSION = 2
MIGRATION_BACKUP_DIRNAME = "migration_backups"
_bootstrapped_db_paths: set[str] = set()


class StateDbError(Exception):
    pass


class StateDbCorruptionError(StateDbError):
    pass


class StateDbParityError(StateDbError):
    pass


class StateDbImportError(StateDbError):
    pass


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


class UploadMaintenanceBlock(TypedDict):
    source: str
    reason: str
    activated_at: float


class PendingSupabaseReuploadDates(TypedDict):
    kst_dates: tuple[str, ...]
    updated_at: float


class FileStateRow(TypedDict):
    file_key: str
    legacy_key: str
    folder: str
    filename: str
    state: str
    resume_offset: int
    last_error: str | None
    retry_count: int
    processed_at: int | None
    failed_at: int | None
    updated_at: int


class StateParitySnapshot(TypedDict):
    processed_keys: list[str]
    processed_lookup_keys: list[str]
    resume: dict[str, int]
    resume_lookup: dict[str, int]
    recent_successful_upload_profile: RecentSuccessfulUploadProfile | None
    failed_retry_set: tuple[FailedRetryEntry, ...]
    last_failed_retry_state: LastFailedRetryState | None
    source_fingerprint: str


class LegacyImportBundle(TypedDict):
    rows: list[FileStateRow]
    alias_rows: list[tuple[str, str]]
    parity_snapshot: StateParitySnapshot


class SQLiteBootstrapResult(TypedDict):
    backup_dir: str | None
    db_path: str
    imported: bool
    source_fingerprint: str


def _normalize_legacy_key(key: str) -> str:
    if "|" not in key:
        return key
    return key.split("|", 1)[0]


def _expand_processed_key(key: str) -> set[str]:
    normalized = str(key).strip()
    if normalized == "":
        return set()

    expanded = {normalized}
    legacy_key = _normalize_legacy_key(normalized)
    if legacy_key != normalized:
        expanded.add(legacy_key)
    return expanded


def _normalize_processed_set(values: Any) -> set[str]:
    if not isinstance(values, (list, set, tuple)):
        return set()

    processed: set[str] = set()
    for value in values:
        key = str(value).strip()
        if key != "":
            processed.add(key)
    return processed


def _normalize_resume_map(values: Any) -> dict[str, int]:
    if not isinstance(values, dict):
        return {}

    resume: dict[str, int] = {}
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


def _normalize_recent_successful_upload_profile(values: Any) -> RecentSuccessfulUploadProfile | None:
    if not isinstance(values, dict):
        return None

    profile_name = _normalize_non_empty_string(values.get("profile_name"))
    if profile_name is None:
        return None

    normalized_profile: dict[str, Any] = {"profile_name": profile_name}
    applied_at = _normalize_positive_float(values.get("applied_at"))
    if applied_at is not None:
        normalized_profile["applied_at"] = applied_at

    profile_values = _normalize_string_map(values.get("values"))
    if profile_values is not None:
        normalized_profile["values"] = profile_values

    return RecentSuccessfulUploadProfile(**normalized_profile)


def _normalize_last_failed_retry_state(values: Any) -> LastFailedRetryState | None:
    if not isinstance(values, dict):
        return None

    file_key = _normalize_non_empty_string(values.get("file_key"))
    offset = _normalize_positive_int(values.get("offset"))
    if file_key is None or offset is None:
        return None

    normalized_state: dict[str, Any] = {
        "file_key": file_key,
        "offset": offset,
    }
    retry_count = _normalize_positive_int(values.get("retry_count"))
    if retry_count is not None:
        normalized_state["retry_count"] = retry_count

    failed_at = _normalize_positive_float(values.get("failed_at"))
    if failed_at is not None:
        normalized_state["failed_at"] = failed_at

    error_message = _normalize_non_empty_string(values.get("error_message"))
    if error_message is not None:
        normalized_state["error_message"] = error_message

    return LastFailedRetryState(**normalized_state)


def _normalize_manifest(raw_manifest: dict[str, Any]) -> tuple[set[str], dict[str, int], RecentSuccessfulUploadProfile | None, LastFailedRetryState | None]:
    return (
        _normalize_processed_set(raw_manifest.get("processed", [])),
        _normalize_resume_map(raw_manifest.get("resume", {})),
        _normalize_recent_successful_upload_profile(raw_manifest.get("recent_successful_upload_profile")),
        _normalize_last_failed_retry_state(raw_manifest.get("last_failed_retry_state")),
    )


def _load_json_object_strict(path: str) -> dict[str, Any]:
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as file_handle:
            raw_value = json.load(file_handle)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError as error:
        raise StateDbCorruptionError(f"Corrupted JSON state file: {path}") from error
    except OSError as error:
        raise StateDbCorruptionError(f"Failed to read JSON state file: {path}") from error
    if isinstance(raw_value, dict):
        return raw_value
    raise StateDbCorruptionError(f"JSON state file must contain an object: {path}")


def _load_legacy_processed_keys_strict(path: str) -> set[str]:
    if not os.path.exists(path):
        return set()
    try:
        with open(path, "r", encoding="utf-8") as file_handle:
            return {line.strip() for line in file_handle if line.strip()}
    except UnicodeDecodeError:
        try:
            with open(path, "r", encoding="cp949", errors="strict") as file_handle:
                return {line.strip() for line in file_handle if line.strip()}
        except OSError as error:
            raise StateDbCorruptionError(f"Failed to read processed log: {path}") from error
    except OSError as error:
        raise StateDbCorruptionError(f"Failed to read processed log: {path}") from error


def _compute_file_hash(path: str) -> str | None:
    if not os.path.exists(path):
        return None
    digest = hashlib.sha256()
    with open(path, "rb") as file_handle:
        for chunk in iter(lambda: file_handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _serialize_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _resolve_group_key(
    raw_key: str,
    basename_candidates: dict[str, set[str]],
) -> str:
    normalized = str(raw_key).strip()
    legacy_key = _normalize_legacy_key(normalized)
    if "/" in legacy_key:
        return legacy_key

    basename = os.path.basename(legacy_key)
    matching_candidates = basename_candidates.get(basename, set())
    if len(matching_candidates) > 1:
        raise StateDbImportError(f"Ambiguous basename alias during legacy import: {basename}")
    if len(matching_candidates) == 1:
        return next(iter(matching_candidates))
    return basename


def _pick_canonical_key(keys: Iterable[str]) -> str:
    normalized_keys = sorted({str(key).strip() for key in keys if str(key).strip() != ""})
    for key in normalized_keys:
        if "|size=" in key and "|mtime_ns=" in key:
            return key
    for key in normalized_keys:
        if "/" in _normalize_legacy_key(key):
            return key
    if normalized_keys == []:
        raise StateDbImportError("No keys available to choose canonical file key")
    return normalized_keys[0]


def _split_legacy_key(legacy_key: str) -> tuple[str, str]:
    normalized = legacy_key.replace("\\", "/")
    if "/" not in normalized:
        return "", normalized
    folder, filename = normalized.rsplit("/", 1)
    return folder, filename


def _build_legacy_import_bundle(
    processed_keys: set[str],
    resume_map: dict[str, int],
    recent_successful_upload_profile: RecentSuccessfulUploadProfile | None,
    last_failed_retry_state: LastFailedRetryState | None,
) -> LegacyImportBundle:
    basename_candidates: dict[str, set[str]] = {}
    all_keys = set(processed_keys) | set(resume_map.keys())
    if last_failed_retry_state is not None:
        all_keys.add(last_failed_retry_state["file_key"])

    for raw_key in all_keys:
        legacy_key = _normalize_legacy_key(raw_key)
        if "/" not in legacy_key:
            continue
        basename = os.path.basename(legacy_key)
        basename_candidates.setdefault(basename, set()).add(legacy_key)

    groups: dict[str, dict[str, Any]] = {}
    for raw_key in all_keys:
        group_key = _resolve_group_key(raw_key, basename_candidates)
        group = groups.setdefault(
            group_key,
            {
                "keys": set(),
                "processed": False,
                "resume_offsets": [],
                "failed_state": None,
            },
        )
        group["keys"].add(raw_key)
        if raw_key in processed_keys:
            group["processed"] = True
        if raw_key in resume_map:
            group["resume_offsets"].append(int(resume_map[raw_key]))

    if last_failed_retry_state is not None:
        failure_group_key = _resolve_group_key(last_failed_retry_state["file_key"], basename_candidates)
        groups.setdefault(
            failure_group_key,
            {
                "keys": set(),
                "processed": False,
                "resume_offsets": [],
                "failed_state": None,
            },
        )
        groups[failure_group_key]["keys"].add(last_failed_retry_state["file_key"])
        groups[failure_group_key]["failed_state"] = dict(last_failed_retry_state)

    rows: list[FileStateRow] = []
    alias_rows: list[tuple[str, str]] = []
    processed_lookup_keys: set[str] = set()
    resume_lookup: dict[str, int] = {}
    failed_retry_entries: list[FailedRetryEntry] = []
    now = int(time.time())

    for group_key in sorted(groups.keys()):
        group = groups[group_key]
        canonical_key = _pick_canonical_key(group["keys"])
        legacy_key = _normalize_legacy_key(canonical_key)
        folder, filename = _split_legacy_key(legacy_key)
        resume_offset = max(group["resume_offsets"], default=0)
        failed_state = group["failed_state"]
        processed_flag = bool(group["processed"])

        if processed_flag and resume_offset > 0:
            raise StateDbImportError(f"Processed/resume conflict during legacy import: {canonical_key}")
        if processed_flag and failed_state is not None:
            raise StateDbImportError(f"Processed/failed conflict during legacy import: {canonical_key}")

        last_error: str | None = None
        retry_count = 0
        processed_at: int | None = None
        failed_at: int | None = None
        state = "in_progress"

        if processed_flag:
            state = "completed"
            processed_at = now
        elif failed_state is not None:
            state = "failed"
            resume_offset = int(failed_state["offset"])
            last_error = failed_state.get("error_message")
            retry_count = int(failed_state.get("retry_count", 0) or 0)
            failed_at = int(failed_state.get("failed_at", now) or now)
        elif resume_offset > 0:
            state = "in_progress"

        row: FileStateRow = {
            "file_key": canonical_key,
            "legacy_key": legacy_key,
            "folder": folder,
            "filename": filename,
            "state": state,
            "resume_offset": resume_offset,
            "last_error": last_error,
            "retry_count": retry_count,
            "processed_at": processed_at,
            "failed_at": failed_at,
            "updated_at": now,
        }
        rows.append(row)
        if state == "failed":
            failed_retry_entry: FailedRetryEntry = {
                "file_key": canonical_key,
                "folder": folder,
                "filename": filename,
                "legacy_key": legacy_key,
                "resume_offset": resume_offset,
                "retry_count": retry_count,
                "failed_at": float(failed_at if failed_at is not None else now),
            }
            if last_error is not None and str(last_error).strip() != "":
                failed_retry_entry["error_message"] = str(last_error)
            failed_retry_entries.append(failed_retry_entry)

        alias_keys = set(group["keys"])
        alias_keys.update(_expand_processed_key(canonical_key))
        alias_keys.update(_expand_processed_key(legacy_key))
        for alias_key in sorted(alias_keys):
            if alias_key == "":
                continue
            alias_rows.append((alias_key, canonical_key))
            if state == "completed":
                processed_lookup_keys.add(alias_key)
            if resume_offset > 0:
                resume_lookup[alias_key] = resume_offset

    parity_snapshot: StateParitySnapshot = {
        "processed_keys": sorted(row["file_key"] for row in rows if row["state"] == "completed"),
        "processed_lookup_keys": sorted(processed_lookup_keys),
        "resume": {row["file_key"]: row["resume_offset"] for row in rows if row["resume_offset"] > 0},
        "resume_lookup": dict(sorted(resume_lookup.items())),
        "recent_successful_upload_profile": recent_successful_upload_profile,
        "failed_retry_set": tuple(sorted(failed_retry_entries, key=lambda entry: (-entry["failed_at"], entry["file_key"]))),
        "last_failed_retry_state": last_failed_retry_state,
        "source_fingerprint": "",
    }
    parity_snapshot["source_fingerprint"] = hashlib.sha256(_serialize_json(parity_snapshot).encode("utf-8")).hexdigest()
    return {
        "rows": rows,
        "alias_rows": alias_rows,
        "parity_snapshot": parity_snapshot,
    }


def build_legacy_snapshot(
    manifest_path: str,
    log_path: str,
    resume_path: str,
) -> LegacyImportBundle:
    if os.path.exists(manifest_path):
        raw_manifest = _load_json_object_strict(manifest_path)
        processed_keys, resume_map, recent_profile, failed_state = _normalize_manifest(raw_manifest)
        legacy_processed_keys = _load_legacy_processed_keys_strict(log_path)
        legacy_resume_map = _normalize_resume_map(_load_json_object_strict(resume_path))
        if legacy_processed_keys and legacy_processed_keys != processed_keys:
            raise StateDbImportError("Manifest/log parity mismatch during legacy import")
        if legacy_resume_map and legacy_resume_map != resume_map:
            raise StateDbImportError("Manifest/resume parity mismatch during legacy import")
        return _build_legacy_import_bundle(processed_keys, resume_map, recent_profile, failed_state)

    processed_keys = _load_legacy_processed_keys_strict(log_path)
    resume_map = _normalize_resume_map(_load_json_object_strict(resume_path))
    return _build_legacy_import_bundle(processed_keys, resume_map, None, None)


def connect_state_db(db_path: str) -> sqlite3.Connection:
    parent_dir = os.path.dirname(os.path.abspath(db_path)) or "."
    os.makedirs(parent_dir, exist_ok=True)
    connection = sqlite3.connect(db_path, timeout=10.0)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=FULL")
    connection.execute("PRAGMA busy_timeout=10000")
    connection.execute("PRAGMA foreign_keys=ON")
    connection.execute(f"PRAGMA user_version={DB_SCHEMA_VERSION}")
    return connection


def bootstrap_database(db_path: str) -> None:
    try:
        connection = connect_state_db(db_path)
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    try:
        now = int(time.time())
        connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS state_meta (
              key TEXT PRIMARY KEY,
              value_json TEXT NOT NULL,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS upload_runs (
              run_id INTEGER PRIMARY KEY AUTOINCREMENT,
              started_at REAL NOT NULL,
              completed_at REAL,
              mode TEXT NOT NULL,
              config_json TEXT NOT NULL,
              status TEXT NOT NULL,
              total_count INTEGER NOT NULL,
              success_count INTEGER NOT NULL DEFAULT 0,
              failure_count INTEGER NOT NULL DEFAULT 0,
              warning_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS file_state (
              file_key TEXT PRIMARY KEY,
              legacy_key TEXT NOT NULL,
              folder TEXT NOT NULL,
              filename TEXT NOT NULL,
              state TEXT NOT NULL CHECK(state IN ('in_progress', 'completed', 'failed')),
              resume_offset INTEGER NOT NULL DEFAULT 0,
              last_error TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              processed_at INTEGER,
              failed_at INTEGER,
              updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS file_key_alias (
              alias_key TEXT PRIMARY KEY,
              file_key TEXT NOT NULL REFERENCES file_state(file_key) ON DELETE CASCADE
            );

            CREATE TABLE IF NOT EXISTS recent_successful_upload_profile (
              singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
              profile_name TEXT NOT NULL,
              applied_at REAL,
              values_json TEXT
            );

            CREATE TABLE IF NOT EXISTS failed_retry_set (
              file_key TEXT PRIMARY KEY REFERENCES file_state(file_key) ON DELETE CASCADE,
              resume_offset INTEGER NOT NULL,
              error_message TEXT,
              retry_count INTEGER NOT NULL DEFAULT 0,
              failed_at REAL NOT NULL,
              last_run_id INTEGER REFERENCES upload_runs(run_id) ON DELETE SET NULL
            );

            CREATE TABLE IF NOT EXISTS last_failed_retry_state (
              singleton_id INTEGER PRIMARY KEY CHECK(singleton_id = 1),
              file_key TEXT NOT NULL,
              offset INTEGER NOT NULL,
              retry_count INTEGER,
              failed_at REAL,
              error_message TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_file_state_state_updated_at
              ON file_state(state, updated_at);

            CREATE INDEX IF NOT EXISTS idx_failed_retry_set_failed_at
              ON failed_retry_set(failed_at DESC, file_key);
            """
        )
        _migrate_phase1_failed_retry_state(connection)
        _upsert_state_meta(connection, "schema_version", DB_SCHEMA_VERSION, now)
        existing_migration_row = connection.execute(
            """
            SELECT value_json
            FROM state_meta
            WHERE key = 'migration_complete'
            """
        ).fetchone()
        if existing_migration_row is None:
            _upsert_state_meta(connection, "migration_complete", True, now)
        connection.commit()
    finally:
        connection.close()


def ensure_bootstrap_database(db_path: str) -> None:
    normalized_db_path = os.path.abspath(db_path)
    if normalized_db_path in _bootstrapped_db_paths and os.path.exists(normalized_db_path):
        return
    bootstrap_database(normalized_db_path)
    _bootstrapped_db_paths.add(normalized_db_path)


def _migrate_phase1_failed_retry_state(connection: sqlite3.Connection) -> None:
    failed_retry_count_row = connection.execute(
        "SELECT COUNT(*) AS count FROM failed_retry_set"
    ).fetchone()
    if failed_retry_count_row is None or int(failed_retry_count_row["count"]) > 0:
        return

    legacy_failed_row = connection.execute(
        """
        SELECT file_key, offset, retry_count, failed_at, error_message
        FROM last_failed_retry_state
        WHERE singleton_id = 1
        """
    ).fetchone()
    if legacy_failed_row is None:
        return

    file_row = connection.execute(
        """
        SELECT file_key, state, resume_offset
        FROM file_state
        WHERE file_key = ?
        """,
        (str(legacy_failed_row["file_key"]),),
    ).fetchone()
    if file_row is None:
        return

    if str(file_row["state"]) != "failed":
        return

    connection.execute(
        """
        INSERT OR REPLACE INTO failed_retry_set(
          file_key, resume_offset, error_message, retry_count, failed_at, last_run_id
        )
        VALUES (?, ?, ?, ?, ?, NULL)
        """,
        (
            str(legacy_failed_row["file_key"]),
            int(legacy_failed_row["offset"]),
            None if legacy_failed_row["error_message"] is None else str(legacy_failed_row["error_message"]),
            int(legacy_failed_row["retry_count"] or 0),
            float(legacy_failed_row["failed_at"] or time.time()),
        ),
    )


def get_user_version(db_path: str) -> int:
    connection = connect_state_db(db_path)
    try:
        row = connection.execute("PRAGMA user_version").fetchone()
        return int(row[0]) if row is not None else 0
    finally:
        connection.close()


def integrity_check(db_path: str) -> None:
    try:
        connection = connect_state_db(db_path)
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    try:
        row = connection.execute("PRAGMA integrity_check").fetchone()
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    finally:
        connection.close()
    if row is None:
        raise StateDbCorruptionError(f"Missing integrity check result for SQLite state database: {db_path}")
    if str(row[0]).strip().lower() != "ok":
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}: {row[0]}")


def quick_check(db_path: str) -> None:
    try:
        connection = connect_state_db(db_path)
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    try:
        row = connection.execute("PRAGMA quick_check").fetchone()
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    finally:
        connection.close()
    if row is None:
        raise StateDbCorruptionError(f"Missing quick check result for SQLite state database: {db_path}")
    if str(row[0]).strip().lower() != "ok":
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}: {row[0]}")


def _upsert_state_meta(connection: sqlite3.Connection, key: str, value: Any, updated_at: int) -> None:
    connection.execute(
        """
        INSERT INTO state_meta(key, value_json, updated_at)
        VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET
          value_json=excluded.value_json,
          updated_at=excluded.updated_at
        """,
        (key, _serialize_json(value), updated_at),
    )


def _load_state_meta_value(connection: sqlite3.Connection, key: str) -> Any | None:
    row = connection.execute(
        """
        SELECT value_json
        FROM state_meta
        WHERE key = ?
        """,
        (key,),
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(str(row["value_json"]))
    except json.JSONDecodeError as error:
        raise StateDbCorruptionError(f"Invalid {key} metadata in SQLite state database") from error


def set_upload_maintenance_block(
    db_path: str,
    source: str,
    reason: str,
) -> None:
    ensure_bootstrap_database(db_path)
    updated_at = int(time.time())
    connection = connect_state_db(db_path)
    try:
        with connection:
            _upsert_state_meta(
                connection,
                "upload_maintenance_block",
                {
                    "source": source,
                    "reason": reason,
                    "activated_at": time.time(),
                },
                updated_at,
            )
    finally:
        connection.close()


def clear_upload_maintenance_block(db_path: str) -> None:
    ensure_bootstrap_database(db_path)
    connection = connect_state_db(db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM state_meta WHERE key = 'upload_maintenance_block'"
            )
    finally:
        connection.close()


def load_upload_maintenance_block(db_path: str) -> UploadMaintenanceBlock | None:
    ensure_bootstrap_database(db_path)
    connection = connect_state_db(db_path)
    try:
        raw_value = _load_state_meta_value(connection, "upload_maintenance_block")
    finally:
        connection.close()
    if raw_value is None:
        return None
    if not isinstance(raw_value, dict):
        raise StateDbCorruptionError(
            f"Invalid upload_maintenance_block metadata in SQLite state database: {db_path}"
        )
    source = str(raw_value.get("source", "")).strip()
    reason = str(raw_value.get("reason", "")).strip()
    activated_at_raw = raw_value.get("activated_at")
    try:
        activated_at = float(activated_at_raw)
    except (TypeError, ValueError) as error:
        raise StateDbCorruptionError(
            f"Invalid upload_maintenance_block metadata in SQLite state database: {db_path}"
        ) from error
    if source == "" or reason == "":
        raise StateDbCorruptionError(
            f"Invalid upload_maintenance_block metadata in SQLite state database: {db_path}"
        )
    return UploadMaintenanceBlock(
        source=source,
        reason=reason,
        activated_at=activated_at,
    )


def save_pending_supabase_reupload_dates(
    db_path: str,
    kst_dates: tuple[str, ...],
) -> None:
    ensure_bootstrap_database(db_path)
    normalized_dates = tuple(sorted({str(value).strip() for value in kst_dates if str(value).strip() != ""}))
    updated_at = int(time.time())
    connection = connect_state_db(db_path)
    try:
        with connection:
            if normalized_dates == ():
                connection.execute(
                    "DELETE FROM state_meta WHERE key = 'pending_supabase_reupload_dates'"
                )
                return
            _upsert_state_meta(
                connection,
                "pending_supabase_reupload_dates",
                {
                    "kst_dates": list(normalized_dates),
                    "updated_at": time.time(),
                },
                updated_at,
            )
    finally:
        connection.close()


def clear_pending_supabase_reupload_dates(db_path: str) -> None:
    ensure_bootstrap_database(db_path)
    connection = connect_state_db(db_path)
    try:
        with connection:
            connection.execute(
                "DELETE FROM state_meta WHERE key = 'pending_supabase_reupload_dates'"
            )
    finally:
        connection.close()


def load_pending_supabase_reupload_dates(db_path: str) -> PendingSupabaseReuploadDates | None:
    ensure_bootstrap_database(db_path)
    connection = connect_state_db(db_path)
    try:
        raw_value = _load_state_meta_value(connection, "pending_supabase_reupload_dates")
    finally:
        connection.close()
    if raw_value is None:
        return None
    if not isinstance(raw_value, dict):
        raise StateDbCorruptionError(
            f"Invalid pending_supabase_reupload_dates metadata in SQLite state database: {db_path}"
        )

    raw_dates = raw_value.get("kst_dates")
    if not isinstance(raw_dates, list):
        raise StateDbCorruptionError(
            f"Invalid pending_supabase_reupload_dates metadata in SQLite state database: {db_path}"
        )
    normalized_dates = tuple(
        sorted({str(value).strip() for value in raw_dates if str(value).strip() != ""})
    )
    updated_at_raw = raw_value.get("updated_at")
    try:
        updated_at = float(updated_at_raw)
    except (TypeError, ValueError) as error:
        raise StateDbCorruptionError(
            f"Invalid pending_supabase_reupload_dates metadata in SQLite state database: {db_path}"
        ) from error
    return PendingSupabaseReuploadDates(
        kst_dates=normalized_dates,
        updated_at=updated_at,
    )


def _build_runtime_legacy_key(folder: str, filename: str) -> str:
    return f"{folder}/{filename}"


def _build_runtime_file_key(folder: str, filename: str, file_path: str) -> str:
    legacy_key = _build_runtime_legacy_key(folder, filename)
    if file_path.strip() == "":
        return legacy_key
    try:
        stat_result = os.stat(file_path)
    except OSError:
        return legacy_key
    return f"{legacy_key}|size={stat_result.st_size}|mtime_ns={stat_result.st_mtime_ns}"


def _build_alias_keys_for_file(file_key: str, legacy_key: str) -> tuple[str, ...]:
    alias_keys = set(_expand_processed_key(file_key))
    alias_keys.update(_expand_processed_key(legacy_key))
    return tuple(sorted(alias_keys))


def _is_runtime_alias_key_supported(alias_key: str) -> bool:
    normalized = alias_key.strip()
    if normalized == "":
        return False
    return "/" in _normalize_legacy_key(normalized)


def _fetch_file_state_row(connection: sqlite3.Connection, file_key: str) -> sqlite3.Row | None:
    return connection.execute(
        """
        SELECT file_key, legacy_key, folder, filename, state, resume_offset, last_error, retry_count, processed_at, failed_at
        FROM file_state
        WHERE file_key = ?
        """,
        (file_key,),
    ).fetchone()


def _upsert_file_state(
    connection: sqlite3.Connection,
    file_key: str,
    legacy_key: str,
    folder: str,
    filename: str,
    state: str,
    resume_offset: int,
    last_error: str | None,
    retry_count: int,
    processed_at: int | None,
    failed_at: int | None,
    updated_at: int,
) -> None:
    connection.execute(
        """
        INSERT INTO file_state(
          file_key, legacy_key, folder, filename, state,
          resume_offset, last_error, retry_count, processed_at,
          failed_at, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(file_key) DO UPDATE SET
          legacy_key=excluded.legacy_key,
          folder=excluded.folder,
          filename=excluded.filename,
          state=excluded.state,
          resume_offset=excluded.resume_offset,
          last_error=excluded.last_error,
          retry_count=excluded.retry_count,
          processed_at=excluded.processed_at,
          failed_at=excluded.failed_at,
          updated_at=excluded.updated_at
        """,
        (
            file_key,
            legacy_key,
            folder,
            filename,
            state,
            resume_offset,
            last_error,
            retry_count,
            processed_at,
            failed_at,
            updated_at,
        ),
    )


def _upsert_aliases(connection: sqlite3.Connection, file_key: str, legacy_key: str) -> None:
    connection.execute(
        """
        DELETE FROM file_key_alias
        WHERE file_key = ?
        """,
        (file_key,),
    )
    for alias_key in _build_alias_keys_for_file(file_key, legacy_key):
        connection.execute(
            """
            INSERT OR REPLACE INTO file_key_alias(alias_key, file_key)
            VALUES (?, ?)
            """,
            (alias_key, file_key),
        )


def _normalize_failed_retry_entries(rows: list[sqlite3.Row]) -> tuple[FailedRetryEntry, ...]:
    entries: list[FailedRetryEntry] = []
    for row in rows:
        entry: FailedRetryEntry = {
            "file_key": str(row["file_key"]),
            "folder": str(row["folder"]),
            "filename": str(row["filename"]),
            "legacy_key": str(row["legacy_key"]),
            "resume_offset": int(row["resume_offset"]),
            "retry_count": int(row["retry_count"]),
            "failed_at": float(row["failed_at"]),
        }
        if row["error_message"] is not None and str(row["error_message"]).strip() != "":
            entry["error_message"] = str(row["error_message"])
        entries.append(entry)
    return tuple(entries)


def _resolve_existing_run_id(connection: sqlite3.Connection, run_id: int | None) -> int | None:
    if run_id is None:
        return None
    row = connection.execute(
        """
        SELECT run_id
        FROM upload_runs
        WHERE run_id = ?
        """,
        (int(run_id),),
    ).fetchone()
    if row is None:
        return None
    return int(row["run_id"])


def _import_bundle(connection: sqlite3.Connection, bundle: LegacyImportBundle) -> None:
    now = int(time.time())
    connection.execute("DELETE FROM file_key_alias")
    connection.execute("DELETE FROM file_state")
    connection.execute("DELETE FROM failed_retry_set")
    connection.execute("DELETE FROM recent_successful_upload_profile")
    connection.execute("DELETE FROM last_failed_retry_state")

    for row in bundle["rows"]:
        connection.execute(
            """
            INSERT INTO file_state(
              file_key, legacy_key, folder, filename, state,
              resume_offset, last_error, retry_count, processed_at,
              failed_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["file_key"],
                row["legacy_key"],
                row["folder"],
                row["filename"],
                row["state"],
                row["resume_offset"],
                row["last_error"],
                row["retry_count"],
                row["processed_at"],
                row["failed_at"],
                row["updated_at"],
            ),
        )

    for alias_key, file_key in bundle["alias_rows"]:
        connection.execute(
            """
            INSERT OR REPLACE INTO file_key_alias(alias_key, file_key)
            VALUES (?, ?)
            """,
            (alias_key, file_key),
        )

    profile = bundle["parity_snapshot"]["recent_successful_upload_profile"]
    if profile is not None:
        connection.execute(
            """
            INSERT INTO recent_successful_upload_profile(singleton_id, profile_name, applied_at, values_json)
            VALUES (1, ?, ?, ?)
            """,
            (
                profile["profile_name"],
                profile.get("applied_at"),
                None if "values" not in profile else _serialize_json(profile["values"]),
            ),
        )

    for failed_retry_entry in bundle["parity_snapshot"]["failed_retry_set"]:
        connection.execute(
            """
            INSERT INTO failed_retry_set(
              file_key, resume_offset, error_message, retry_count, failed_at, last_run_id
            )
            VALUES (?, ?, ?, ?, ?, NULL)
            """,
            (
                failed_retry_entry["file_key"],
                failed_retry_entry["resume_offset"],
                failed_retry_entry.get("error_message"),
                failed_retry_entry["retry_count"],
                failed_retry_entry["failed_at"],
            ),
        )

    failed_state = bundle["parity_snapshot"]["last_failed_retry_state"]
    if failed_state is not None:
        connection.execute(
            """
            INSERT INTO last_failed_retry_state(
              singleton_id, file_key, offset, retry_count, failed_at, error_message
            )
            VALUES (1, ?, ?, ?, ?, ?)
            """,
            (
                failed_state["file_key"],
                failed_state["offset"],
                failed_state.get("retry_count"),
                failed_state.get("failed_at"),
                failed_state.get("error_message"),
            ),
        )

    _upsert_state_meta(connection, "source_fingerprint", bundle["parity_snapshot"]["source_fingerprint"], now)
    _upsert_state_meta(connection, "migration_complete", True, now)
    _upsert_state_meta(connection, "schema_version", DB_SCHEMA_VERSION, now)


def load_sqlite_snapshot(db_path: str) -> StateParitySnapshot:
    try:
        connection = connect_state_db(db_path)
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    try:
        rows = connection.execute(
            """
            SELECT file_key, state, resume_offset
            FROM file_state
            ORDER BY file_key
            """
        ).fetchall()
        alias_rows = connection.execute(
            """
            SELECT alias_key, file_key
            FROM file_key_alias
            ORDER BY alias_key
            """
        ).fetchall()
        profile_row = connection.execute(
            """
            SELECT profile_name, applied_at, values_json
            FROM recent_successful_upload_profile
            WHERE singleton_id = 1
            """
        ).fetchone()
        failed_rows = connection.execute(
            """
            SELECT
              fr.file_key,
              fs.folder,
              fs.filename,
              fs.legacy_key,
              fr.resume_offset,
              fr.retry_count,
              fr.failed_at,
              fr.error_message
            FROM failed_retry_set fr
            INNER JOIN file_state fs ON fs.file_key = fr.file_key
            ORDER BY fr.failed_at DESC, fr.file_key
            """
        ).fetchall()
        fingerprint_row = connection.execute(
            """
            SELECT value_json
            FROM state_meta
            WHERE key = 'source_fingerprint'
            """
        ).fetchone()
        schema_version_row = connection.execute(
            """
            SELECT value_json
            FROM state_meta
            WHERE key = 'schema_version'
            """
        ).fetchone()
        migration_complete_row = connection.execute(
            """
            SELECT value_json
            FROM state_meta
            WHERE key = 'migration_complete'
            """
        ).fetchone()
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    finally:
        connection.close()

    alias_by_file_key: dict[str, set[str]] = {}
    for alias_row in alias_rows:
        alias_key = str(alias_row["alias_key"])
        if not _is_runtime_alias_key_supported(alias_key):
            continue
        alias_by_file_key.setdefault(str(alias_row["file_key"]), set()).add(alias_key)

    processed_keys: list[str] = []
    processed_lookup_keys: set[str] = set()
    resume: dict[str, int] = {}
    resume_lookup: dict[str, int] = {}
    state_by_file_key: dict[str, str] = {}
    for row in rows:
        file_key = str(row["file_key"])
        state = str(row["state"])
        resume_offset = int(row["resume_offset"])
        state_by_file_key[file_key] = state
        if state == "completed" and resume_offset != 0:
            raise StateDbCorruptionError(f"Completed file has non-zero resume offset: {file_key}")
        if state == "failed" and resume_offset <= 0:
            raise StateDbCorruptionError(f"Failed file has invalid resume offset: {file_key}")
        if state == "completed":
            processed_keys.append(file_key)
            processed_lookup_keys.update(alias_by_file_key.get(file_key, set()))
            processed_lookup_keys.update(_expand_processed_key(file_key))
        if resume_offset > 0:
            resume[file_key] = resume_offset
            for alias_key in alias_by_file_key.get(file_key, set()):
                resume_lookup[alias_key] = resume_offset
            resume_lookup[file_key] = resume_offset
            resume_lookup[_normalize_legacy_key(file_key)] = resume_offset

    recent_profile: RecentSuccessfulUploadProfile | None = None
    if profile_row is not None:
        values: dict[str, Any] = {"profile_name": str(profile_row["profile_name"])}
        if profile_row["applied_at"] is not None:
            values["applied_at"] = float(profile_row["applied_at"])
        if profile_row["values_json"] is not None:
            try:
                values["values"] = json.loads(str(profile_row["values_json"]))
            except json.JSONDecodeError as error:
                raise StateDbCorruptionError(f"Invalid recent_successful_upload_profile metadata in SQLite state database: {db_path}") from error
        recent_profile = _normalize_recent_successful_upload_profile(values)

    failed_retry_set = _normalize_failed_retry_entries(list(failed_rows))
    failed_retry_keys = {entry["file_key"] for entry in failed_retry_set}
    failed_state: LastFailedRetryState | None = None
    for failed_retry_entry in failed_retry_set:
        failed_file_key = failed_retry_entry["file_key"]
        if state_by_file_key.get(failed_file_key) != "failed":
            raise StateDbCorruptionError(f"Failed retry state points to non-failed file: {failed_file_key}")
        if resume.get(failed_file_key) != failed_retry_entry["resume_offset"]:
            raise StateDbCorruptionError(f"Failed retry state offset mismatch: {failed_file_key}")
    for file_key, state in state_by_file_key.items():
        if state == "failed" and file_key not in failed_retry_keys:
            raise StateDbCorruptionError(f"Failed file missing retry set row: {file_key}")
    if failed_retry_set != ():
        first_failed_entry = failed_retry_set[0]
        failed_values: dict[str, Any] = {
            "file_key": first_failed_entry["file_key"],
            "offset": first_failed_entry["resume_offset"],
            "retry_count": first_failed_entry["retry_count"],
            "failed_at": first_failed_entry["failed_at"],
        }
        if "error_message" in first_failed_entry:
            failed_values["error_message"] = first_failed_entry["error_message"]
        failed_state = _normalize_last_failed_retry_state(failed_values)
        if failed_state is None:
            raise StateDbCorruptionError("Failed retry state row is invalid")

    source_fingerprint = ""
    if fingerprint_row is not None:
        try:
            source_fingerprint = str(json.loads(str(fingerprint_row["value_json"])))
        except json.JSONDecodeError as error:
            raise StateDbCorruptionError(f"Invalid source_fingerprint metadata in SQLite state database: {db_path}") from error
    if schema_version_row is None:
        raise StateDbCorruptionError(f"Missing schema_version metadata in SQLite state database: {db_path}")
    try:
        schema_version = int(json.loads(str(schema_version_row["value_json"])))
    except (ValueError, TypeError, json.JSONDecodeError) as error:
        raise StateDbCorruptionError(f"Invalid schema_version metadata in SQLite state database: {db_path}") from error
    if schema_version != DB_SCHEMA_VERSION:
        raise StateDbCorruptionError(
            f"Unsupported SQLite state schema version: {schema_version} (expected {DB_SCHEMA_VERSION})"
        )
    if migration_complete_row is None:
        raise StateDbCorruptionError(f"Missing migration_complete metadata in SQLite state database: {db_path}")
    try:
        migration_complete = bool(json.loads(str(migration_complete_row["value_json"])))
    except json.JSONDecodeError as error:
        raise StateDbCorruptionError(f"Invalid migration_complete metadata in SQLite state database: {db_path}") from error
    if not migration_complete:
        raise StateDbCorruptionError(f"SQLite state database is not marked migration_complete: {db_path}")

    return {
        "processed_keys": sorted(processed_keys),
        "processed_lookup_keys": sorted(processed_lookup_keys),
        "resume": dict(sorted(resume.items())),
        "resume_lookup": dict(sorted(resume_lookup.items())),
        "recent_successful_upload_profile": recent_profile,
        "failed_retry_set": failed_retry_set,
        "last_failed_retry_state": failed_state,
        "source_fingerprint": source_fingerprint,
    }


def parity_check(expected_snapshot: StateParitySnapshot, actual_snapshot: StateParitySnapshot) -> None:
    if expected_snapshot["source_fingerprint"] != actual_snapshot["source_fingerprint"]:
        raise StateDbParityError("Source fingerprint parity mismatch")
    if expected_snapshot["processed_keys"] != actual_snapshot["processed_keys"]:
        raise StateDbParityError("Processed canonical parity mismatch")
    if expected_snapshot["processed_lookup_keys"] != actual_snapshot["processed_lookup_keys"]:
        raise StateDbParityError("Processed lookup parity mismatch")
    if expected_snapshot["resume"] != actual_snapshot["resume"]:
        raise StateDbParityError("Resume canonical parity mismatch")
    if expected_snapshot["resume_lookup"] != actual_snapshot["resume_lookup"]:
        raise StateDbParityError("Resume lookup parity mismatch")
    if expected_snapshot["recent_successful_upload_profile"] != actual_snapshot["recent_successful_upload_profile"]:
        raise StateDbParityError("Recent successful upload profile parity mismatch")
    if expected_snapshot["failed_retry_set"] != actual_snapshot["failed_retry_set"]:
        raise StateDbParityError("Failed retry set parity mismatch")
    if expected_snapshot["last_failed_retry_state"] != actual_snapshot["last_failed_retry_state"]:
        raise StateDbParityError("Last failed retry state parity mismatch")


def load_recent_successful_upload_profile(db_path: str) -> RecentSuccessfulUploadProfile | None:
    snapshot = load_sqlite_snapshot(db_path)
    profile = snapshot["recent_successful_upload_profile"]
    if profile is None:
        return None
    cloned_profile: dict[str, Any] = dict(profile)
    if "values" in cloned_profile and isinstance(cloned_profile["values"], dict):
        cloned_profile["values"] = dict(cloned_profile["values"])
    return RecentSuccessfulUploadProfile(**cloned_profile)


def save_recent_successful_upload_profile(
    db_path: str,
    profile: RecentSuccessfulUploadProfile | None,
) -> None:
    ensure_bootstrap_database(db_path)
    updated_at = int(time.time())
    normalized_profile = _normalize_recent_successful_upload_profile(profile)
    connection = connect_state_db(db_path)
    try:
        with connection:
            connection.execute("DELETE FROM recent_successful_upload_profile")
            if normalized_profile is not None:
                connection.execute(
                    """
                    INSERT INTO recent_successful_upload_profile(singleton_id, profile_name, applied_at, values_json)
                    VALUES (1, ?, ?, ?)
                    """,
                    (
                        normalized_profile["profile_name"],
                        normalized_profile.get("applied_at"),
                        None if "values" not in normalized_profile else _serialize_json(normalized_profile["values"]),
                    ),
                )
            _upsert_state_meta(connection, "schema_version", DB_SCHEMA_VERSION, updated_at)
    finally:
        connection.close()


def load_failed_retry_set(db_path: str) -> tuple[FailedRetryEntry, ...]:
    snapshot = load_sqlite_snapshot(db_path)
    return snapshot["failed_retry_set"]


def load_file_state_rows(db_path: str) -> tuple[FileStateRow, ...]:
    ensure_bootstrap_database(db_path)
    connection = connect_state_db(db_path)
    try:
        rows = connection.execute(
            """
            SELECT
              file_key,
              legacy_key,
              folder,
              filename,
              state,
              resume_offset,
              last_error,
              retry_count,
              processed_at,
              failed_at,
              updated_at
            FROM file_state
            ORDER BY legacy_key, file_key
            """
        ).fetchall()
    finally:
        connection.close()
    normalized_rows: list[FileStateRow] = []
    for row in rows:
        normalized_rows.append(
            FileStateRow(
                file_key=str(row["file_key"]),
                legacy_key=str(row["legacy_key"]),
                folder=str(row["folder"]),
                filename=str(row["filename"]),
                state=str(row["state"]),
                resume_offset=int(row["resume_offset"]),
                last_error=None if row["last_error"] is None else str(row["last_error"]),
                retry_count=int(row["retry_count"]),
                processed_at=None if row["processed_at"] is None else int(row["processed_at"]),
                failed_at=None if row["failed_at"] is None else int(row["failed_at"]),
                updated_at=int(row["updated_at"]),
            )
        )
    return tuple(normalized_rows)


def clear_file_state_by_legacy_keys(
    db_path: str,
    legacy_keys: tuple[str, ...],
) -> int:
    ensure_bootstrap_database(db_path)
    normalized_legacy_keys = tuple(
        sorted({str(value).strip() for value in legacy_keys if str(value).strip() != ""})
    )
    if normalized_legacy_keys == ():
        return 0

    placeholders = ", ".join("?" for _ in normalized_legacy_keys)
    connection = connect_state_db(db_path)
    try:
        with connection:
            removed_file_key_rows = connection.execute(
                f"""
                SELECT file_key
                FROM file_state
                WHERE legacy_key IN ({placeholders})
                """,
                normalized_legacy_keys,
            ).fetchall()
            removed_file_keys = tuple(str(row["file_key"]) for row in removed_file_key_rows)
            delete_cursor = connection.execute(
                f"""
                DELETE FROM file_state
                WHERE legacy_key IN ({placeholders})
                """,
                normalized_legacy_keys,
            )
            if removed_file_keys != ():
                failed_placeholders = ", ".join("?" for _ in removed_file_keys)
                connection.execute(
                    f"""
                    DELETE FROM last_failed_retry_state
                    WHERE file_key IN ({failed_placeholders})
                    """,
                    removed_file_keys,
                )
            return int(delete_cursor.rowcount)
    finally:
        connection.close()


def start_upload_run(
    db_path: str,
    total_count: int,
    retry_failed_only: bool,
    config_values: dict[str, str],
) -> int:
    ensure_bootstrap_database(db_path)
    started_at = time.time()
    connection = connect_state_db(db_path)
    try:
        with connection:
            cursor = connection.execute(
                """
                INSERT INTO upload_runs(
                  started_at, completed_at, mode, config_json, status,
                  total_count, success_count, failure_count, warning_count
                )
                VALUES (?, NULL, ?, ?, 'running', ?, 0, 0, 0)
                """,
                (
                    started_at,
                    "retry_failed" if retry_failed_only else "default",
                    _serialize_json(config_values),
                    int(total_count),
                ),
            )
            run_id = int(cursor.lastrowid)
    finally:
        connection.close()
    return run_id


def finish_upload_run(
    db_path: str,
    run_id: int,
    total_count: int,
    success_count: int,
    failure_count: int,
    warning_messages: tuple[str, ...],
    recent_successful_upload_profile: RecentSuccessfulUploadProfile | None,
) -> None:
    ensure_bootstrap_database(db_path)
    completed_at = time.time()
    warning_count = len(warning_messages)
    status = "completed"
    if failure_count > 0:
        status = "partial_failure"
    elif warning_count > 0:
        status = "completed_with_warning"
    connection = connect_state_db(db_path)
    try:
        with connection:
            connection.execute(
                """
                UPDATE upload_runs
                SET completed_at = ?,
                    status = ?,
                    total_count = ?,
                    success_count = ?,
                    failure_count = ?,
                    warning_count = ?
                WHERE run_id = ?
                """,
                (
                    completed_at,
                    status,
                    int(total_count),
                    int(success_count),
                    int(failure_count),
                    int(warning_count),
                    int(run_id),
                ),
            )
            if failure_count == 0:
                connection.execute("DELETE FROM recent_successful_upload_profile")
                normalized_profile = _normalize_recent_successful_upload_profile(recent_successful_upload_profile)
                if normalized_profile is not None:
                    connection.execute(
                        """
                        INSERT INTO recent_successful_upload_profile(singleton_id, profile_name, applied_at, values_json)
                        VALUES (1, ?, ?, ?)
                        """,
                        (
                            normalized_profile["profile_name"],
                            normalized_profile.get("applied_at"),
                            None if "values" not in normalized_profile else _serialize_json(normalized_profile["values"]),
                        ),
                    )
    finally:
        connection.close()


def set_resume_offset(db_path: str, key: str, offset: int) -> None:
    ensure_bootstrap_database(db_path)
    file_key = str(key).strip()
    if file_key == "":
        raise StateDbError("Resume key must not be empty")
    legacy_key = _normalize_legacy_key(file_key)
    folder, filename = _split_legacy_key(legacy_key)
    updated_at = int(time.time())
    connection = connect_state_db(db_path)
    try:
        with connection:
            existing_row = _fetch_file_state_row(connection, file_key)
            if int(offset) <= 0:
                if existing_row is None:
                    return
                connection.execute("DELETE FROM failed_retry_set WHERE file_key = ?", (file_key,))
                next_state = str(existing_row["state"])
                processed_at = existing_row["processed_at"]
                if next_state != "completed":
                    next_state = "in_progress"
                    processed_at = None
                _upsert_file_state(
                    connection,
                    file_key,
                    legacy_key,
                    folder,
                    filename,
                    next_state,
                    0,
                    None if next_state != "failed" else existing_row["last_error"],
                    int(existing_row["retry_count"]),
                    None if processed_at is None else int(processed_at),
                    None,
                    updated_at,
                )
                _upsert_aliases(connection, file_key, legacy_key)
                return

            retry_count = 0 if existing_row is None else int(existing_row["retry_count"])
            _upsert_file_state(
                connection,
                file_key,
                legacy_key,
                folder,
                filename,
                "in_progress",
                int(offset),
                None,
                retry_count,
                None,
                None,
                updated_at,
            )
            _upsert_aliases(connection, file_key, legacy_key)
            connection.execute("DELETE FROM failed_retry_set WHERE file_key = ?", (file_key,))
    finally:
        connection.close()


def mark_file_completed(
    db_path: str,
    folder: str,
    filename: str,
    file_path: str,
    run_id: int | None,
) -> None:
    ensure_bootstrap_database(db_path)
    file_key = _build_runtime_file_key(folder, filename, file_path)
    legacy_key = _build_runtime_legacy_key(folder, filename)
    updated_at = int(time.time())
    connection = connect_state_db(db_path)
    try:
        with connection:
            existing_row = _fetch_file_state_row(connection, file_key)
            retry_count = 0 if existing_row is None else int(existing_row["retry_count"])
            _upsert_file_state(
                connection,
                file_key,
                legacy_key,
                folder,
                filename,
                "completed",
                0,
                None,
                retry_count,
                updated_at,
                None,
                updated_at,
            )
            _upsert_aliases(connection, file_key, legacy_key)
            connection.execute("DELETE FROM failed_retry_set WHERE file_key = ?", (file_key,))
            if run_id is not None:
                _upsert_state_meta(connection, "last_completed_run_id", int(run_id), updated_at)
    finally:
        connection.close()


def record_file_failure(
    db_path: str,
    folder: str,
    filename: str,
    file_path: str,
    resume_offset: int,
    error_message: str,
    run_id: int | None,
) -> None:
    ensure_bootstrap_database(db_path)
    file_key = _build_runtime_file_key(folder, filename, file_path)
    legacy_key = _build_runtime_legacy_key(folder, filename)
    normalized_offset = max(int(resume_offset), 1)
    normalized_error_message = str(error_message).strip()
    updated_at = int(time.time())
    connection = connect_state_db(db_path)
    try:
        with connection:
            resolved_run_id = _resolve_existing_run_id(connection, run_id)
            existing_row = _fetch_file_state_row(connection, file_key)
            existing_failed_row = connection.execute(
                """
                SELECT retry_count
                FROM failed_retry_set
                WHERE file_key = ?
                """,
                (file_key,),
            ).fetchone()
            retry_count = 1
            if existing_row is not None:
                retry_count = max(retry_count, int(existing_row["retry_count"]) + 1)
            if existing_failed_row is not None:
                retry_count = max(retry_count, int(existing_failed_row["retry_count"]) + 1)
            _upsert_file_state(
                connection,
                file_key,
                legacy_key,
                folder,
                filename,
                "failed",
                normalized_offset,
                normalized_error_message if normalized_error_message != "" else None,
                retry_count,
                None,
                updated_at,
                updated_at,
            )
            _upsert_aliases(connection, file_key, legacy_key)
            connection.execute(
                """
                INSERT INTO failed_retry_set(
                  file_key, resume_offset, error_message, retry_count, failed_at, last_run_id
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(file_key) DO UPDATE SET
                  resume_offset=excluded.resume_offset,
                  error_message=excluded.error_message,
                  retry_count=excluded.retry_count,
                  failed_at=excluded.failed_at,
                  last_run_id=excluded.last_run_id
                """,
                (
                    file_key,
                    normalized_offset,
                        normalized_error_message if normalized_error_message != "" else None,
                        retry_count,
                        float(updated_at),
                        resolved_run_id,
                    ),
                )
    finally:
        connection.close()


def backup_legacy_state(
    manifest_path: str,
    log_path: str,
    resume_path: str,
    db_path: str,
    backup_root: str,
) -> str:
    os.makedirs(backup_root, exist_ok=True)
    timestamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    backup_dir = os.path.join(backup_root, timestamp)
    os.makedirs(backup_dir, exist_ok=True)
    copied_files: list[dict[str, Any]] = []
    for source_path in [manifest_path, log_path, resume_path, db_path]:
        if not os.path.exists(source_path):
            continue
        target_path = os.path.join(backup_dir, os.path.basename(source_path))
        shutil.copy2(source_path, target_path)
        copied_files.append(
            {
                "name": os.path.basename(source_path),
                "size": os.path.getsize(source_path),
                "mtime_ns": os.stat(source_path).st_mtime_ns,
                "sha256": _compute_file_hash(source_path),
            }
        )
    with open(os.path.join(backup_dir, "source_manifest.json"), "w", encoding="utf-8") as file_handle:
        json.dump({"files": copied_files}, file_handle, ensure_ascii=False, indent=2, sort_keys=True)
    return backup_dir


def _read_source_fingerprint(db_path: str) -> str | None:
    if not os.path.exists(db_path):
        return None
    try:
        integrity_check(db_path)
        connection = connect_state_db(db_path)
        row = connection.execute(
            """
            SELECT value_json
            FROM state_meta
            WHERE key = 'source_fingerprint'
            """
        ).fetchone()
        connection.close()
    except StateDbCorruptionError:
        raise
    except json.JSONDecodeError as error:
        raise StateDbCorruptionError(f"Invalid source_fingerprint metadata in SQLite state database: {db_path}") from error
    except sqlite3.DatabaseError as error:
        raise StateDbCorruptionError(f"Corrupted SQLite state database: {db_path}") from error
    if row is None:
        return None
    try:
        return str(json.loads(str(row["value_json"])))
    except json.JSONDecodeError as error:
        raise StateDbCorruptionError(f"Invalid source_fingerprint metadata in SQLite state database: {db_path}") from error


def ensure_sqlite_snapshot_from_legacy(
    manifest_path: str,
    log_path: str,
    resume_path: str,
    db_path: str,
    backup_root: str,
) -> SQLiteBootstrapResult:
    legacy_source_exists = any(os.path.exists(path) for path in [manifest_path, log_path, resume_path])
    if not legacy_source_exists and os.path.exists(db_path):
        raise StateDbImportError("Legacy state source is missing while SQLite snapshot already exists")

    bundle = build_legacy_snapshot(manifest_path, log_path, resume_path)
    expected_snapshot = bundle["parity_snapshot"]
    existing_fingerprint = _read_source_fingerprint(db_path) if os.path.exists(db_path) else None
    if existing_fingerprint == expected_snapshot["source_fingerprint"]:
        actual_snapshot = load_sqlite_snapshot(db_path)
        parity_check(expected_snapshot, actual_snapshot)
        return {
            "backup_dir": None,
            "db_path": db_path,
            "imported": False,
            "source_fingerprint": expected_snapshot["source_fingerprint"],
        }

    backup_dir = backup_legacy_state(manifest_path, log_path, resume_path, db_path, backup_root)
    temp_db_path = db_path + ".tmp"
    if os.path.exists(temp_db_path):
        os.remove(temp_db_path)

    ensure_bootstrap_database(temp_db_path)
    connection = connect_state_db(temp_db_path)
    try:
        with connection:
            _import_bundle(connection, bundle)
    finally:
        connection.close()

    actual_snapshot = load_sqlite_snapshot(temp_db_path)
    parity_check(expected_snapshot, actual_snapshot)
    os.replace(temp_db_path, db_path)
    return {
        "backup_dir": backup_dir,
        "db_path": db_path,
        "imported": True,
        "source_fingerprint": expected_snapshot["source_fingerprint"],
    }
