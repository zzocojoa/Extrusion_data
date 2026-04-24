import os
import sys
import inspect
import re
import threading
import queue
import shutil
from dataclasses import dataclass
from pathlib import Path
from types import ModuleType
from typing import Any, Callable, Mapping
from urllib.parse import urlparse

from datetime import datetime, timedelta, timezone
import subprocess
import time
import webbrowser

from core.config import (
    ConfigLoadMetadata,
    get_data_dir,
    load_config as core_load_config,
    load_config_with_sources as core_load_config_with_sources,
    save_config as core_save_config,
    compute_edge_url,
    is_edge_url_origin_mismatch,
    normalize_edge_url,
    validate_config,
)
from core.i18n import (
    DEFAULT_UI_LANGUAGE,
    SUPPORTED_UI_LANGUAGES,
    TranslationBundle,
    load_translation_bundle,
    normalize_language_code,
    translate,
)
import core.state as core_state
from core import files as core_files
try:
    from core import wsl_storage as core_wsl_storage
except Exception:
    core_wsl_storage = None
from core.archive_metrics import (
    ArchiveStats,
    DEFAULT_ARCHIVE_CHUNK_SIZE,
    DbConnectionSettings,
    build_archive_output_path,
    delete_archived_all_metrics,
    export_all_metrics_to_parquet,
    load_archive_environment,
    parse_archive_before_date,
    read_all_metrics_archive_stats,
    read_parquet_archive_stats,
    read_local_db_port,
    resolve_archive_dir,
    resolve_db_connection_settings,
    validate_archive_stats_match,
)
from core.cycle_operations import (
    CycleHealthReport,
    execute_canonical_refresh,
    execute_cycle_health_check,
    execute_cycle_snapshot_sync,
    resolve_cycle_db_connection_settings,
)


# Tkinter UI
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

KST = timezone(timedelta(hours=9))
PROJECT_ROOT = Path(__file__).resolve().parent
PREVIEW_VALIDATION_SAMPLE_ROWS = 32
STARTUP_PROFILE_ENV_NAME = "EXTRUSION_STARTUP_PROFILE"

# Data directory (AppData) for persistent state
# Data directory (AppData) for persistent state
DATA_DIR = get_data_dir()
STATE_DB_PATH = core_state.get_db_path()

def resource_path(relative_path):
    """ Get absolute path to resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.dirname(os.path.abspath(__file__))

    return os.path.join(base_path, relative_path)

# Icon path for window/taskbar (local asset)
APP_ICON = resource_path(os.path.join('assets', 'app.ico'))

# Set explicit AppUserModelID on Windows so taskbar uses our icon
if os.name == 'nt':
    try:
        import ctypes

        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("ExtrusionUploader")
    except Exception:
        pass

def kst_now() -> datetime:
    return datetime.now(KST)


def load_processed() -> set:
    return core_state.load_processed(STATE_DB_PATH)


def log_processed(folder: str, filename: str, file_path: str):
    core_state.log_processed(folder, filename, file_path, STATE_DB_PATH)


# --- Resume state (파일별 마지막 배치 오프셋) ---
def load_resume() -> dict:
    return core_state.load_resume(STATE_DB_PATH)


def save_resume(data: dict):
    core_state.save_resume(data, STATE_DB_PATH)


def set_resume_offset(key: str, offset: int):
    core_state.set_resume_offset(key, offset, STATE_DB_PATH)


def get_resume_offset(key: str) -> int:
    return core_state.get_resume_offset(key, STATE_DB_PATH)


def load_recent_successful_upload_profile() -> dict[str, object] | None:
    return core_state.load_recent_successful_upload_profile(STATE_DB_PATH)


def save_recent_successful_upload_profile(profile: dict[str, object] | None) -> None:
    core_state.save_recent_successful_upload_profile(profile, STATE_DB_PATH)


def load_failed_retry_set() -> tuple[core_state.FailedRetryEntry, ...]:
    return core_state.load_failed_retry_set(STATE_DB_PATH)


def load_state_health_snapshot(verify_integrity: bool) -> core_state.StateHealthSnapshot:
    return core_state.load_state_health(STATE_DB_PATH, verify_integrity)


def load_upload_dashboard_state_snapshot() -> core_state.UploadDashboardStateSnapshot:
    return core_state.load_upload_dashboard_state(STATE_DB_PATH)


def start_upload_run(total_count: int, retry_failed_only: bool, config_values: dict[str, str]) -> int:
    return core_state.start_upload_run(total_count, retry_failed_only, config_values, STATE_DB_PATH)


def finish_upload_run(
    run_id: int,
    total_count: int,
    success_count: int,
    failure_count: int,
    warning_messages: tuple[str, ...],
    recent_successful_upload_profile: dict[str, object] | None,
) -> None:
    core_state.finish_upload_run(
        run_id,
        total_count,
        success_count,
        failure_count,
        warning_messages,
        recent_successful_upload_profile,
        STATE_DB_PATH,
    )


def mark_file_completed(folder: str, filename: str, file_path: str, run_id: int | None) -> None:
    core_state.mark_file_completed(folder, filename, file_path, run_id, STATE_DB_PATH)


def record_file_failure(
    folder: str,
    filename: str,
    file_path: str,
    resume_offset: int,
    error_message: str,
    run_id: int | None,
) -> None:
    core_state.record_file_failure(
        folder,
        filename,
        file_path,
        resume_offset,
        error_message,
        run_id,
        STATE_DB_PATH,
    )


def load_pandas_module() -> ModuleType:
    import pandas as pd

    return pd


def load_core_transform_module() -> ModuleType:
    import core.transform as core_transform

    return core_transform


def load_core_upload_module() -> ModuleType:
    import core.upload as core_upload

    return core_upload


def load_core_work_log_module() -> ModuleType:
    import core.work_log as core_work_log

    return core_work_log


def load_core_cycle_module() -> ModuleType:
    import core.cycle_processing as core_cycle

    return core_cycle


def load_build_training_base() -> Callable[[str, str, str, str | None], str]:
    from scripts.build_training_base import build_training_base

    return build_training_base


def load_build_training_dataset_v1() -> Callable[[str, str], str]:
    from scripts.build_training_dataset_v1 import build_training_dataset_v1

    return build_training_dataset_v1


def build_records_plc(file_path: str, filename: str, chunksize: int | None = None) -> Any:
    return load_core_transform_module().build_records_plc(file_path, filename, chunksize)


def is_locked(path: str) -> bool:
    return core_files.is_locked(path)


def file_mtime_kst(path: str) -> datetime:
    return core_files.file_mtime_kst(path)


def parse_plc_date_from_filename(name: str) -> datetime | None:
    return core_files.parse_plc_date_from_filename(name)


def parse_temp_end_date_from_filename(name: str) -> datetime | None:
    return core_files.parse_temp_end_date_from_filename(name)


def stable_enough(path: str, lag_minutes: int) -> bool:
    return core_files.stable_enough(path, lag_minutes)


def load_config(path: str | None) -> tuple[dict[str, str], str]:
    return core_load_config(path)


def load_config_with_sources(path: str | None) -> tuple[dict[str, str], str, ConfigLoadMetadata]:
    return core_load_config_with_sources(path)


def save_config(values: dict[str, str], path: str | None) -> str:
    return core_save_config(values, path)


def _normalize_host_name(host: str) -> str:
    normalized = host.strip().lower()
    if normalized in {"localhost", "127.0.0.1", "::1"}:
        return "local"
    return normalized


def _describe_config_source(
    config_path: str,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> str:
    if not config_path.strip():
        return translate_fn("common.unknown", {})

    resolved_path = Path(config_path).resolve()
    if resolved_path.parent == Path(DATA_DIR).resolve():
        return translate_fn("settings.runtime.config_source.appdata", {})
    if resolved_path.name == "config.ini":
        return str(resolved_path)
    return str(resolved_path)


def _build_source_summary(
    metadata: ConfigLoadMetadata,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> str:
    source_values = set(metadata["source_by_key"].values())
    active_overrides: list[str] = []
    if ".env" in source_values:
        active_overrides.append(".env")
    if "os.environ" in source_values:
        active_overrides.append("os.environ")
    if "default" in source_values and active_overrides == []:
        active_overrides.append(translate_fn("common.none", {}))
    if active_overrides == []:
        active_overrides.append(translate_fn("common.none", {}))
    return translate_fn(
        "settings.runtime.source_summary",
        {
            "config_source": _describe_config_source(metadata["config_path"], translate_fn),
            "override_source": ", ".join(active_overrides),
        },
    )


def _build_edge_runtime_state(
    supabase_url: str,
    edge_url: str,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> tuple[str, str, bool]:
    resolved_edge = compute_edge_url(
        {
            "SUPABASE_URL": supabase_url,
            "EDGE_FUNCTION_URL": edge_url,
        }
    ).strip()
    default_edge = compute_edge_url(
        {
            "SUPABASE_URL": supabase_url,
            "EDGE_FUNCTION_URL": "",
        }
    ).strip()

    if not resolved_edge:
        unknown_text = translate_fn("common.unknown", {})
        return unknown_text, unknown_text, False

    manual_override = bool(edge_url.strip()) and resolved_edge != default_edge
    if manual_override:
        edge_state = translate_fn("settings.runtime.edge_state.manual_override", {})
    else:
        edge_state = translate_fn("settings.runtime.edge_state.auto", {})

    resolved_host = _normalize_host_name(urlparse(resolved_edge).hostname or "")
    default_host = _normalize_host_name(urlparse(default_edge).hostname or "")
    host_mismatch = manual_override and resolved_host != default_host and default_host != ""
    if host_mismatch:
        edge_state = translate_fn(
            "settings.runtime.edge_state.host_mismatch",
            {"base_state": edge_state},
        )

    return resolved_edge, edge_state, host_mismatch


def build_runtime_context_text(
    metadata: ConfigLoadMetadata,
    supabase_url: str,
    edge_url: str,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> str:
    config_text = _describe_config_source(metadata["config_path"], translate_fn)
    edge_text, edge_state, host_mismatch = _build_edge_runtime_state(
        supabase_url,
        edge_url,
        translate_fn,
    )
    lines = [
        translate_fn("settings.runtime.config_path", {"config_path": config_text}),
        _build_source_summary(metadata, translate_fn),
        translate_fn("settings.runtime.edge_state.label", {"edge_state": edge_state}),
        translate_fn("settings.runtime.upload_url", {"upload_url": edge_text}),
    ]
    if host_mismatch:
        lines.append(translate_fn("settings.runtime.edge_host_mismatch", {}))
    return "\n".join(lines)


def resolve_custom_range_texts(
    custom_date_start: str,
    custom_date_end: str,
    legacy_custom_date: str,
) -> tuple[str, str]:
    return core_files.resolve_custom_range_texts(custom_date_start, custom_date_end, legacy_custom_date)


def compute_date_window(
    mode: str,
    custom_date_start: str,
    custom_date_end: str,
) -> tuple["date | None", "date"]:
    return core_files.compute_date_window(mode, custom_date_start, custom_date_end)


def within_date_window(file_date: datetime, window_start: "date | None", window_end: "date") -> bool:
    return core_files.within_date_window(file_date, window_start, window_end)


def format_optional_timestamp_text(raw_value: object) -> str:
    if raw_value is None:
        return ""
    try:
        timestamp = float(raw_value)
    except Exception:
        return ""
    if timestamp <= 0:
        return ""
    return datetime.fromtimestamp(timestamp, KST).strftime("%Y-%m-%d %H:%M:%S")


def build_upload_selection_fingerprint(
    vals: Mapping[str, str],
) -> "UploadSelectionFingerprint":
    return UploadSelectionFingerprint(
        plc_dir=str(vals.get("PLC_DIR", "")).strip(),
        range_mode=str(vals.get("RANGE_MODE", "")).strip(),
        custom_date_start=str(vals.get("CUSTOM_DATE_START", "")).strip(),
        custom_date_end=str(vals.get("CUSTOM_DATE_END", "")).strip(),
        custom_date=str(vals.get("CUSTOM_DATE", "")).strip(),
        mtime_lag_min=str(vals.get("MTIME_LAG_MIN", "")).strip(),
        check_lock=str(vals.get("CHECK_LOCK", "")).strip().lower(),
    )


def read_directory_mtime_ns(folder: str) -> int | None:
    try:
        return int(os.stat(folder).st_mtime_ns)
    except OSError:
        return None


def build_preview_candidate_snapshots(
    items: list[tuple[str, str, str, str]],
) -> tuple["PreviewCandidateSnapshot", ...]:
    snapshots: list[PreviewCandidateSnapshot] = []
    for folder, filename, path, kind in items:
        stat_result = os.stat(path)
        snapshots.append(
            PreviewCandidateSnapshot(
                folder=folder,
                filename=filename,
                path=path,
                kind=kind,
                size=int(stat_result.st_size),
                mtime_ns=int(stat_result.st_mtime_ns),
            )
        )
    return tuple(snapshots)


def build_preview_preflight_detail_lines(
    target_count: int,
    total_count: int,
    excluded: list[tuple[str, str, str]],
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> tuple[str, ...]:
    detail_lines = [
        translate_fn(
            "dashboard.upload.detail.preflight_summary",
            {"target_count": target_count, "total_count": total_count},
        )
    ]
    if excluded != []:
        detail_lines.append(
            translate_fn(
                "dashboard.upload.detail.preflight_excluded",
                {"excluded_count": len(excluded)},
            )
        )
        for _, filename, reason in excluded[:2]:
            detail_lines.append(f"{filename}: {reason}")
    return tuple(detail_lines[:4])


def build_preview_scan_result(
    vals: Mapping[str, str],
    items: list[tuple[str, str, str, str]],
    excluded: list[tuple[str, str, str]],
    completed_at: float,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> "PreviewScanResult":
    fingerprint = build_upload_selection_fingerprint(vals)
    total_count = len(items) + len(excluded)
    detail_lines = build_preview_preflight_detail_lines(
        len(items),
        total_count,
        excluded,
        translate_fn,
    )
    return PreviewScanResult(
        fingerprint=fingerprint,
        completed_at=completed_at,
        directory_mtime_ns=read_directory_mtime_ns(fingerprint.plc_dir),
        total_count=total_count,
        target_count=len(items),
        excluded_count=len(excluded),
        candidate_items=build_preview_candidate_snapshots(items),
        detail_lines=detail_lines,
    )


def can_reuse_preview_scan_for_upload(
    preview_scan_result: "PreviewScanResult | None",
    fingerprint: "UploadSelectionFingerprint",
    include_today: bool,
    retry_failed_only: bool,
    current_time: float,
) -> bool:
    if preview_scan_result is None:
        return False
    if retry_failed_only:
        return False
    if include_today:
        return False
    if preview_scan_result.fingerprint != fingerprint:
        return False
    if current_time - preview_scan_result.completed_at > PREVIEW_REUSE_TTL_SECONDS:
        return False
    current_directory_mtime_ns = read_directory_mtime_ns(fingerprint.plc_dir)
    return current_directory_mtime_ns == preview_scan_result.directory_mtime_ns


def collect_reusable_preview_candidate_items(
    preview_scan_result: "PreviewScanResult",
    processed: set[str],
    window_start: "date | None",
    window_end: "date",
    lag_min: int,
    include_today: bool,
    check_lock: bool,
) -> tuple[tuple[str, str, str, str], ...] | None:
    reusable_items: list[tuple[str, str, str, str]] = []
    for candidate_snapshot in preview_scan_result.candidate_items:
        try:
            stat_result = os.stat(candidate_snapshot.path)
        except OSError:
            return None
        if int(stat_result.st_size) != candidate_snapshot.size:
            return None
        if int(stat_result.st_mtime_ns) != candidate_snapshot.mtime_ns:
            return None
        lookup_keys = core_state.build_file_state_lookup_keys(
            candidate_snapshot.folder,
            candidate_snapshot.filename,
            candidate_snapshot.path,
        )
        if any(lookup_key in processed for lookup_key in lookup_keys):
            return None
        if candidate_snapshot.kind != "plc":
            return None
        file_date = parse_plc_date_from_filename(candidate_snapshot.filename)
        if file_date is None or not within_date_window(file_date, window_start, window_end):
            return None
        if file_date.date() == kst_now().date() and include_today:
            if not stable_enough(candidate_snapshot.path, lag_min):
                return None
            if check_lock and is_locked(candidate_snapshot.path):
                return None
        reusable_items.append(
            (
                candidate_snapshot.folder,
                candidate_snapshot.filename,
                candidate_snapshot.path,
                candidate_snapshot.kind,
            )
        )
    return tuple(reusable_items)


def collect_retryable_upload_items_from_state(
    items: list[tuple[str, str, str, str]],
    resume_map: dict[str, int],
    failed_retry_set: tuple[core_state.FailedRetryEntry, ...],
) -> tuple[tuple[str, str, str, str], ...]:
    active_retry_keys = {
        key
        for key, offset in resume_map.items()
        if int(offset) > 0
    }
    for failed_retry_entry in failed_retry_set:
        failed_key = str(failed_retry_entry.get("file_key", "")).strip()
        if failed_key != "":
            active_retry_keys.add(failed_key)
            if "|" in failed_key:
                active_retry_keys.add(failed_key.split("|", 1)[0])

    retryable_items: list[tuple[str, str, str, str]] = []
    for folder, filename, path, kind in items:
        lookup_keys = core_state.build_file_state_lookup_keys(folder, filename, path)
        if any(lookup_key in active_retry_keys for lookup_key in lookup_keys):
            retryable_items.append((folder, filename, path, kind))
    return tuple(retryable_items)


def process_file(kind: str, path: str, filename: str) -> Any:
    pd = load_pandas_module()
    try:
        # Single mode: always use build_records_plc (now integrated)
        return build_records_plc(path, filename)
    except Exception:
        pass
    return pd.DataFrame()


def preview_diagnostics(
    plc_dir: str,
    temp_dir: str | None,
    window_start: "date | None",
    window_end: "date",
    lag_min: int,
    include_today: bool,
    check_lock: bool,
    processed: set[str],
    translate_fn: Callable[[str, Mapping[str, object]], str],
):
    import core.state as core_state

    included = []  # (folder, filename, path, kind)
    excluded = []  # (folder, filename, reason)

    def has_data(kind: str, path: str, filename: str) -> bool:
        del filename
        pd = load_pandas_module()
        try:
            return core_files.preview_has_data(kind, path, PREVIEW_VALIDATION_SAMPLE_ROWS)
        except (OSError, UnicodeError, ValueError, pd.errors.EmptyDataError, pd.errors.ParserError):
            return False

    # PLC
    if os.path.isdir(plc_dir):
        for entry in core_files._iter_sorted_csv_entries(plc_dir):
            fn = entry.name
            full = entry.path
            fdate = parse_plc_date_from_filename(fn)
            if not fdate or not within_date_window(fdate, window_start, window_end):
                excluded.append((plc_dir, fn, translate_fn("dashboard.preview.excluded.out_of_range", {})))
                continue
            lookup_keys = core_state.build_file_state_lookup_keys(plc_dir, fn, full)
            if any(lookup_key in processed for lookup_key in lookup_keys):
                excluded.append((plc_dir, fn, translate_fn("dashboard.preview.excluded.already_processed", {})))
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(full, lag_min):
                    excluded.append(
                        (
                            plc_dir,
                            fn,
                            translate_fn(
                                "dashboard.preview.excluded.unstable_today",
                                {"lag_min": lag_min},
                            ),
                        )
                    )
                    continue
                if check_lock and is_locked(full):
                    excluded.append((plc_dir, fn, translate_fn("dashboard.preview.excluded.locked", {})))
                    continue
            # content check
            if has_data('plc', full, fn):
                included.append((plc_dir, fn, full, 'plc'))
            else:
                excluded.append((plc_dir, fn, translate_fn("dashboard.preview.excluded.no_data", {})))

    # Temperature
    if temp_dir and os.path.isdir(temp_dir):
        for fn in sorted(os.listdir(temp_dir)):
            full = os.path.join(temp_dir, fn)
            if not fn.lower().endswith('.csv'):
                excluded.append((temp_dir, fn, translate_fn("dashboard.preview.excluded.not_csv", {})))
                continue
            fdate = parse_temp_end_date_from_filename(fn)
            if not fdate:
                try:
                    fdate = file_mtime_kst(full)
                except Exception:
                    fdate = None
            if not fdate or not within_date_window(fdate, window_start, window_end):
                excluded.append((temp_dir, fn, translate_fn("dashboard.preview.excluded.out_of_range", {})))
                continue
            lookup_keys = core_state.build_file_state_lookup_keys(temp_dir, fn, full)
            if any(lookup_key in processed for lookup_key in lookup_keys):
                excluded.append((temp_dir, fn, translate_fn("dashboard.preview.excluded.already_processed", {})))
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(full, lag_min):
                    excluded.append(
                        (
                            temp_dir,
                            fn,
                            translate_fn(
                                "dashboard.preview.excluded.unstable_today",
                                {"lag_min": lag_min},
                            ),
                        )
                    )
                    continue
                if check_lock and is_locked(full):
                    excluded.append((temp_dir, fn, translate_fn("dashboard.preview.excluded.locked", {})))
                    continue
            if has_data('temp', full, fn):
                included.append((temp_dir, fn, full, 'temp'))
            else:
                excluded.append((temp_dir, fn, translate_fn("dashboard.preview.excluded.no_data", {})))

    return included, excluded

import calendar
import customtkinter as ctk

# Set theme
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

TRAINING_MODE_OPTIONS: tuple[str, ...] = (
    "build-all",
    "build-base",
    "build-v1",
)


@dataclass(frozen=True)
class TrainingBuildRequest:
    mode: str
    plc_file_path: str
    spot_file_path: str
    training_base_file_path: str
    training_base_output_path: str
    training_dataset_output_path: str
    filename_hint: str


@dataclass(frozen=True)
class ArchiveMetricsRequest:
    before_date: str
    archive_dir: str


@dataclass(frozen=True)
class ArchiveJobContext:
    db_settings: DbConnectionSettings
    before_datetime: datetime
    output_path: Path
    source_stats: ArchiveStats


@dataclass(frozen=True)
class LegacyCycleRequest:
    machine_id: str
    mode: str
    custom_date: str


@dataclass(frozen=True)
class LocalSupabaseRuntime:
    project_root: Path
    startup_script_path: Path
    api_host: str
    api_port: int
    db_host: str
    db_port: int
    studio_host: str
    studio_port: int


@dataclass(frozen=True)
class LocalSupabaseUiState:
    status_text: str
    status_color: str
    show_progress: bool
    start_button_text: str
    start_button_enabled: bool
    studio_button_text: str
    studio_button_enabled: bool
    stop_button_text: str
    stop_button_enabled: bool


@dataclass(frozen=True)
class LocalSupabaseStatusOverride:
    status_text: str
    status_color: str


@dataclass(frozen=True)
class LocalSupabaseStatusSnapshot:
    supabase_url: str
    runtime: LocalSupabaseRuntime | None
    is_ready: bool
    is_studio_ready: bool


@dataclass(frozen=True)
class LocalDockerCheckResult:
    is_ready: bool
    detail: str


@dataclass(frozen=True)
class WslStorageSnapshot:
    state: str
    status_text: str
    status_color: str
    used_label_text: str
    available_label_text: str
    total_label_text: str
    usage_label_text: str
    used_text: str
    available_text: str
    total_text: str
    usage_text: str
    vhdx_text: str
    host_free_text: str
    distro_text: str
    source_text: str
    detail_text: str
    last_updated_text: str
    progress_value: float | None
    is_refreshing: bool
    is_partial: bool
    is_available: bool


@dataclass(frozen=True)
class ResponsiveLayoutState:
    window_width: int
    is_compact: bool
    sidebar_width: int
    main_pad_x: int
    main_pad_y: int
    logo_wraplength: int


@dataclass(frozen=True)
class LabelWrapBinding:
    widget: tk.Misc
    container: tk.Misc
    horizontal_padding: int
    min_wraplength: int


@dataclass(frozen=True)
class ResponsiveLayoutModeSignature:
    is_compact: bool
    sidebar_width: int
    main_pad_x: int
    main_pad_y: int
    logo_wraplength: int


@dataclass(frozen=True)
class LabelWrapSignature:
    container_width: int
    wraplength: int


@dataclass(frozen=True)
class WidgetLayoutSignature:
    layout_name: str
    mode: str
    child_count: int
    detail: str


@dataclass(frozen=True)
class DashboardLayoutSignature:
    is_split_body: bool
    metric_column_count: int
    info_column_count: int
    is_split_footer: bool
    button_stack: bool


@dataclass(frozen=True)
class WorkLogViewState:
    selected_path: str
    messages: tuple[str, ...]


@dataclass(frozen=True)
class UploadOperationalCardsState:
    recent_successful_upload_profile: core_state.RecentSuccessfulUploadProfile | None
    failed_retry_set: tuple[core_state.FailedRetryEntry, ...]
    state_health_status_text: str
    state_health_status_color: str
    state_health_detail_lines: tuple[str, ...]
    state_health_blocks_upload: bool
    is_upload_preflight_blocked: bool
    has_retryable_state: bool
    retryable_upload_items: tuple[tuple[str, str, str, str], ...]
    preflight_status_text: str
    preflight_status_color: str
    preflight_detail_lines: tuple[str, ...]
    resume_status_text: str
    resume_status_color: str
    resume_detail_lines: tuple[str, ...]
    recent_success_status_text: str
    recent_success_status_color: str
    recent_success_detail_lines: tuple[str, ...]
    can_rerun_recent_success: bool


@dataclass(frozen=True)
class UploadSelectionFingerprint:
    plc_dir: str
    range_mode: str
    custom_date_start: str
    custom_date_end: str
    custom_date: str
    mtime_lag_min: str
    check_lock: str


@dataclass(frozen=True)
class PreviewCandidateSnapshot:
    folder: str
    filename: str
    path: str
    kind: str
    size: int
    mtime_ns: int


@dataclass(frozen=True)
class PreviewScanResult:
    fingerprint: UploadSelectionFingerprint
    completed_at: float
    directory_mtime_ns: int | None
    total_count: int
    target_count: int
    excluded_count: int
    candidate_items: tuple[PreviewCandidateSnapshot, ...]
    detail_lines: tuple[str, ...]


@dataclass(frozen=True)
class StartupTimingEntry:
    label: str
    elapsed_seconds: float


WSL_STORAGE_WARNING_THRESHOLD = 0.80
WSL_STORAGE_CRITICAL_THRESHOLD = 0.90
PREVIEW_REUSE_TTL_SECONDS = 30.0


def _extract_source_value(source: object, name: str) -> object | None:
    if isinstance(source, Mapping):
        if name in source:
            return source[name]
        return None
    if hasattr(source, name):
        return getattr(source, name)
    return None


def is_startup_profiling_enabled() -> bool:
    return os.environ.get(STARTUP_PROFILE_ENV_NAME, "").strip() == "1"


def format_startup_timing_report(
    startup_timing_entries: tuple[StartupTimingEntry, ...],
    total_seconds: float,
) -> str:
    lines = [
        "[startup] timing report",
        f"[startup] total={total_seconds:.3f}s",
    ]
    for startup_timing_entry in startup_timing_entries:
        lines.append(
            f"[startup] {startup_timing_entry.label}={startup_timing_entry.elapsed_seconds:.3f}s"
        )
    return "\n".join(lines)


def _coerce_optional_int(raw_value: object) -> int | None:
    if raw_value is None or isinstance(raw_value, bool):
        return None
    try:
        return int(raw_value)
    except Exception:
        return None


def _coerce_optional_float(raw_value: object) -> float | None:
    if raw_value is None or isinstance(raw_value, bool):
        return None
    try:
        return float(raw_value)
    except Exception:
        return None


def _normalize_wsl_storage_state(raw_state: object) -> str:
    state = str(raw_state).strip().lower()
    if state in {"safe", "ok", "healthy", "ready", "normal"}:
        return "safe"
    if state in {"warning", "warn", "caution"}:
        return "warning"
    if state in {"critical", "risk", "danger"}:
        return "critical"
    if state in {"partial", "degraded", "limited"}:
        return "partial"
    if state in {"error", "failed", "failure"}:
        return "error"
    if state in {"unavailable", "missing", "not_found", "not found"}:
        return "unavailable"
    if state in {"refreshing", "loading", "checking"}:
        return "refreshing"
    return state


def _format_storage_bytes(value: int | None) -> str:
    if value is None or value < 0:
        return "—"

    units = ("B", "KB", "MB", "GB", "TB", "PB")
    amount = float(value)
    unit_index = 0
    while amount >= 1024.0 and unit_index < len(units) - 1:
        amount /= 1024.0
        unit_index += 1
    if unit_index == 0:
        return f"{int(amount)} {units[unit_index]}"
    return f"{amount:.1f} {units[unit_index]}"


def _format_storage_timestamp(raw_value: object) -> str:
    if not isinstance(raw_value, datetime):
        return "—"
    return raw_value.astimezone(KST).strftime("%Y-%m-%d %H:%M:%S")


def _format_compact_path(raw_value: object, max_length: int) -> str:
    if not isinstance(raw_value, str):
        return ""
    normalized_path = raw_value.strip()
    if normalized_path == "":
        return ""
    if len(normalized_path) <= max_length:
        return normalized_path
    tail_length = max(max_length - 3, 8)
    return "..." + normalized_path[-tail_length:]


def _format_storage_ratio(raw_value: float | None) -> str:
    if raw_value is None:
        return "—"
    return f"{raw_value * 100:.0f}%"


def _resolve_wsl_storage_status_color(state: str) -> str:
    normalized_state = _normalize_wsl_storage_state(state)
    if normalized_state == "safe":
        return "#2CC985"
    if normalized_state in {"warning", "partial"}:
        return "#E5C07B"
    if normalized_state in {"critical", "error"}:
        return "#E06C75"
    if normalized_state == "refreshing":
        return "#3B8ED0"
    return "gray"


def normalize_optional_training_path(raw_value: str) -> Path | None:
    normalized_value = raw_value.strip()
    if normalized_value == "":
        return None
    return Path(normalized_value).resolve()


def normalize_required_training_path(
    raw_value: str,
    field_name: str,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> Path:
    normalized_path = normalize_optional_training_path(raw_value)
    if normalized_path is None:
        raise ValueError(
            translate_fn(
                "data_mgmt.training.validation.path_required",
                {"field_name": field_name},
            )
        )
    return normalized_path


def resolve_training_filename_hint(
    raw_value: str,
    plc_file_path: Path | None,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> str:
    normalized_value = raw_value.strip()
    if normalized_value != "":
        return normalized_value
    if plc_file_path is None:
        raise ValueError(
            translate_fn("data_mgmt.training.validation.filename_hint_required", {})
        )
    return plc_file_path.name


def normalize_optional_archive_dir(raw_value: str) -> str | None:
    normalized_value = raw_value.strip()
    if normalized_value == "":
        return None
    return normalized_value


def is_loopback_host(host: str) -> bool:
    normalized_host = host.strip().lower()
    return normalized_host in {"localhost", "127.0.0.1", "::1"}


def parse_http_endpoint(url_text: str) -> tuple[str, int]:
    parsed_url = urlparse(url_text.strip())
    host = parsed_url.hostname or ""
    if parsed_url.port is not None:
        return host, parsed_url.port
    if parsed_url.scheme == "https":
        return host, 443
    return host, 80


def can_connect_tcp(host: str, port: int) -> bool:
    import socket

    if host.strip() == "":
        return False
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def wait_for_tcp_ready(host: str, port: int, attempts: int, interval_seconds: float) -> bool:
    for _ in range(attempts):
        if can_connect_tcp(host, port):
            return True
        time.sleep(interval_seconds)
    return False


def find_runtime_project_root(
    start_path: Path,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> Path:
    for candidate_path in (start_path, *start_path.parents):
        if (candidate_path / "startup.sh").is_file() and (candidate_path / "supabase" / "config.toml").is_file():
            return candidate_path
    raise FileNotFoundError(
        translate_fn(
            "dashboard.local_supabase.runtime.startup_script_not_found",
            {"start_path": start_path},
        )
    )


def resolve_runtime_project_root(
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> Path:
    frozen_candidates: list[Path] = []
    if getattr(sys, "frozen", False):
        frozen_candidates.append(Path(sys.executable).resolve().parent)
        frozen_candidates.append(Path(sys.executable).resolve().parent.parent)

    for candidate in [*frozen_candidates, PROJECT_ROOT]:
        try:
            return find_runtime_project_root(candidate, translate_fn)
        except FileNotFoundError:
            continue

    raise FileNotFoundError(
        translate_fn("dashboard.local_supabase.runtime.project_root_not_found", {})
    )


def read_local_studio_port(
    project_root: Path,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> int:
    import tomllib

    config_path = project_root / "supabase" / "config.toml"
    with config_path.open("rb") as file_handle:
        config_values = tomllib.load(file_handle)

    studio_section = config_values.get("studio")
    if not isinstance(studio_section, dict):
        raise ValueError(
            translate_fn(
                "dashboard.local_supabase.runtime.studio_section_missing",
                {"config_path": config_path},
            )
        )

    port_value = studio_section.get("port")
    if not isinstance(port_value, int):
        raise ValueError(
            translate_fn(
                "dashboard.local_supabase.runtime.studio_port_invalid",
                {"config_path": config_path},
            )
        )

    return port_value


def convert_windows_path_to_wsl_path(
    path: Path,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> str:
    normalized_path = str(path.resolve()).replace("\\", "/")
    match = re.match(r"^([A-Za-z]):/(.*)$", normalized_path)
    if match is None:
        raise ValueError(
            translate_fn(
                "dashboard.local_supabase.runtime.wsl_path_conversion_failed",
                {"normalized_path": normalized_path},
            )
        )
    drive_letter = match.group(1).lower()
    remainder = match.group(2)
    return f"/mnt/{drive_letter}/{remainder}"


def resolve_local_supabase_runtime(
    supabase_url: str,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> LocalSupabaseRuntime:
    api_host, api_port = parse_http_endpoint(supabase_url)
    project_root = resolve_runtime_project_root(translate_fn)
    startup_script_path = project_root / "startup.sh"
    db_port = read_local_db_port(project_root)
    studio_port = read_local_studio_port(project_root, translate_fn)
    return LocalSupabaseRuntime(
        project_root=project_root,
        startup_script_path=startup_script_path,
        api_host=api_host,
        api_port=api_port,
        db_host="127.0.0.1",
        db_port=db_port,
        studio_host="127.0.0.1",
        studio_port=studio_port,
    )


def is_local_supabase_target(supabase_url: str) -> bool:
    api_host, _ = parse_http_endpoint(supabase_url)
    return is_loopback_host(api_host)


def is_local_supabase_stack_ready(runtime: LocalSupabaseRuntime) -> bool:
    return can_connect_tcp(runtime.api_host, runtime.api_port) and can_connect_tcp(runtime.db_host, runtime.db_port)


def is_local_supabase_studio_ready(runtime: LocalSupabaseRuntime) -> bool:
    return can_connect_tcp(runtime.studio_host, runtime.studio_port)


def build_local_supabase_studio_url(runtime: LocalSupabaseRuntime) -> str:
    return f"http://{runtime.studio_host}:{runtime.studio_port}/"


def build_wsl_start_command(
    runtime: LocalSupabaseRuntime,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> list[str]:
    wsl_project_root = convert_windows_path_to_wsl_path(runtime.project_root, translate_fn)
    return [
        "wsl.exe",
        "bash",
        "-lc",
        f"cd '{wsl_project_root}' && ./startup.sh",
    ]


def build_wsl_docker_check_command(
    runtime: LocalSupabaseRuntime,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> list[str]:
    wsl_project_root = convert_windows_path_to_wsl_path(runtime.project_root, translate_fn)
    return [
        "wsl.exe",
        "bash",
        "-lc",
        f"cd '{wsl_project_root}' && docker info >/dev/null 2>&1",
    ]


def build_wsl_stop_command(
    runtime: LocalSupabaseRuntime,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> list[str]:
    wsl_project_root = convert_windows_path_to_wsl_path(runtime.project_root, translate_fn)
    return [
        "wsl.exe",
        "bash",
        "-lc",
        (
            f"cd '{wsl_project_root}' && "
            "if command -v supabase >/dev/null 2>&1; then supabase stop || true; fi && "
            "docker ps -a --format '{{.Names}}' | "
            "grep -E '^(supabase_.*_Extrusion_data|grafana_local)$' | "
            "xargs -r docker stop || true"
        ),
    ]


def is_any_local_supabase_service_ready(runtime: LocalSupabaseRuntime) -> bool:
    return is_local_supabase_stack_ready(runtime) or is_local_supabase_studio_ready(runtime)


def check_local_docker_ready(
    runtime: LocalSupabaseRuntime,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> LocalDockerCheckResult:
    try:
        completed = subprocess.run(
            build_wsl_docker_check_command(runtime, translate_fn),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=10,
            check=False,
        )
    except FileNotFoundError:
        return LocalDockerCheckResult(
            False,
            translate_fn("dashboard.local_supabase.runtime.wsl_missing", {}),
        )
    except subprocess.TimeoutExpired:
        return LocalDockerCheckResult(
            False,
            translate_fn("dashboard.local_supabase.runtime.docker_check_timeout", {}),
        )
    except Exception as error:
        return LocalDockerCheckResult(
            False,
            translate_fn(
                "dashboard.local_supabase.runtime.docker_check_failed",
                {"error": error},
            ),
        )

    if completed.returncode == 0:
        return LocalDockerCheckResult(True, "")

    stderr_text = completed.stderr.strip()
    stdout_text = completed.stdout.strip()
    if stderr_text != "":
        return LocalDockerCheckResult(False, stderr_text)
    if stdout_text != "":
        return LocalDockerCheckResult(False, stdout_text)
    return LocalDockerCheckResult(
        False,
        translate_fn("dashboard.local_supabase.runtime.docker_desktop_not_running", {}),
    )


def build_local_supabase_ui_state(
    supabase_url: str,
    is_starting: bool,
    is_stopping: bool,
    pending_open_studio: bool,
    runtime: LocalSupabaseRuntime | None,
    is_ready: bool,
    is_studio_ready: bool,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> LocalSupabaseUiState:
    if not is_local_supabase_target(supabase_url):
        return LocalSupabaseUiState(
            status_text=translate_fn("dashboard.local_supabase.status.remote", {}),
            status_color="gray",
            show_progress=False,
            start_button_text=translate_fn("dashboard.local_supabase.button.start.disabled_remote", {}),
            start_button_enabled=False,
            studio_button_text=translate_fn("dashboard.local_supabase.button.studio.disabled_remote", {}),
            studio_button_enabled=False,
            stop_button_text=translate_fn("dashboard.local_supabase.button.stop.disabled_remote", {}),
            stop_button_enabled=False,
        )

    if is_stopping:
        return LocalSupabaseUiState(
            status_text=translate_fn("dashboard.local_supabase.status.stopping", {}),
            status_color="#E5C07B",
            show_progress=True,
            start_button_text=translate_fn("dashboard.local_supabase.button.start", {}),
            start_button_enabled=False,
            studio_button_text=translate_fn("dashboard.local_supabase.button.studio", {}),
            studio_button_enabled=False,
            stop_button_text=translate_fn("dashboard.local_supabase.button.stop.stopping", {}),
            stop_button_enabled=False,
        )

    if is_starting:
        if pending_open_studio:
            return LocalSupabaseUiState(
                status_text=translate_fn("dashboard.local_supabase.status.starting_pending_studio", {}),
                status_color="#3B8ED0",
                show_progress=True,
                start_button_text=translate_fn("dashboard.local_supabase.button.start.starting", {}),
                start_button_enabled=False,
                studio_button_text=translate_fn("dashboard.local_supabase.button.studio.pending", {}),
                studio_button_enabled=False,
                stop_button_text=translate_fn("dashboard.local_supabase.button.stop", {}),
                stop_button_enabled=False,
            )
        return LocalSupabaseUiState(
            status_text=translate_fn("dashboard.local_supabase.status.starting", {}),
            status_color="#3B8ED0",
            show_progress=True,
            start_button_text=translate_fn("dashboard.local_supabase.button.start.starting", {}),
            start_button_enabled=False,
            studio_button_text=translate_fn("dashboard.local_supabase.button.studio", {}),
            studio_button_enabled=False,
            stop_button_text=translate_fn("dashboard.local_supabase.button.stop", {}),
            stop_button_enabled=False,
        )

    if runtime is not None and is_ready and is_studio_ready:
        return LocalSupabaseUiState(
            status_text=translate_fn("dashboard.local_supabase.status.ready_with_studio", {}),
            status_color="#2CC985",
            show_progress=False,
            start_button_text=translate_fn("dashboard.local_supabase.button.start.ready", {}),
            start_button_enabled=False,
            studio_button_text=translate_fn("dashboard.local_supabase.button.studio", {}),
            studio_button_enabled=True,
            stop_button_text=translate_fn("dashboard.local_supabase.button.stop", {}),
            stop_button_enabled=True,
        )

    if runtime is not None and is_ready:
        return LocalSupabaseUiState(
            status_text=translate_fn("dashboard.local_supabase.status.ready_without_studio", {}),
            status_color="#3B8ED0",
            show_progress=False,
            start_button_text=translate_fn("dashboard.local_supabase.button.start.ready", {}),
            start_button_enabled=False,
            studio_button_text=translate_fn("dashboard.local_supabase.button.studio", {}),
            studio_button_enabled=True,
            stop_button_text=translate_fn("dashboard.local_supabase.button.stop", {}),
            stop_button_enabled=True,
        )

    return LocalSupabaseUiState(
        status_text=translate_fn("dashboard.local_supabase.status.stopped", {}),
        status_color="gray",
        show_progress=False,
        start_button_text=translate_fn("dashboard.local_supabase.button.start", {}),
        start_button_enabled=True,
        studio_button_text=translate_fn("dashboard.local_supabase.button.studio", {}),
        studio_button_enabled=True,
        stop_button_text=translate_fn("dashboard.local_supabase.button.stop", {}),
        stop_button_enabled=False,
    )


def build_local_supabase_checking_ui_state(
    pending_open_studio: bool,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> LocalSupabaseUiState:
    studio_button_key = "dashboard.local_supabase.button.studio"
    if pending_open_studio:
        studio_button_key = "dashboard.local_supabase.button.studio.pending"
    return LocalSupabaseUiState(
        status_text=translate_fn("dashboard.local_supabase.status.checking", {}),
        status_color="#3B8ED0",
        show_progress=True,
        start_button_text=translate_fn("dashboard.local_supabase.button.start", {}),
        start_button_enabled=False,
        studio_button_text=translate_fn(studio_button_key, {}),
        studio_button_enabled=False,
        stop_button_text=translate_fn("dashboard.local_supabase.button.stop", {}),
        stop_button_enabled=False,
    )


def build_archive_job_context(request: ArchiveMetricsRequest) -> ArchiveJobContext:
    environment_values = load_archive_environment(PROJECT_ROOT)
    before_datetime = parse_archive_before_date(request.before_date)
    archive_dir = resolve_archive_dir(
        normalize_optional_archive_dir(request.archive_dir),
        environment_values,
    )
    db_settings = resolve_db_connection_settings(PROJECT_ROOT, environment_values)
    output_path = build_archive_output_path(archive_dir, before_datetime)
    source_stats = read_all_metrics_archive_stats(db_settings, before_datetime)
    return ArchiveJobContext(
        db_settings=db_settings,
        before_datetime=before_datetime,
        output_path=output_path,
        source_stats=source_stats,
    )


def execute_archive_preview(
    request: ArchiveMetricsRequest,
    log_callback: Callable[[str], None],
    progress_callback: Callable[[float], None],
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> Path:
    progress_callback(0.0)
    context = build_archive_job_context(request)
    progress_callback(1.0)
    log_callback(translate_fn("data_mgmt.archive.log.preview_completed", {}))
    log_callback(translate_fn("data_mgmt.archive.log.before_date", {"before_date": request.before_date}))
    log_callback(
        translate_fn(
            "data_mgmt.archive.log.db_target",
            {
                "host": context.db_settings.host,
                "port": context.db_settings.port,
                "dbname": context.db_settings.dbname,
            },
        )
    )
    log_callback(translate_fn("data_mgmt.archive.log.output_path", {"output_path": context.output_path}))
    log_callback(translate_fn("data_mgmt.archive.log.source_row_count", {"row_count": context.source_stats.row_count}))
    log_callback(
        translate_fn(
            "data_mgmt.archive.log.timestamp_range",
            {
                "start_timestamp": context.source_stats.min_timestamp,
                "end_timestamp": context.source_stats.max_timestamp,
            },
        )
    )
    return context.output_path


def execute_archive_export(
    request: ArchiveMetricsRequest,
    log_callback: Callable[[str], None],
    progress_callback: Callable[[float], None],
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> Path:
    progress_callback(0.0)
    context = build_archive_job_context(request)
    progress_callback(0.2)
    log_callback(translate_fn("data_mgmt.archive.log.export_started", {"output_path": context.output_path}))
    result = export_all_metrics_to_parquet(
        context.db_settings,
        context.before_datetime,
        context.output_path,
        DEFAULT_ARCHIVE_CHUNK_SIZE,
    )
    progress_callback(0.8)
    archive_stats = read_parquet_archive_stats(context.output_path)
    validate_archive_stats_match(context.source_stats, archive_stats)
    progress_callback(1.0)
    log_callback(translate_fn("data_mgmt.archive.log.export_completed", {}))
    log_callback(translate_fn("data_mgmt.archive.log.output_file", {"output_path": result.output_path}))
    log_callback(translate_fn("data_mgmt.archive.log.export_row_count", {"row_count": result.row_count}))
    log_callback(
        translate_fn(
            "data_mgmt.archive.log.timestamp_range",
            {
                "start_timestamp": result.min_timestamp,
                "end_timestamp": result.max_timestamp,
            },
        )
    )
    return result.output_path


def execute_archive_export_and_delete(
    request: ArchiveMetricsRequest,
    log_callback: Callable[[str], None],
    progress_callback: Callable[[float], None],
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> Path:
    progress_callback(0.0)
    context = build_archive_job_context(request)
    progress_callback(0.2)
    log_callback(translate_fn("data_mgmt.archive.log.export_started", {"output_path": context.output_path}))
    result = export_all_metrics_to_parquet(
        context.db_settings,
        context.before_datetime,
        context.output_path,
        DEFAULT_ARCHIVE_CHUNK_SIZE,
    )
    progress_callback(0.6)
    archive_stats = read_parquet_archive_stats(context.output_path)
    validate_archive_stats_match(context.source_stats, archive_stats)
    deleted_stats = delete_archived_all_metrics(
        context.db_settings,
        context.before_datetime,
        context.source_stats,
    )
    progress_callback(1.0)
    log_callback(translate_fn("data_mgmt.archive.log.export_delete_completed", {}))
    log_callback(translate_fn("data_mgmt.archive.log.output_file", {"output_path": result.output_path}))
    log_callback(translate_fn("data_mgmt.archive.log.deleted_row_count", {"row_count": deleted_stats.row_count}))
    log_callback(
        translate_fn(
            "data_mgmt.archive.log.deleted_timestamp_range",
            {
                "start_timestamp": deleted_stats.min_timestamp,
                "end_timestamp": deleted_stats.max_timestamp,
            },
        )
    )
    return result.output_path


def execute_training_build(
    request: TrainingBuildRequest,
    log_callback: Callable[[str], None],
    progress_callback: Callable[[float], None],
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> tuple[Path, ...]:
    if request.mode not in TRAINING_MODE_OPTIONS:
        raise ValueError(
            translate_fn(
                "data_mgmt.training.validation.unsupported_mode",
                {"mode": request.mode},
            )
        )

    progress_callback(0.0)

    if request.mode == "build-base":
        build_training_base = load_build_training_base()
        plc_file_path = normalize_required_training_path(
            request.plc_file_path,
            translate_fn("data_mgmt.training.label.raw_csv", {}),
            translate_fn,
        )
        spot_file_path = normalize_optional_training_path(request.spot_file_path)
        training_base_output_path = normalize_required_training_path(
            request.training_base_output_path,
            translate_fn("data_mgmt.training.label.base_output", {}),
            translate_fn,
        )
        filename_hint = resolve_training_filename_hint(
            request.filename_hint,
            plc_file_path,
            translate_fn,
        )
        log_callback(translate_fn("data_mgmt.training.log.base_started", {}))
        written_path = build_training_base(
            plc_file_path,
            training_base_output_path,
            filename_hint,
            spot_file_path,
        )
        progress_callback(1.0)
        return (written_path,)

    if request.mode == "build-v1":
        build_training_dataset_v1 = load_build_training_dataset_v1()
        training_base_file_path = normalize_required_training_path(
            request.training_base_file_path,
            translate_fn("data_mgmt.training.label.base_input", {}),
            translate_fn,
        )
        training_dataset_output_path = normalize_required_training_path(
            request.training_dataset_output_path,
            translate_fn("data_mgmt.training.label.dataset_output", {}),
            translate_fn,
        )
        log_callback(translate_fn("data_mgmt.training.log.dataset_started", {}))
        written_path = build_training_dataset_v1(
            training_base_file_path,
            training_dataset_output_path,
        )
        progress_callback(1.0)
        return (written_path,)

    build_training_base = load_build_training_base()
    build_training_dataset_v1 = load_build_training_dataset_v1()
    plc_file_path = normalize_required_training_path(
        request.plc_file_path,
        translate_fn("data_mgmt.training.label.raw_csv", {}),
        translate_fn,
    )
    spot_file_path = normalize_optional_training_path(request.spot_file_path)
    training_base_output_path = normalize_required_training_path(
        request.training_base_output_path,
        translate_fn("data_mgmt.training.label.base_output", {}),
        translate_fn,
    )
    training_dataset_output_path = normalize_required_training_path(
        request.training_dataset_output_path,
        translate_fn("data_mgmt.training.label.dataset_output", {}),
        translate_fn,
    )
    filename_hint = resolve_training_filename_hint(
        request.filename_hint,
        plc_file_path,
        translate_fn,
    )
    log_callback(translate_fn("data_mgmt.training.log.base_started", {}))
    training_base_written_path = build_training_base(
        plc_file_path,
        training_base_output_path,
        filename_hint,
        spot_file_path,
    )
    progress_callback(0.5)
    log_callback(translate_fn("data_mgmt.training.log.dataset_started", {}))
    training_dataset_written_path = build_training_dataset_v1(
        training_base_written_path,
        training_dataset_output_path,
    )
    progress_callback(1.0)
    return (
        training_base_written_path,
        training_dataset_written_path,
    )


def normalize_legacy_cycle_request(
    machine_id: str,
    mode: str,
    custom_date: str,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> LegacyCycleRequest:
    normalized_machine_id = machine_id.strip()
    if normalized_machine_id == "":
        raise ValueError(translate_fn("cycle_ops.legacy.validation.machine_id_required", {}))
    normalized_mode = mode.strip()
    if normalized_mode not in {"incremental", "all", "today", "yesterday", "custom"}:
        raise ValueError(
            translate_fn(
                "cycle_ops.legacy.validation.unsupported_mode",
                {"mode": normalized_mode},
            )
        )
    normalized_custom_date = custom_date.strip()
    if normalized_mode == "custom" and normalized_custom_date == "":
        raise ValueError(translate_fn("cycle_ops.legacy.validation.custom_date_required", {}))
    return LegacyCycleRequest(
        machine_id=normalized_machine_id,
        mode=normalized_mode,
        custom_date=normalized_custom_date,
    )


def format_cycle_health_report(
    report: CycleHealthReport,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> list[str]:
    return [
        translate_fn("cycle_ops.health.cache_row_count", {"row_count": report.cache_row_count}),
        translate_fn("cycle_ops.health.cache_cycle_count", {"cycle_count": report.cache_cycle_count}),
        translate_fn(
            "cycle_ops.health.cache_timestamp_range",
            {
                "min_timestamp": report.cache_min_timestamp,
                "max_timestamp": report.cache_max_timestamp,
            },
        ),
        translate_fn("cycle_ops.health.snapshot_row_count", {"row_count": report.snapshot_row_count}),
        translate_fn(
            "cycle_ops.health.snapshot_canonical_count",
            {"row_count": report.snapshot_canonical_count},
        ),
        translate_fn(
            "cycle_ops.health.snapshot_legacy_count",
            {"row_count": report.snapshot_legacy_count},
        ),
        translate_fn(
            "cycle_ops.health.snapshot_unmapped_count",
            {"row_count": report.snapshot_unmapped_count},
        ),
        translate_fn("cycle_ops.health.snapshot_latest_end", {"timestamp": report.snapshot_latest_end}),
        translate_fn(
            "cycle_ops.health.snapshot_latest_update",
            {"timestamp": report.snapshot_latest_update},
        ),
    ]


def format_state_health_view(
    state_health_snapshot: core_state.StateHealthSnapshot,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> tuple[str, str, tuple[str, ...]]:
    summary_code = state_health_snapshot["summary_code"]
    state = state_health_snapshot["state"]
    pending_resume_count = state_health_snapshot["pending_resume_count"]
    failed_retry_count = state_health_snapshot["failed_retry_count"]
    if summary_code == "legacy_mode":
        status_text = translate_fn("dashboard.state_store.status.legacy_mode", {})
    elif state == "blocked":
        status_text = translate_fn("dashboard.state_store.status.recovery_required", {})
    elif state == "attention":
        status_text = translate_fn("dashboard.state_store.status.recovery_available", {})
    else:
        status_text = translate_fn("dashboard.state_store.status.ready", {})
    status_color = "#E06C75" if state == "blocked" else "#E5C07B" if state == "attention" else "#2CC985"
    detail_lines: list[str] = []
    for detail_code in state_health_snapshot["detail_codes"]:
        if detail_code == "legacy_mode":
            detail_lines.append(translate_fn("dashboard.state_store.detail.legacy_mode", {}))
        elif detail_code == "restore_sqlite":
            detail_lines.append(translate_fn("dashboard.state_store.detail.restore_sqlite", {}))
        elif detail_code == "backup_dir":
            backup_dir = str(state_health_snapshot.get("backup_dir", "")).strip()
            if backup_dir != "":
                detail_lines.append(
                    translate_fn("dashboard.state_store.detail.backup_dir", {"backup_dir": backup_dir})
                )
        elif detail_code == "failed_retry_present":
            detail_lines.append(
                translate_fn("dashboard.state_store.detail.failed_retry_present", {"count": failed_retry_count})
            )
        elif detail_code == "resume_present":
            detail_lines.append(
                translate_fn("dashboard.state_store.detail.resume_present", {"count": pending_resume_count})
            )
        elif detail_code == "can_resume":
            detail_lines.append(translate_fn("dashboard.state_store.detail.can_resume", {}))
        elif detail_code == "ready":
            detail_lines.append(translate_fn("dashboard.state_store.detail.ready", {}))
    error_message = str(state_health_snapshot.get("error_message", "")).strip()
    if error_message != "":
        detail_lines.insert(0, error_message)
    if detail_lines == []:
        detail_lines.append(translate_fn("dashboard.state_store.detail.ready", {}))
    return status_text, status_color, tuple(detail_lines[:4])


def build_upload_operational_cards_state(
    vals: dict[str, str],
    dashboard_state_snapshot: core_state.UploadDashboardStateSnapshot,
    state_health_snapshot: core_state.StateHealthSnapshot,
    preview_scan_result: PreviewScanResult | None,
    local_supabase_status_text: str,
    translate_fn: Callable[[str, Mapping[str, object]], str],
) -> UploadOperationalCardsState:
    recent_successful_upload_profile = dashboard_state_snapshot["recent_successful_upload_profile"]
    failed_retry_set = dashboard_state_snapshot["failed_retry_set"]
    resume_map = dashboard_state_snapshot["resume"]
    state_health_status_text, state_health_status_color, state_health_detail_lines = format_state_health_view(
        state_health_snapshot,
        translate_fn,
    )
    state_health_blocks_upload = not state_health_snapshot["can_start_upload"]

    ok_cfg, missing = validate_config(vals)
    preflight_status_text = translate_fn("dashboard.upload.status.preflight_pending", {})
    preflight_status_color = "gray"
    preflight_detail_lines: list[str] = []
    is_upload_preflight_blocked = False

    if not ok_cfg:
        preflight_status_text = translate_fn("dashboard.upload.status.preflight_blocked", {})
        preflight_status_color = "#E06C75"
        preflight_detail_lines.append(", ".join(missing))
        is_upload_preflight_blocked = True
    else:
        if vals.get("PLC_DIR", "").strip() == "" or not os.path.isdir(vals["PLC_DIR"]):
            preflight_status_text = translate_fn("dashboard.upload.status.preflight_blocked", {})
            preflight_status_color = "#E06C75"
            is_upload_preflight_blocked = True
            preflight_detail_lines.append(
                translate_fn("settings.validation.required_fields", {"fields": "PLC_DIR"})
            )
        if is_edge_url_origin_mismatch(vals.get("EDGE_FUNCTION_URL", ""), vals.get("SUPABASE_URL", "")):
            preflight_status_text = translate_fn("dashboard.upload.status.preflight_blocked", {})
            preflight_status_color = "#E06C75"
            preflight_detail_lines.append(translate_fn("settings.validation.edge_host_mismatch", {}))
            is_upload_preflight_blocked = True
        if is_local_supabase_target(vals.get("SUPABASE_URL", "")):
            try:
                runtime = resolve_local_supabase_runtime(vals.get("SUPABASE_URL", ""), translate_fn)
            except Exception as error:
                preflight_status_text = translate_fn("dashboard.upload.status.preflight_blocked", {})
                preflight_status_color = "#E06C75"
                preflight_detail_lines.append(str(error))
                is_upload_preflight_blocked = True
            else:
                if not is_local_supabase_stack_ready(runtime):
                    preflight_status_text = translate_fn("dashboard.upload.status.preflight_blocked", {})
                    preflight_status_color = "#E06C75"
                    preflight_detail_lines.append(local_supabase_status_text)
                    is_upload_preflight_blocked = True
    if state_health_blocks_upload:
        preflight_status_text = translate_fn("dashboard.upload.status.preflight_blocked", {})
        preflight_status_color = "#E06C75"
        is_upload_preflight_blocked = True
        preflight_detail_lines = list(state_health_detail_lines)
    elif (
        preview_scan_result is not None
        and preview_scan_result.fingerprint == build_upload_selection_fingerprint(vals)
    ):
        preflight_status_text = translate_fn("dashboard.upload.status.preflight_ready", {})
        preflight_status_color = "#2CC985"
        preflight_detail_lines = list(preview_scan_result.detail_lines)

    if preflight_detail_lines == []:
        preflight_detail_lines.append(translate_fn("dashboard.upload.detail.preflight_pending", {}))

    has_retryable_state = any(int(offset) > 0 for offset in resume_map.values()) or failed_retry_set != ()

    retryable_upload_items: tuple[tuple[str, str, str, str], ...] = ()

    if not has_retryable_state:
        resume_status_text = translate_fn("dashboard.upload.status.resume_empty", {})
        resume_status_color = "gray"
        resume_detail_lines: list[str] = []
    else:
        resume_status_text = translate_fn("dashboard.upload.status.resume_available", {})
        resume_status_color = "#E5C07B"
        resume_detail_lines = []
        for failed_retry_entry in failed_retry_set[:3]:
            filename = str(failed_retry_entry.get("filename", "")).strip()
            error_message = str(failed_retry_entry.get("error_message", "")).strip()
            if filename != "":
                resume_detail_lines.append(filename)
            if error_message != "":
                resume_detail_lines.append(error_message)
        for _, filename, _, _ in retryable_upload_items[:3]:
            resume_detail_lines.append(filename)

    if recent_successful_upload_profile is None:
        recent_success_status_text = translate_fn("dashboard.upload.status.recent_success_empty", {})
        recent_success_status_color = "gray"
        recent_success_detail_lines: list[str] = []
        can_rerun_recent_success = False
    else:
        profile_name = str(recent_successful_upload_profile.get("profile_name", "")).strip()
        applied_at_text = format_optional_timestamp_text(
            recent_successful_upload_profile.get("applied_at")
        )
        recent_success_detail_lines = [
            line for line in [profile_name, applied_at_text] if line != ""
        ]
        recent_success_status_text = (
            profile_name
            if profile_name != ""
            else translate_fn("dashboard.upload.recent_success.title", {})
        )
        recent_success_status_color = "#2CC985"
        can_rerun_recent_success = True

    return UploadOperationalCardsState(
        recent_successful_upload_profile=recent_successful_upload_profile,
        failed_retry_set=failed_retry_set,
        state_health_status_text=state_health_status_text,
        state_health_status_color=state_health_status_color,
        state_health_detail_lines=state_health_detail_lines,
        state_health_blocks_upload=state_health_blocks_upload,
        is_upload_preflight_blocked=is_upload_preflight_blocked,
        has_retryable_state=has_retryable_state,
        retryable_upload_items=retryable_upload_items,
        preflight_status_text=preflight_status_text,
        preflight_status_color=preflight_status_color,
        preflight_detail_lines=tuple(preflight_detail_lines[:4]),
        resume_status_text=resume_status_text,
        resume_status_color=resume_status_color,
        resume_detail_lines=tuple(resume_detail_lines[:4]),
        recent_success_status_text=recent_success_status_text,
        recent_success_status_color=recent_success_status_color,
        recent_success_detail_lines=tuple(recent_success_detail_lines[:2]),
        can_rerun_recent_success=can_rerun_recent_success,
    )

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.startup_profiling_enabled = is_startup_profiling_enabled()
        self.startup_started_at = time.perf_counter()
        self.startup_last_checkpoint = self.startup_started_at
        self.startup_timing_entries: list[StartupTimingEntry] = []
        self.startup_timing_report = ""
        self.title("Extrusion Uploader")
        self.geometry('1240x780')
        self.minsize(1160, 760)
        self.resizable(True, True)
        try:
            self.iconbitmap(APP_ICON)
        except Exception:
            pass
        self.record_startup_timing("window_setup")
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        self.record_startup_timing("load_config")
        self.ui_language = normalize_language_code(
            self.cfg.get("UI_LANGUAGE", DEFAULT_UI_LANGUAGE)
        )
        self.translation_bundle = load_translation_bundle(PROJECT_ROOT, self.ui_language)
        self.record_startup_timing("load_translations")
        self.is_shutting_down = False
        self.title(self.tr("app.title"))
        
        # Shared state
        self.active_progress: dict[str, tuple[int, int]] = {}
        self.progress_lock = threading.Lock()
        self.total_files = 0
        self.processed_count = 0
        self.is_uploading = False
        self.upload_dashboard_status_text = self.tr("common.status.waiting")
        self.upload_dashboard_status_color = "gray"
        self.is_data_task_running = False
        self.is_supabase_starting = False
        self.is_supabase_stopping = False
        self.pending_open_studio = False
        self.pending_close_after_supabase_stop = False
        self.local_supabase_status_override: LocalSupabaseStatusOverride | None = None
        self.local_supabase_status_snapshot: LocalSupabaseStatusSnapshot | None = None
        self.is_local_supabase_status_refreshing = False
        self.local_supabase_status_request_id = 0
        self.recent_successful_upload_profile = None
        self.failed_retry_set: list[core_state.FailedRetryEntry] = []
        self.state_health_blocks_upload = True
        self.is_upload_preflight_blocked = True
        self.has_retryable_state = False
        self.retryable_upload_items: list[tuple[str, str, str, str]] = []
        self.is_upload_operational_cards_refreshing = False
        self.upload_operational_cards_request_id = 0
        self.pause_event = threading.Event()
        self.pause_event.set() # Start as running (not paused)
        self.var_training_mode = tk.StringVar(value="build-all")
        self.var_training_plc_file = tk.StringVar(value="")
        self.var_training_spot_file = tk.StringVar(value="")
        self.var_training_base_file = tk.StringVar(value="")
        self.var_training_base_output = tk.StringVar(value="")
        self.var_training_dataset_output = tk.StringVar(value="")
        self.var_training_filename_hint = tk.StringVar(value="")
        self.var_training_status = tk.StringVar(value=self.tr("common.status.idle"))
        self.var_cycle_ops_status = tk.StringVar(value=self.tr("common.status.idle"))
        self.var_legacy_cycle_status = tk.StringVar(value=self.tr("common.status.idle"))
        self.var_legacy_cycle_machine_id = tk.StringVar(
            value=self.cfg.get("LEGACY_CYCLE_MACHINE_ID", "").strip()
        )
        self.var_legacy_cycle_range = tk.StringVar(value="incremental")
        self.var_legacy_cycle_custom_date = tk.StringVar(value=kst_now().date().isoformat())
        self.var_local_supabase_status = tk.StringVar(
            value=self.tr("dashboard.local_supabase.status.checking")
        )
        self.var_state_store_status = tk.StringVar(
            value=self.tr("dashboard.state_store.status.checking")
        )
        self.var_settings_dirty = tk.StringVar(value=self.tr("settings.dirty.clean"))
        self.var_settings_validation = tk.StringVar(
            value=self.tr("settings.validation.no_action_needed")
        )
        self.is_settings_dirty = False
        self.settings_calendar_popup = None
        self.settings_calendar_target = ""
        self.settings_calendar_year = 0
        self.settings_calendar_month = 0
        self.current_view = ""
        self.dashboard_layout_after_id: str | None = None
        self.dashboard_update_after_id: str | None = None
        self.is_dashboard_update_loop_running = False
        self.dashboard_view_generation = 0
        self.selected_work_log_path = ""
        self.work_log_messages: list[str] = []
        self.responsive_layout_state = self.build_responsive_layout_state(1240)
        self.responsive_layout_mode_signature = self.build_responsive_layout_mode_signature(
            self.responsive_layout_state
        )
        self.resize_debounce_after_id: str | None = None
        self.resize_debounce_ms = 120
        self.last_window_width = 0
        self.label_wrap_bindings: dict[str, LabelWrapBinding] = {}
        self.label_wrap_after_id: str | None = None
        self.wsl_storage_raw_snapshot: object | None = None
        self.wsl_storage_error_detail = ""
        self.is_wsl_storage_refreshing = False
        self.wsl_storage_snapshot = self.build_wsl_storage_ui_snapshot()
        self.rendered_wsl_storage_snapshot: WslStorageSnapshot | None = None
        self.dashboard_layout_signature: DashboardLayoutSignature | None = None
        self.upload_operational_cards_state: UploadOperationalCardsState | None = None
        self.last_preview_scan_result: PreviewScanResult | None = None
        self.record_startup_timing("initialize_runtime_state")
        archive_environment = load_archive_environment(PROJECT_ROOT)
        self.var_archive_before_date = tk.StringVar(value=kst_now().date().isoformat())
        self.var_archive_dir = tk.StringVar(value=archive_environment.get("ARCHIVE_DIR", ""))
        self.var_archive_delete = tk.BooleanVar(value=False)
        self.var_archive_status = tk.StringVar(value=self.tr("common.status.idle"))
        self.record_startup_timing("load_archive_environment")
        
        # Thread-safe logging
        self.log_queue = queue.Queue()
        self.check_log_queue()
        self.record_startup_timing("initialize_log_queue")
        
        # Grid layout (1x2)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.record_startup_timing("initialize_root_layout")
        
        self.create_sidebar()
        self.record_startup_timing("create_sidebar")
        self.create_main_area()
        self.record_startup_timing("create_main_area")
        
        # Handle window close
        self.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.record_startup_timing("bind_close_protocol")
        
        # Initial View
        self.show_dashboard()
        self.record_startup_timing("show_dashboard")
        
        # Close Splash Screen
        self.after(200, self.close_splash)
        self.record_startup_timing("schedule_close_splash")
        self.emit_startup_timing_report()

    def tr(self, key: str, **params: object) -> str:
        try:
            return translate(self.translation_bundle, key, params)
        except Exception as error:
            print(f"[i18n] {key}: {error}")
            raise

    def tr_map(self, key: str, params: Mapping[str, object]) -> str:
        try:
            return translate(self.translation_bundle, key, params)
        except Exception as error:
            print(f"[i18n] {key}: {error}")
            raise

    def record_startup_timing(self, label: str) -> None:
        if not self.startup_profiling_enabled:
            return
        now = time.perf_counter()
        self.startup_timing_entries.append(
            StartupTimingEntry(
                label=label,
                elapsed_seconds=now - self.startup_last_checkpoint,
            )
        )
        self.startup_last_checkpoint = now

    def emit_startup_timing_report(self) -> None:
        total_seconds = time.perf_counter() - self.startup_started_at
        self.startup_timing_report = format_startup_timing_report(
            tuple(self.startup_timing_entries),
            total_seconds,
        )
        if self.startup_profiling_enabled:
            print(self.startup_timing_report)

    def build_responsive_layout_state(self, window_width: int) -> ResponsiveLayoutState:
        is_compact = window_width < 1320
        if is_compact:
            return ResponsiveLayoutState(
                window_width=window_width,
                is_compact=True,
                sidebar_width=210,
                main_pad_x=16,
                main_pad_y=16,
                logo_wraplength=150,
            )
        return ResponsiveLayoutState(
            window_width=window_width,
            is_compact=False,
            sidebar_width=230,
            main_pad_x=20,
            main_pad_y=20,
            logo_wraplength=170,
        )

    def build_responsive_layout_mode_signature(
        self,
        layout_state: ResponsiveLayoutState,
    ) -> ResponsiveLayoutModeSignature:
        return ResponsiveLayoutModeSignature(
            is_compact=layout_state.is_compact,
            sidebar_width=layout_state.sidebar_width,
            main_pad_x=layout_state.main_pad_x,
            main_pad_y=layout_state.main_pad_y,
            logo_wraplength=layout_state.logo_wraplength,
        )

    def bind_responsive_resize(self) -> None:
        self.bind("<Configure>", self.on_responsive_configure, add="+")
        self.main_shell.bind("<Configure>", self.on_responsive_configure, add="+")
        self.after(0, self.refresh_responsive_layout)

    def on_responsive_configure(self, _event: object) -> None:
        current_width = self.winfo_width()
        if current_width <= 1:
            return
        if current_width == self.last_window_width:
            return
        self.last_window_width = current_width
        self.schedule_responsive_layout_refresh()

    def schedule_responsive_layout_refresh(self) -> None:
        if self.is_shutting_down:
            return
        if self.resize_debounce_after_id is not None:
            return
        try:
            self.resize_debounce_after_id = self.after(
                self.resize_debounce_ms,
                self.refresh_responsive_layout,
            )
        except (RuntimeError, tk.TclError):
            self.resize_debounce_after_id = None

    def refresh_responsive_layout(self) -> None:
        self.resize_debounce_after_id = None
        current_width = self.winfo_width()
        if current_width <= 1:
            current_width = self.winfo_reqwidth()
        if current_width <= 1:
            return

        layout_state = self.build_responsive_layout_state(current_width)
        layout_mode_signature = self.build_responsive_layout_mode_signature(layout_state)
        self.responsive_layout_state = layout_state
        self.last_window_width = current_width

        if self.responsive_layout_mode_signature != layout_mode_signature:
            self.responsive_layout_mode_signature = layout_mode_signature
            if hasattr(self, "sidebar") and self.sidebar.winfo_exists():
                self.sidebar.configure(width=layout_state.sidebar_width)
            if hasattr(self, "logo_label") and self.logo_label.winfo_exists():
                self.logo_label.configure(wraplength=layout_state.logo_wraplength)
            if hasattr(self, "main_shell") and self.main_shell.winfo_exists():
                self.main_shell.grid_configure(
                    padx=layout_state.main_pad_x,
                    pady=layout_state.main_pad_y,
                )

        self.refresh_label_wraps()
        if self.current_view == "dashboard" and hasattr(self, "dashboard_body_frame"):
            self.schedule_dashboard_layout_refresh(None)
        if self.current_view == "cycle_ops":
            self.layout_cycle_legacy_range_row()
        if self.current_view == "data_mgmt":
            self.layout_training_mode_row()
            self.layout_archive_date_row()

    def is_widget_alive(self, widget: tk.Misc) -> bool:
        try:
            return bool(widget.winfo_exists())
        except tk.TclError:
            return False

    def forget_widget_geometry(self, widget: tk.Misc) -> None:
        if not self.is_widget_alive(widget):
            return
        manager = widget.winfo_manager()
        if manager == "":
            return
        if manager == "grid":
            widget.grid_forget()
            return
        if manager == "pack":
            widget.pack_forget()
            return
        if manager == "place":
            widget.place_forget()
            return
        raise RuntimeError(f"Unsupported geometry manager: {manager}")

    def resolve_wrap_container_width(self, binding: LabelWrapBinding) -> int:
        if self.is_widget_alive(binding.container):
            container_width = binding.container.winfo_width()
            if container_width > 1:
                return container_width
            requested_width = binding.container.winfo_reqwidth()
            if requested_width > 1:
                return requested_width
        if hasattr(self, "main_shell") and self.is_widget_alive(self.main_shell):
            main_shell_width = self.main_shell.winfo_width()
            if main_shell_width > 1:
                return main_shell_width
        return 0

    def read_wraplength(self, widget: tk.Misc) -> int:
        raw_wraplength = widget.cget("wraplength")
        if isinstance(raw_wraplength, int):
            return raw_wraplength
        if isinstance(raw_wraplength, float):
            return int(raw_wraplength)
        if isinstance(raw_wraplength, str):
            stripped_wraplength = raw_wraplength.strip()
            if stripped_wraplength == "":
                return 0
            return int(float(stripped_wraplength))
        raise TypeError(f"Unsupported wraplength value: {raw_wraplength!r}")

    def read_label_wrap_signature(self, widget: tk.Misc) -> LabelWrapSignature | None:
        cached_signature = getattr(widget, "_responsive_wrap_signature", None)
        if isinstance(cached_signature, LabelWrapSignature):
            return cached_signature
        return None

    def write_label_wrap_signature(
        self,
        widget: tk.Misc,
        signature: LabelWrapSignature,
    ) -> None:
        setattr(widget, "_responsive_wrap_signature", signature)

    def clear_label_wrap_signature(self, widget: tk.Misc) -> None:
        if hasattr(widget, "_responsive_wrap_signature"):
            delattr(widget, "_responsive_wrap_signature")

    def read_widget_layout_signature(
        self,
        widget: tk.Misc,
    ) -> WidgetLayoutSignature | None:
        cached_signature = getattr(widget, "_responsive_layout_signature", None)
        if isinstance(cached_signature, WidgetLayoutSignature):
            return cached_signature
        return None

    def write_widget_layout_signature(
        self,
        widget: tk.Misc,
        signature: WidgetLayoutSignature,
    ) -> None:
        setattr(widget, "_responsive_layout_signature", signature)

    def refresh_label_wraps(self) -> None:
        self.label_wrap_after_id = None
        stale_keys: list[str] = []
        for binding_key, binding in self.label_wrap_bindings.items():
            if not self.is_widget_alive(binding.widget):
                stale_keys.append(binding_key)
                continue
            if not self.is_widget_alive(binding.container):
                stale_keys.append(binding_key)
                continue

            container_width = self.resolve_wrap_container_width(binding)
            if container_width <= 1:
                continue

            wraplength = max(
                binding.min_wraplength,
                container_width - binding.horizontal_padding,
            )
            wrap_signature = LabelWrapSignature(
                container_width=container_width,
                wraplength=wraplength,
            )
            if self.read_label_wrap_signature(binding.widget) == wrap_signature:
                continue
            current_wraplength = self.read_wraplength(binding.widget)
            if current_wraplength != wraplength:
                binding.widget.configure(wraplength=wraplength)
            self.write_label_wrap_signature(binding.widget, wrap_signature)

        for binding_key in stale_keys:
            self.label_wrap_bindings.pop(binding_key, None)

    def schedule_label_wrap_refresh(self) -> None:
        if self.is_shutting_down:
            return
        if self.label_wrap_after_id is not None:
            return
        try:
            self.label_wrap_after_id = self.after(0, self.refresh_label_wraps)
        except (RuntimeError, tk.TclError):
            self.label_wrap_after_id = None

    def on_label_wrap_configure(self, _event: object) -> None:
        self.schedule_label_wrap_refresh()

    def on_label_wrap_destroy(self, event: object) -> None:
        widget = getattr(event, "widget", None)
        if isinstance(widget, tk.Misc):
            self.label_wrap_bindings.pop(str(widget), None)
            self.clear_label_wrap_signature(widget)

    def bind_label_wrap(self, widget: tk.Misc, horizontal_padding: int, min_wraplength: int) -> None:
        container = widget.master
        if container is None:
            raise RuntimeError("Label wrap binding requires a parent container.")

        binding = LabelWrapBinding(
            widget=widget,
            container=container,
            horizontal_padding=horizontal_padding,
            min_wraplength=min_wraplength,
        )
        previous_binding = self.label_wrap_bindings.get(str(widget))
        self.label_wrap_bindings[str(widget)] = binding
        if not bool(getattr(widget, "_responsive_wrap_bound", False)):
            widget.bind("<Destroy>", self.on_label_wrap_destroy, add="+")
            setattr(widget, "_responsive_wrap_bound", True)
        if not bool(getattr(container, "_responsive_wrap_bound", False)):
            container.bind("<Configure>", self.on_label_wrap_configure, add="+")
            setattr(container, "_responsive_wrap_bound", True)
        if previous_binding == binding:
            return
        self.clear_label_wrap_signature(widget)
        self.schedule_label_wrap_refresh()

    def reload_translations(self) -> None:
        self.ui_language = normalize_language_code(
            self.cfg.get("UI_LANGUAGE", DEFAULT_UI_LANGUAGE)
        )
        self.translation_bundle = load_translation_bundle(PROJECT_ROOT, self.ui_language)
        self.title(self.tr("app.title"))

    def schedule_gui_callback(
        self,
        delay_ms: int,
        callback: Callable[..., object],
        *args: object,
    ) -> bool:
        if self.is_shutting_down:
            return False
        try:
            self.after(delay_ms, callback, *args)
            return True
        except (RuntimeError, tk.TclError):
            return False

    def show_info(self, title_key: str, message_key: str, **params: object) -> None:
        messagebox.showinfo(self.tr(title_key), self.tr(message_key, **params))

    def show_warning(self, title_key: str, message_key: str, **params: object) -> None:
        messagebox.showwarning(self.tr(title_key), self.tr(message_key, **params))

    def show_error(self, title_key: str, message_key: str, **params: object) -> None:
        messagebox.showerror(self.tr(title_key), self.tr(message_key, **params))

    def ask_yes_no(self, title_key: str, message_key: str, **params: object) -> bool:
        return messagebox.askyesno(self.tr(title_key), self.tr(message_key, **params))

    def ask_yes_no_cancel(
        self,
        title_key: str,
        message_key: str,
        **params: object,
    ) -> bool | None:
        return messagebox.askyesnocancel(self.tr(title_key), self.tr(message_key, **params))

    def format_percent_text(self, value: float) -> str:
        return self.tr("common.progress.percent", percent=int(value * 100))

    def format_progress_summary(self, percent: float, processed: int, total: int) -> str:
        return self.tr(
            "common.progress.summary",
            percent=percent * 100,
            processed=processed,
            total=total,
        )

    def get_range_mode_options(self) -> dict[str, str]:
        return {
            "today": self.tr("settings.range_mode.today"),
            "yesterday": self.tr("settings.range_mode.yesterday"),
            "twodays": self.tr("settings.range_mode.twodays"),
            "custom": self.tr("settings.range_mode.custom"),
        }

    def get_range_mode_label(self, value: str) -> str:
        return self.get_range_mode_options().get(value, value)

    def get_selected_range_mode(self) -> str:
        selected_value = self.var_range.get()
        range_mode_options = self.get_range_mode_options()
        if selected_value in range_mode_options:
            return selected_value
        reverse_options = {label: key for key, label in range_mode_options.items()}
        return reverse_options.get(selected_value, selected_value)

    def get_legacy_cycle_mode_options(self) -> dict[str, str]:
        return {
            "incremental": self.tr("cycle_ops.legacy.mode.incremental"),
            "all": self.tr("cycle_ops.legacy.mode.all"),
            "today": self.tr("cycle_ops.legacy.mode.today"),
            "yesterday": self.tr("cycle_ops.legacy.mode.yesterday"),
            "custom": self.tr("cycle_ops.legacy.mode.custom"),
        }

    def get_legacy_cycle_mode_label(self, value: str) -> str:
        return self.get_legacy_cycle_mode_options().get(value, value)

    def get_selected_legacy_cycle_mode(self) -> str:
        selected_value = self.var_legacy_cycle_range.get()
        legacy_mode_options = self.get_legacy_cycle_mode_options()
        if selected_value in legacy_mode_options:
            return selected_value
        reverse_options = {label: key for key, label in legacy_mode_options.items()}
        return reverse_options.get(selected_value, selected_value)

    def get_training_mode_options(self) -> dict[str, str]:
        return {
            "build-all": self.tr("data_mgmt.training.mode.build_all"),
            "build-base": self.tr("data_mgmt.training.mode.build_base"),
            "build-v1": self.tr("data_mgmt.training.mode.build_v1"),
        }

    def get_training_mode_label(self, value: str) -> str:
        return self.get_training_mode_options().get(value, value)

    def get_selected_training_mode(self) -> str:
        selected_value = self.var_training_mode.get()
        training_mode_options = self.get_training_mode_options()
        if selected_value in training_mode_options:
            return selected_value
        reverse_options = {label: key for key, label in training_mode_options.items()}
        return reverse_options.get(selected_value, selected_value)

    def refresh_sidebar_texts(self) -> None:
        if hasattr(self, "logo_label") and self.logo_label.winfo_exists():
            self.logo_label.configure(text=self.tr("app.brand"))
        if hasattr(self, "btn_dash") and self.btn_dash.winfo_exists():
            self.btn_dash.configure(text=self.tr("sidebar.dashboard"))
        if hasattr(self, "btn_settings") and self.btn_settings.winfo_exists():
            self.btn_settings.configure(text=self.tr("sidebar.settings"))
        if hasattr(self, "btn_logs") and self.btn_logs.winfo_exists():
            self.btn_logs.configure(text=self.tr("sidebar.logs"))
        if hasattr(self, "btn_work_log") and self.btn_work_log.winfo_exists():
            self.btn_work_log.configure(text=self.tr("sidebar.work_log"))
        if hasattr(self, "btn_cycle_ops") and self.btn_cycle_ops.winfo_exists():
            self.btn_cycle_ops.configure(text=self.tr("sidebar.cycle_ops"))
        if hasattr(self, "btn_data") and self.btn_data.winfo_exists():
            self.btn_data.configure(text=self.tr("sidebar.data_mgmt"))

    def refresh_current_view(self) -> None:
        current_view = self.current_view
        self.current_view = ""
        if current_view == "dashboard":
            self.show_dashboard()
            return
        if current_view == "settings":
            self.show_settings()
            return
        if current_view == "logs":
            self.show_logs()
            return
        if current_view == "work_log":
            self.show_work_log()
            return
        if current_view == "cycle_ops":
            self.show_cycle_ops()
            return
        if current_view == "data_mgmt":
            self.show_data_mgmt()


    def on_closing(self):
        if not self.confirm_leave_settings(self.tr("app.close.target")):
            return

        supabase_url = self.cfg.get('SUPABASE_URL', '')
        if not is_local_supabase_target(supabase_url):
            self.close_application()
            return

        try:
            runtime = resolve_local_supabase_runtime(supabase_url, self.tr_map)
        except Exception:
            self.close_application()
            return

        if self.is_supabase_stopping:
            should_close = self.ask_yes_no(
                "dialog.app_close_while_stopping.title",
                "dialog.app_close_while_stopping.body",
            )
            if should_close:
                self.close_application()
            return

        if self.is_supabase_starting:
            should_close = self.ask_yes_no(
                "dialog.app_close_while_starting.title",
                "dialog.app_close_while_starting.body",
            )
            if should_close:
                self.close_application()
            return

        if not is_any_local_supabase_service_ready(runtime):
            self.close_application()
            return

        should_stop = self.ask_yes_no_cancel(
            "dialog.app_close.title",
            "dialog.app_close.body",
        )
        if should_stop is None:
            return
        if should_stop:
            self.stop_local_supabase(runtime, True)
            return
        self.close_application()

    def close_application(self):
        self.is_shutting_down = True
        if self.resize_debounce_after_id is not None:
            try:
                self.after_cancel(self.resize_debounce_after_id)
            except tk.TclError:
                pass
            self.resize_debounce_after_id = None
        if self.label_wrap_after_id is not None:
            try:
                self.after_cancel(self.label_wrap_after_id)
            except tk.TclError:
                pass
            self.label_wrap_after_id = None
        self.cancel_dashboard_callbacks()
        try:
            self.quit()
        except tk.TclError:
            pass
        try:
            self.destroy()
        except tk.TclError:
            pass

    def close_splash(self):
        try:
            import pyi_splash
            pyi_splash.close()
            print("Splash screen closed.")
        except ImportError:
            pass

    def create_sidebar(self):
        self.sidebar = ctk.CTkFrame(self, width=230, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.sidebar.grid_columnconfigure(0, weight=1)
        self.sidebar.grid_rowconfigure(6, weight=1)
        
        # Load Logo
        logo_path = resource_path(os.path.join('assets', 'logo.png'))
        try:
            from PIL import Image

            logo_img = ctk.CTkImage(light_image=Image.open(logo_path), dark_image=Image.open(logo_path), size=(80, 80))
            self.logo_label = ctk.CTkLabel(
                self.sidebar,
                text=self.tr("app.brand"),
                image=logo_img,
                compound="top",
                font=ctk.CTkFont(size=20, weight="bold"),
                justify="center",
            )
        except Exception as e:
            print(f"Logo load failed: {e}")
            self.logo_label = ctk.CTkLabel(
                self.sidebar,
                text=self.tr("app.brand"),
                font=ctk.CTkFont(size=20, weight="bold"),
                justify="center",
            )
            
        self.logo_label.grid(row=0, column=0, padx=20, pady=(20, 10))
        self.logo_label.configure(wraplength=170)
        
        self.btn_dash = ctk.CTkButton(self.sidebar, text=self.tr("sidebar.dashboard"), command=self.show_dashboard)
        self.btn_dash.grid(row=1, column=0, sticky="ew", padx=18, pady=10)
        
        self.btn_settings = ctk.CTkButton(self.sidebar, text=self.tr("sidebar.settings"), command=self.show_settings)
        self.btn_settings.grid(row=2, column=0, sticky="ew", padx=18, pady=10)
        
        self.btn_logs = ctk.CTkButton(self.sidebar, text=self.tr("sidebar.logs"), command=self.show_logs)
        self.btn_logs.grid(row=3, column=0, sticky="ew", padx=18, pady=10)
        
        self.btn_work_log = ctk.CTkButton(self.sidebar, text=self.tr("sidebar.work_log"), command=self.show_work_log)
        self.btn_work_log.grid(row=4, column=0, sticky="ew", padx=18, pady=10)

        self.btn_cycle_ops = ctk.CTkButton(self.sidebar, text=self.tr("sidebar.cycle_ops"), command=self.show_cycle_ops)
        self.btn_cycle_ops.grid(row=5, column=0, sticky="ew", padx=18, pady=10)

        self.btn_data = ctk.CTkButton(self.sidebar, text=self.tr("sidebar.data_mgmt"), command=self.show_data_mgmt)
        self.btn_data.grid(row=6, column=0, sticky="ew", padx=18, pady=10)
        
        # Status indicator at bottom
        self.status_label = ctk.CTkLabel(self.sidebar, text=self.tr("common.status.ready"), text_color="gray")

        # Auto-Start Upload Check
        if self.cfg.get('AUTO_UPLOAD') == 'true':
            self.log_queue.put(self.tr("dashboard.auto_upload.enabled"))
            self.after(5000, self.auto_start_upload)

    def auto_start_upload(self):
        """Called after delay if AUTO_UPLOAD is true"""
        if not self.is_uploading:
            if not self._ensure_upload_state_ready():
                return
            self.log_queue.put(self.tr("dashboard.auto_upload.started"))
            self.on_start()

    def create_main_area(self):
        # 페이지 셸과 실제 페이지 루트를 분리해 공통 레이아웃 기준을 유지한다.
        self.main_shell = ctk.CTkFrame(self, corner_radius=0, fg_color="transparent")
        self.main_shell.grid(row=0, column=1, sticky="nsew")
        self.main_shell.grid_rowconfigure(0, weight=1)
        self.main_shell.grid_columnconfigure(0, weight=1)
        self.main_frame = self.create_page_root(self.main_shell)
        self.bind_responsive_resize()

    def create_page_root(self, parent: tk.Misc) -> ctk.CTkFrame:
        page_root = ctk.CTkFrame(parent, corner_radius=0, fg_color="transparent")
        page_root.grid(row=0, column=0, sticky="nsew")
        page_root.grid_rowconfigure(0, weight=1)
        page_root.grid_columnconfigure(0, weight=1)
        return page_root

    def cancel_dashboard_callbacks(self) -> None:
        if self.dashboard_layout_after_id is not None:
            try:
                self.after_cancel(self.dashboard_layout_after_id)
            except tk.TclError:
                pass
            self.dashboard_layout_after_id = None
        if self.dashboard_update_after_id is not None:
            try:
                self.after_cancel(self.dashboard_update_after_id)
            except tk.TclError:
                pass
            self.dashboard_update_after_id = None
        self.is_dashboard_update_loop_running = False
        self.dashboard_view_generation += 1
        self.dashboard_layout_signature = None
        self.rendered_wsl_storage_snapshot = None

    def build_work_log_view_state(self) -> WorkLogViewState:
        return WorkLogViewState(
            selected_path=self.selected_work_log_path,
            messages=tuple(self.work_log_messages),
        )

    def restore_work_log_view_state(self, view_state: WorkLogViewState) -> None:
        if view_state.selected_path.strip() == "":
            self.lbl_work_log_file.configure(
                text=self.tr("work_log.label.no_file"),
                text_color="gray",
            )
            self.btn_upload_work_log.configure(state="disabled")
        else:
            self.lbl_work_log_file.configure(
                text=os.path.basename(view_state.selected_path),
                text_color="white",
            )
            self.btn_upload_work_log.configure(state="normal")

        if view_state.messages == ():
            return
        self.work_log_box.insert("end", "\n".join(view_state.messages) + "\n")
        self.work_log_box.see("end")

    def clear_main(self):
        self.cancel_dashboard_callbacks()
        if self.label_wrap_after_id is not None:
            try:
                self.after_cancel(self.label_wrap_after_id)
            except tk.TclError:
                pass
            self.label_wrap_after_id = None
        self.label_wrap_bindings.clear()
        if hasattr(self, "main_frame") and self.main_frame.winfo_exists():
            self.main_frame.destroy()
        self.main_frame = self.create_page_root(self.main_shell)
        self.schedule_responsive_layout_refresh()

    def confirm_leave_settings(self, target_name: str) -> bool:
        if self.current_view != "settings":
            return True
        if not self.is_settings_dirty:
            return True

        should_save = messagebox.askyesnocancel(
            self.tr("dialog.unsaved_changes.title"),
            self.tr("dialog.unsaved_changes.body", target_name=target_name),
        )
        if should_save is None:
            return False
        if should_save:
            return self.on_save()
        return True

    def confirm_leave_data_tasks(self, target_name: str) -> bool:
        if not self.is_data_task_running:
            return True
        self.show_warning(
            "dialog.data_task_running.title",
            "dialog.data_task_running.body",
            target_name=target_name,
        )
        return False

    # --- Views ---
    def show_dashboard(self):
        if self.current_view == "dashboard":
            if hasattr(self, "hero_frame") and self.hero_frame.winfo_exists():
                self.refresh_local_supabase_button()
                self.render_wsl_storage_card()
                self.refresh_upload_operational_cards()
                self.schedule_dashboard_update_loop(0)
                self.schedule_dashboard_layout_refresh(None)
                return
        if not self.confirm_leave_data_tasks(self.tr("sidebar.dashboard")):
            return
        if not self.confirm_leave_settings(self.tr("navigation.dashboard")):
            return
        self.current_view = "dashboard"
        self.clear_main()
        self.record_startup_timing("dashboard_clear_main")

        self.main_frame.grid_rowconfigure(0, weight=0)
        self.main_frame.grid_rowconfigure(1, weight=1)
        self.main_frame.grid_rowconfigure(2, weight=0)

        self.hero_frame = ctk.CTkFrame(self.main_frame)
        self.hero_frame.grid(row=0, column=0, sticky="ew", pady=(0, 16))
        self.hero_frame.grid_columnconfigure(0, weight=1)

        self.hero_status_frame = ctk.CTkFrame(self.hero_frame, fg_color="transparent")
        self.hero_status_frame.grid(row=0, column=0, sticky="ew", padx=18, pady=(18, 14))
        self.hero_status_frame.grid_columnconfigure(0, weight=1)

        self.lbl_big_status = ctk.CTkLabel(
            self.hero_status_frame,
            text=self.tr("common.status.waiting"),
            font=ctk.CTkFont(size=24, weight="bold"),
            anchor="w",
        )
        self.lbl_big_status.grid(row=0, column=0, sticky="w", pady=(0, 8))

        self.prog_bar = ctk.CTkProgressBar(self.hero_status_frame)
        self.prog_bar.grid(row=1, column=0, sticky="ew", pady=(0, 10))
        self.prog_bar.set(0)

        self.lbl_prog_text = ctk.CTkLabel(
            self.hero_status_frame,
            text=self.format_progress_summary(0.0, 0, 0),
            anchor="w",
        )
        self.lbl_prog_text.grid(row=2, column=0, sticky="w", pady=(0, 18))

        self.lbl_runtime_context = ctk.CTkLabel(
            self.hero_status_frame,
            text=build_runtime_context_text(
                self.config_metadata,
                self.cfg.get('SUPABASE_URL', ''),
                self.cfg.get('EDGE_FUNCTION_URL', ''),
                self.tr_map,
            ),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        self.lbl_runtime_context.grid(row=3, column=0, sticky="ew", pady=(0, 10))
        self.bind_label_wrap(self.lbl_runtime_context, horizontal_padding=20, min_wraplength=420)
        self.lbl_local_supabase_status = ctk.CTkLabel(
            self.hero_status_frame,
            textvariable=self.var_local_supabase_status,
            justify="left",
            anchor="w",
        )
        self.lbl_local_supabase_status.grid(row=4, column=0, sticky="ew", pady=(0, 6))
        self.bind_label_wrap(self.lbl_local_supabase_status, horizontal_padding=20, min_wraplength=420)
        self.lbl_state_store_status = ctk.CTkLabel(
            self.hero_status_frame,
            textvariable=self.var_state_store_status,
            justify="left",
            anchor="w",
        )
        self.lbl_state_store_status.grid(row=5, column=0, sticky="ew", pady=(0, 6))
        self.lbl_state_store_detail = ctk.CTkLabel(
            self.hero_status_frame,
            text="",
            justify="left",
            anchor="w",
            text_color="gray",
        )
        self.lbl_state_store_detail.grid(row=6, column=0, sticky="ew", pady=(0, 6))
        self.bind_label_wrap(self.lbl_state_store_detail, horizontal_padding=20, min_wraplength=420)
        self.upload_precheck_frame = ctk.CTkFrame(self.hero_status_frame)
        self.upload_precheck_frame.grid(row=7, column=0, sticky="ew", pady=(4, 0))
        self.upload_precheck_frame.grid_columnconfigure(0, weight=1)
        self.lbl_upload_precheck_title = ctk.CTkLabel(
            self.upload_precheck_frame,
            text=self.tr("dashboard.upload.preflight.title"),
            font=ctk.CTkFont(size=15, weight="bold"),
            anchor="w",
        )
        self.lbl_upload_precheck_title.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 2))
        self.lbl_upload_precheck_summary = ctk.CTkLabel(
            self.upload_precheck_frame,
            text=self.tr("common.status.waiting"),
            anchor="w",
        )
        self.lbl_upload_precheck_summary.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 2))
        self.lbl_upload_precheck_items = ctk.CTkLabel(
            self.upload_precheck_frame,
            text="",
            justify="left",
            anchor="w",
            text_color="gray",
        )
        self.lbl_upload_precheck_items.grid(row=2, column=0, sticky="ew", padx=14, pady=(0, 12))
        self.bind_label_wrap(self.lbl_upload_precheck_items, horizontal_padding=36, min_wraplength=420)
        self.local_supabase_progress = ctk.CTkProgressBar(
            self.hero_status_frame,
            width=320,
            mode="indeterminate",
        )
        self.record_startup_timing("dashboard_build_hero")

        self.dashboard_body_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.dashboard_body_frame.grid(row=1, column=0, sticky="nsew")

        self.wsl_storage_frame = ctk.CTkFrame(self.dashboard_body_frame)
        self.wsl_storage_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 10))
        self.wsl_storage_frame.grid_columnconfigure(0, weight=1)
        self.wsl_storage_frame.grid_rowconfigure(5, weight=0)

        self.wsl_storage_header_frame = ctk.CTkFrame(self.wsl_storage_frame, fg_color="transparent")
        self.wsl_storage_header_frame.grid(row=0, column=0, sticky="ew", padx=16, pady=(14, 6))
        self.wsl_storage_header_frame.grid_columnconfigure(0, weight=1)

        self.lbl_wsl_storage_title = ctk.CTkLabel(
            self.wsl_storage_header_frame,
            text=self.tr("dashboard.wsl_storage.title"),
            font=ctk.CTkFont(size=16, weight="bold"),
            anchor="w",
        )
        self.lbl_wsl_storage_title.grid(row=0, column=0, sticky="w")

        self.lbl_wsl_storage_badge = ctk.CTkLabel(
            self.wsl_storage_header_frame,
            text="",
            corner_radius=999,
            padx=10,
            pady=4,
            anchor="center",
        )
        self.lbl_wsl_storage_badge.grid(row=0, column=1, sticky="e", padx=(8, 8))

        self.btn_refresh_wsl_storage = ctk.CTkButton(
            self.wsl_storage_header_frame,
            text=self.tr("dashboard.wsl_storage.button.refresh"),
            command=self.request_wsl_storage_refresh,
            width=110,
            fg_color="#3B8ED0",
            hover_color="#2D6FA6",
        )
        self.btn_refresh_wsl_storage.grid(row=0, column=2, sticky="e")

        self.wsl_storage_metrics_frame = ctk.CTkFrame(self.wsl_storage_frame, fg_color="transparent")
        self.wsl_storage_metrics_frame.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 8))

        self.wsl_storage_used_card = ctk.CTkFrame(self.wsl_storage_metrics_frame)
        self.wsl_storage_used_card.grid_columnconfigure(0, weight=1)
        self.lbl_wsl_storage_used_label = ctk.CTkLabel(
            self.wsl_storage_used_card,
            text=self.tr("dashboard.wsl_storage.label.used"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_used_label.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 2))
        self.lbl_wsl_storage_used_value = ctk.CTkLabel(
            self.wsl_storage_used_card,
            text="—",
            font=ctk.CTkFont(size=22, weight="bold"),
            anchor="w",
        )
        self.lbl_wsl_storage_used_value.grid(row=1, column=0, sticky="w", padx=14, pady=(0, 12))

        self.wsl_storage_available_card = ctk.CTkFrame(self.wsl_storage_metrics_frame)
        self.wsl_storage_available_card.grid(row=0, column=1, sticky="ew", padx=4)
        self.wsl_storage_available_card.grid_columnconfigure(0, weight=1)
        self.lbl_wsl_storage_available_label = ctk.CTkLabel(
            self.wsl_storage_available_card,
            text=self.tr("dashboard.wsl_storage.label.available"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_available_label.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 2))
        self.lbl_wsl_storage_available_value = ctk.CTkLabel(
            self.wsl_storage_available_card,
            text="—",
            font=ctk.CTkFont(size=22, weight="bold"),
            anchor="w",
        )
        self.lbl_wsl_storage_available_value.grid(row=1, column=0, sticky="w", padx=14, pady=(0, 12))

        self.wsl_storage_total_card = ctk.CTkFrame(self.wsl_storage_metrics_frame)
        self.wsl_storage_total_card.grid(row=0, column=2, sticky="ew", padx=(8, 0))
        self.wsl_storage_total_card.grid_columnconfigure(0, weight=1)
        self.lbl_wsl_storage_total_label = ctk.CTkLabel(
            self.wsl_storage_total_card,
            text=self.tr("dashboard.wsl_storage.label.total"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_total_label.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 2))
        self.lbl_wsl_storage_total_value = ctk.CTkLabel(
            self.wsl_storage_total_card,
            text="—",
            font=ctk.CTkFont(size=22, weight="bold"),
            anchor="w",
        )
        self.lbl_wsl_storage_total_value.grid(row=1, column=0, sticky="w", padx=14, pady=(0, 12))

        self.wsl_storage_metric_cards: list[ctk.CTkFrame] = [
            self.wsl_storage_used_card,
            self.wsl_storage_available_card,
            self.wsl_storage_total_card,
        ]

        self.wsl_storage_usage_row = ctk.CTkFrame(self.wsl_storage_frame, fg_color="transparent")
        self.wsl_storage_usage_row.grid(row=2, column=0, sticky="ew", padx=16, pady=(0, 4))
        self.wsl_storage_usage_row.grid_columnconfigure(0, weight=1)
        self.wsl_storage_usage_row.grid_columnconfigure(1, weight=0)

        self.lbl_wsl_storage_usage_label = ctk.CTkLabel(
            self.wsl_storage_usage_row,
            text=self.tr("dashboard.wsl_storage.label.usage"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_usage_label.grid(row=0, column=0, sticky="w")
        self.lbl_wsl_storage_usage_value = ctk.CTkLabel(
            self.wsl_storage_usage_row,
            text="—",
            anchor="e",
        )
        self.lbl_wsl_storage_usage_value.grid(row=0, column=1, sticky="e")

        self.wsl_storage_progress = ctk.CTkProgressBar(self.wsl_storage_frame, height=14)
        self.wsl_storage_progress.grid(row=3, column=0, sticky="ew", padx=16, pady=(0, 8))
        self.wsl_storage_progress.set(0)

        self.wsl_storage_detail_frame = ctk.CTkFrame(self.wsl_storage_frame)
        self.wsl_storage_detail_frame.grid(row=4, column=0, sticky="ew", padx=16, pady=(0, 8))
        self.wsl_storage_detail_frame.grid_columnconfigure(0, weight=1)
        self.lbl_wsl_storage_detail = ctk.CTkLabel(
            self.wsl_storage_detail_frame,
            text="",
            justify="left",
            anchor="w",
            text_color="gray",
            font=ctk.CTkFont(size=13, weight="bold"),
        )
        self.lbl_wsl_storage_detail.grid(row=0, column=0, sticky="ew", padx=14, pady=10)
        self.bind_label_wrap(self.lbl_wsl_storage_detail, horizontal_padding=32, min_wraplength=420)

        self.wsl_storage_info_frame = ctk.CTkFrame(self.wsl_storage_frame, fg_color="transparent")
        self.wsl_storage_info_frame.grid(row=5, column=0, sticky="ew", padx=16, pady=(0, 14))

        self.wsl_storage_distro_item = ctk.CTkFrame(self.wsl_storage_info_frame)
        self.wsl_storage_distro_item.grid_columnconfigure(0, weight=1)

        self.lbl_wsl_storage_distro_label = ctk.CTkLabel(
            self.wsl_storage_distro_item,
            text=self.tr("dashboard.wsl_storage.label.distro"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_distro_label.grid(row=0, column=0, sticky="w")
        self.lbl_wsl_storage_distro_value = ctk.CTkLabel(
            self.wsl_storage_distro_item,
            text="—",
            anchor="w",
        )
        self.lbl_wsl_storage_distro_value.grid(row=1, column=0, sticky="w", pady=(0, 8))

        self.wsl_storage_source_item = ctk.CTkFrame(self.wsl_storage_info_frame)
        self.wsl_storage_source_item.grid_columnconfigure(0, weight=1)
        self.lbl_wsl_storage_source_label = ctk.CTkLabel(
            self.wsl_storage_source_item,
            text=self.tr("dashboard.wsl_storage.label.source"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_source_label.grid(row=0, column=0, sticky="w")
        self.lbl_wsl_storage_source_value = ctk.CTkLabel(
            self.wsl_storage_source_item,
            text="—",
            anchor="w",
        )
        self.lbl_wsl_storage_source_value.grid(row=1, column=0, sticky="w", pady=(0, 8))

        self.wsl_storage_vhdx_item = ctk.CTkFrame(self.wsl_storage_info_frame)
        self.wsl_storage_vhdx_item.grid_columnconfigure(0, weight=1)

        self.lbl_wsl_storage_vhdx_label = ctk.CTkLabel(
            self.wsl_storage_vhdx_item,
            text=self.tr("dashboard.wsl_storage.label.host_vhdx_size"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_vhdx_label.grid(row=0, column=0, sticky="w")
        self.lbl_wsl_storage_vhdx_value = ctk.CTkLabel(
            self.wsl_storage_vhdx_item,
            text="—",
            anchor="w",
        )
        self.lbl_wsl_storage_vhdx_value.grid(row=1, column=0, sticky="w", pady=(0, 8))

        self.wsl_storage_host_free_item = ctk.CTkFrame(self.wsl_storage_info_frame)
        self.wsl_storage_host_free_item.grid_columnconfigure(0, weight=1)
        self.lbl_wsl_storage_host_free_label = ctk.CTkLabel(
            self.wsl_storage_host_free_item,
            text=self.tr("dashboard.wsl_storage.label.host_drive_free"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_host_free_label.grid(row=0, column=0, sticky="w")
        self.lbl_wsl_storage_host_free_value = ctk.CTkLabel(
            self.wsl_storage_host_free_item,
            text="—",
            anchor="w",
        )
        self.lbl_wsl_storage_host_free_value.grid(row=1, column=0, sticky="w", pady=(0, 8))

        self.wsl_storage_meta_item = ctk.CTkFrame(self.wsl_storage_info_frame)
        self.wsl_storage_meta_item.grid_columnconfigure(0, weight=1)

        self.lbl_wsl_storage_meta_label = ctk.CTkLabel(
            self.wsl_storage_meta_item,
            text=self.tr("dashboard.wsl_storage.label.last_updated"),
            text_color="gray",
            anchor="w",
        )
        self.lbl_wsl_storage_meta_label.grid(row=0, column=0, sticky="w")
        self.lbl_wsl_storage_meta_value = ctk.CTkLabel(
            self.wsl_storage_meta_item,
            text="—",
            anchor="w",
        )
        self.lbl_wsl_storage_meta_value.grid(row=1, column=0, sticky="w")

        self.wsl_storage_info_items: list[ctk.CTkFrame] = [
            self.wsl_storage_distro_item,
            self.wsl_storage_source_item,
            self.wsl_storage_vhdx_item,
            self.wsl_storage_host_free_item,
            self.wsl_storage_meta_item,
        ]
        self.record_startup_timing("dashboard_build_wsl_card")

        self.tasks_frame = ctk.CTkScrollableFrame(
            self.dashboard_body_frame,
            label_text=self.tr("dashboard.label.task_status"),
        )
        self.tasks_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 10))

        self.upload_resume_card = ctk.CTkFrame(self.tasks_frame)
        self.upload_resume_card.pack(fill="x", padx=8, pady=(8, 8))
        self.upload_resume_card.grid_columnconfigure(0, weight=1)
        self.lbl_upload_resume_title = ctk.CTkLabel(
            self.upload_resume_card,
            text=self.tr("dashboard.upload.resume.title"),
            font=ctk.CTkFont(size=15, weight="bold"),
            anchor="w",
        )
        self.lbl_upload_resume_title.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 2))
        self.lbl_upload_resume_state = ctk.CTkLabel(
            self.upload_resume_card,
            text=self.tr("dashboard.upload.status.resume_empty"),
            anchor="w",
        )
        self.lbl_upload_resume_state.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 2))
        self.lbl_upload_resume_detail = ctk.CTkLabel(
            self.upload_resume_card,
            text="",
            justify="left",
            anchor="w",
            text_color="gray",
        )
        self.lbl_upload_resume_detail.grid(row=2, column=0, sticky="ew", padx=14, pady=(0, 12))
        self.bind_label_wrap(self.lbl_upload_resume_detail, horizontal_padding=36, min_wraplength=320)

        self.recent_success_card = ctk.CTkFrame(self.tasks_frame)
        self.recent_success_card.pack(fill="x", padx=8, pady=(0, 8))
        self.recent_success_card.grid_columnconfigure(0, weight=1)
        self.lbl_recent_success_title = ctk.CTkLabel(
            self.recent_success_card,
            text=self.tr("dashboard.upload.recent_success.title"),
            font=ctk.CTkFont(size=15, weight="bold"),
            anchor="w",
        )
        self.lbl_recent_success_title.grid(row=0, column=0, sticky="w", padx=14, pady=(12, 2))
        self.lbl_recent_success_state = ctk.CTkLabel(
            self.recent_success_card,
            text=self.tr("dashboard.upload.status.recent_success_empty"),
            anchor="w",
        )
        self.lbl_recent_success_state.grid(row=1, column=0, sticky="ew", padx=14, pady=(0, 2))
        self.lbl_recent_success_detail = ctk.CTkLabel(
            self.recent_success_card,
            text="",
            justify="left",
            anchor="w",
            text_color="gray",
        )
        self.lbl_recent_success_detail.grid(row=2, column=0, sticky="ew", padx=14, pady=(0, 10))
        self.bind_label_wrap(self.lbl_recent_success_detail, horizontal_padding=36, min_wraplength=320)
        self.btn_rerun_recent_success = ctk.CTkButton(
            self.recent_success_card,
            text=self.tr("dashboard.upload.button.rerun_recent"),
            command=self.on_rerun_recent_success,
            state="disabled",
            width=150,
            fg_color="#3B8ED0",
            hover_color="#2D6FA6",
        )
        self.btn_rerun_recent_success.grid(row=3, column=0, sticky="ew", padx=14, pady=(0, 12))

        self.active_tasks_list_frame = ctk.CTkFrame(self.tasks_frame, fg_color="transparent")
        self.active_tasks_list_frame.pack(fill="both", expand=True, padx=8, pady=(0, 8))

        self.task_labels = {}
        self.record_startup_timing("dashboard_build_task_cards")

        self.action_frame = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.action_frame.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        self.action_frame.grid_columnconfigure(0, weight=1)
        self.action_frame.grid_columnconfigure(1, weight=1)

        self.supabase_action_row = ctk.CTkFrame(self.action_frame)
        self.supabase_action_row.grid(row=0, column=0, sticky="ew", padx=(0, 8), pady=(0, 8))

        self.upload_action_row = ctk.CTkFrame(self.action_frame)
        self.upload_action_row.grid(row=0, column=1, sticky="ew", padx=(8, 0), pady=(0, 8))

        start_state = "disabled" if self.is_uploading else "normal"
        self.btn_start = ctk.CTkButton(
            self.upload_action_row,
            text=self.tr("dashboard.button.start_upload"),
            command=self.on_start,
            state=start_state,
            width=150,
            fg_color="#2CC985",
            hover_color="#26A670",
        )

        pause_state = "normal" if self.is_uploading else "disabled"
        pause_text = (
            self.tr("dashboard.button.resume")
            if self.is_uploading and not self.pause_event.is_set()
            else self.tr("dashboard.button.pause")
        )

        self.btn_pause = ctk.CTkButton(
            self.upload_action_row,
            text=pause_text,
            command=self.on_pause,
            state=pause_state,
            width=120,
            fg_color="#E5C07B",
            hover_color="#D1A03D",
        )
        self.btn_preview = ctk.CTkButton(
            self.upload_action_row,
            text=self.tr("common.button.preview"),
            command=self.on_preview,
            width=120,
        )
        self.btn_retry_failed = ctk.CTkButton(
            self.upload_action_row,
            text=self.tr("dashboard.upload.button.retry_failed"),
            command=self.on_retry_failed,
            state="disabled",
            width=140,
            fg_color="#D97706",
            hover_color="#B45309",
        )
        self.btn_start_supabase = ctk.CTkButton(
            self.supabase_action_row,
            text=self.tr("dashboard.local_supabase.button.start"),
            command=self.on_start_local_supabase,
            width=180,
            fg_color="#3B8ED0",
            hover_color="#2D6FA6",
        )
        self.btn_open_studio = ctk.CTkButton(
            self.supabase_action_row,
            text=self.tr("dashboard.local_supabase.button.studio"),
            command=self.on_open_local_supabase_studio,
            width=150,
            fg_color="#3B8ED0",
            hover_color="#2D6FA6",
        )
        self.btn_stop_supabase = ctk.CTkButton(
            self.supabase_action_row,
            text=self.tr("dashboard.local_supabase.button.stop"),
            command=self.on_stop_local_supabase,
            width=180,
            fg_color="#D97706",
            hover_color="#B45309",
        )
        self.supabase_action_buttons: list[ctk.CTkButton] = [
            self.btn_start_supabase,
            self.btn_open_studio,
            self.btn_stop_supabase,
        ]
        self.upload_action_buttons: list[ctk.CTkButton] = [
            self.btn_retry_failed,
            self.btn_preview,
            self.btn_pause,
            self.btn_start,
        ]
        self.record_startup_timing("dashboard_build_action_rows")

        self.dashboard_body_frame.bind("<Configure>", self.schedule_dashboard_layout_refresh, add="+")
        self.wsl_storage_frame.bind("<Configure>", self.schedule_dashboard_layout_refresh, add="+")
        self.action_frame.bind("<Configure>", self.schedule_dashboard_layout_refresh, add="+")

        self.schedule_dashboard_layout_refresh(None)
        self.refresh_local_supabase_button()
        if self.is_wsl_storage_refreshing:
            self.render_wsl_storage_card()
        else:
            self.request_wsl_storage_refresh()
        self.refresh_upload_operational_cards()
        self.schedule_dashboard_update_loop(0)
        self.record_startup_timing("dashboard_schedule_followups")

    def schedule_dashboard_update_loop(self, delay_ms: int) -> None:
        if self.current_view != "dashboard":
            return
        if self.dashboard_update_after_id is not None:
            return
        if self.is_dashboard_update_loop_running:
            return
        dashboard_view_generation = self.dashboard_view_generation
        try:
            self.dashboard_update_after_id = self.after(
                delay_ms,
                self.update_dashboard_loop,
                dashboard_view_generation,
            )
        except (RuntimeError, tk.TclError):
            self.dashboard_update_after_id = None

    def schedule_dashboard_layout_refresh(self, event: object) -> None:
        if self.current_view != "dashboard":
            self.dashboard_layout_after_id = None
            return
        if not hasattr(self, "dashboard_body_frame") or not self.dashboard_body_frame.winfo_exists():
            self.dashboard_layout_after_id = None
            return
        if self.dashboard_layout_after_id is not None:
            return
        try:
            self.dashboard_layout_after_id = self.after(0, self.refresh_dashboard_layout)
        except (RuntimeError, tk.TclError):
            self.dashboard_layout_after_id = None

    def refresh_dashboard_layout(self) -> None:
        self.dashboard_layout_after_id = None
        if not hasattr(self, "dashboard_body_frame") or not self.dashboard_body_frame.winfo_exists():
            return

        body_width = self.dashboard_body_frame.winfo_width()
        if body_width <= 1:
            body_width = self.main_frame.winfo_width()

        is_split_body = body_width >= 1180
        self.dashboard_body_frame.grid_columnconfigure(0, weight=1)
        self.dashboard_body_frame.grid_columnconfigure(1, weight=1 if is_split_body else 0)
        self.dashboard_body_frame.grid_rowconfigure(0, weight=1 if is_split_body else 0)
        self.dashboard_body_frame.grid_rowconfigure(1, weight=1 if not is_split_body else 0)

        wsl_width = self.wsl_storage_frame.winfo_width()
        if wsl_width <= 1:
            wsl_width = body_width

        metric_column_count = 3 if wsl_width >= 900 else 2 if wsl_width >= 560 else 1
        info_column_count = 3 if wsl_width >= 900 else 2 if wsl_width >= 620 else 1
        footer_width = self.action_frame.winfo_width()
        if footer_width <= 1:
            footer_width = body_width

        is_split_footer = footer_width >= 1040
        button_stack = footer_width < 760
        layout_signature = DashboardLayoutSignature(
            is_split_body=is_split_body,
            metric_column_count=metric_column_count,
            info_column_count=info_column_count,
            is_split_footer=is_split_footer,
            button_stack=button_stack,
        )
        if self.dashboard_layout_signature == layout_signature:
            return
        self.dashboard_layout_signature = layout_signature

        self.wsl_storage_frame.grid_forget()
        self.tasks_frame.grid_forget()
        if is_split_body:
            self.wsl_storage_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 8), pady=(0, 10))
            self.tasks_frame.grid(row=0, column=1, sticky="nsew", padx=(8, 0), pady=(0, 10))
        else:
            self.wsl_storage_frame.grid(row=0, column=0, sticky="nsew", padx=0, pady=(0, 10))
            self.tasks_frame.grid(row=1, column=0, sticky="nsew", padx=0, pady=(0, 10))

        self.layout_dashboard_collection(
            self.wsl_storage_metrics_frame,
            self.wsl_storage_metric_cards,
            metric_column_count,
        )
        self.layout_dashboard_collection(
            self.wsl_storage_info_frame,
            self.wsl_storage_info_items,
            info_column_count,
        )
        self.action_frame.grid_columnconfigure(0, weight=1)
        self.action_frame.grid_columnconfigure(1, weight=1 if is_split_footer else 0)

        if is_split_footer:
            self.supabase_action_row.grid_configure(row=0, column=0, padx=(0, 8), pady=(0, 8))
            self.upload_action_row.grid_configure(row=0, column=1, padx=(8, 0), pady=(0, 8))
        else:
            self.supabase_action_row.grid_configure(row=0, column=0, padx=0, pady=(0, 8))
            self.upload_action_row.grid_configure(row=1, column=0, padx=0, pady=(0, 8))

        self.layout_dashboard_button_row(self.supabase_action_row, self.supabase_action_buttons, button_stack)
        self.layout_dashboard_button_row(self.upload_action_row, self.upload_action_buttons, button_stack)

    def layout_dashboard_collection(
        self,
        container: ctk.CTkFrame,
        items: list[ctk.CTkFrame],
        column_count: int,
    ) -> None:
        effective_column_count = min(max(column_count, 1), len(items))
        for column_index in range(len(items)):
            container.grid_columnconfigure(
                column_index,
                weight=1 if column_index < effective_column_count else 0,
            )

        for item_index, item in enumerate(items):
            row_index = item_index // effective_column_count
            column_index = item_index % effective_column_count
            left_padding = 0 if column_index == 0 else 6
            right_padding = 0 if column_index == effective_column_count - 1 else 6
            item.grid(
                row=row_index,
                column=column_index,
                sticky="nsew",
                padx=(left_padding, right_padding),
                pady=(0, 10),
            )

    def layout_dashboard_button_row(
        self,
        container: ctk.CTkFrame,
        buttons: list[ctk.CTkButton],
        stack_buttons: bool,
    ) -> None:
        if stack_buttons:
            for column_index in range(len(buttons)):
                container.grid_columnconfigure(column_index, weight=0)
            container.grid_columnconfigure(0, weight=1)
            for button_index, button in enumerate(buttons):
                button.grid(
                    row=button_index,
                    column=0,
                    sticky="ew",
                    padx=12,
                    pady=(0, 8),
                )
            return

        for column_index in range(len(buttons)):
            container.grid_columnconfigure(column_index, weight=1)
        for button_index, button in enumerate(buttons):
            left_padding = 0 if button_index == 0 else 6
            right_padding = 0 if button_index == len(buttons) - 1 else 6
            button.grid(
                row=0,
                column=button_index,
                sticky="ew",
                padx=(left_padding, right_padding),
                pady=12,
            )

    def show_settings(self):
        if self.current_view == "settings":
            return
        if not self.confirm_leave_data_tasks(self.tr("sidebar.settings")):
            return
        self.current_view = "settings"
        self.clear_main()
        self.main_frame.grid_rowconfigure(0, weight=1)
        self.main_frame.grid_rowconfigure(1, weight=0)
        self.main_frame.grid_rowconfigure(2, weight=0)
        self.main_frame.grid_rowconfigure(3, weight=0)
        self.main_frame.grid_columnconfigure(0, weight=1)

        sf = ctk.CTkScrollableFrame(self.main_frame, label_text=self.tr("settings.title"))
        sf.grid(row=0, column=0, sticky="nsew")
        sf.grid_columnconfigure(0, weight=1)

        self.var_url = tk.StringVar(value=self.cfg['SUPABASE_URL'])
        self.var_anon = tk.StringVar(value=self.cfg['SUPABASE_ANON_KEY'])
        self.var_edge = tk.StringVar(value=self.cfg['EDGE_FUNCTION_URL'])
        self.var_plc = tk.StringVar(value=self.cfg['PLC_DIR'])
        self.var_wsl_vhdx_path = tk.StringVar(value=self.cfg.get('WSL_VHDX_PATH', ''))

        self.var_smart_sync = tk.BooleanVar(value=(str(self.cfg.get('SMART_SYNC', 'true')).lower() == 'true'))
        self.var_auto_upload = tk.BooleanVar(value=(str(self.cfg.get('AUTO_UPLOAD', 'false')).lower() == 'true'))
        self.var_ui_language = tk.StringVar(
            value=normalize_language_code(self.cfg.get("UI_LANGUAGE", DEFAULT_UI_LANGUAGE))
        )
        self.var_range = tk.StringVar(
            value=self.get_range_mode_label(self.cfg['RANGE_MODE'])
        )
        custom_date_start, custom_date_end = resolve_custom_range_texts(
            self.cfg.get('CUSTOM_DATE_START', ''),
            self.cfg.get('CUSTOM_DATE_END', ''),
            self.cfg.get('CUSTOM_DATE', ''),
        )
        self.var_custom_date_start = tk.StringVar(value=custom_date_start)
        self.var_custom_date_end = tk.StringVar(value=custom_date_end)
        self.var_custom_range_summary = tk.StringVar(value="")

        def add_entry(parent: ctk.CTkFrame, label: str, var: tk.StringVar, row: int) -> ctk.CTkEntry:
            row_frame = ctk.CTkFrame(parent, fg_color="transparent")
            row_frame.grid(row=row, column=0, columnspan=3, sticky="ew", padx=10, pady=5)
            label_widget = ctk.CTkLabel(
                row_frame,
                text=label,
                width=170,
                anchor="w",
                justify="left",
            )
            entry_widget = ctk.CTkEntry(row_frame, textvariable=var)
            row_frame.bind(
                "<Configure>",
                lambda _event, frame=row_frame, lw=label_widget, ew=entry_widget: self.layout_responsive_labeled_entry_row(
                    frame,
                    lw,
                    ew,
                ),
            )
            self.layout_responsive_labeled_entry_row(row_frame, label_widget, entry_widget)
            return entry_widget

        def add_path(
            parent: ctk.CTkFrame,
            label: str,
            var: tk.StringVar,
            row: int,
            cmd: Callable[[], None],
        ) -> tuple[ctk.CTkEntry, ctk.CTkButton]:
            row_frame = ctk.CTkFrame(parent, fg_color="transparent")
            row_frame.grid(row=row, column=0, columnspan=3, sticky="ew", padx=10, pady=5)
            label_widget = ctk.CTkLabel(
                row_frame,
                text=label,
                width=170,
                anchor="w",
                justify="left",
            )
            entry_widget = ctk.CTkEntry(row_frame, textvariable=var)
            button_widget = ctk.CTkButton(
                row_frame,
                text=self.tr("common.button.browse"),
                width=80,
                command=cmd,
            )
            row_frame.bind(
                "<Configure>",
                lambda _event, frame=row_frame, lw=label_widget, ew=entry_widget, bw=button_widget: self.layout_responsive_labeled_entry_action_row(
                    frame,
                    lw,
                    ew,
                    bw,
                ),
            )
            self.layout_responsive_labeled_entry_action_row(
                row_frame,
                label_widget,
                entry_widget,
                button_widget,
            )
            return entry_widget, button_widget

        def add_option_row(
            parent: ctk.CTkFrame,
            label: str,
            control_widget: ctk.CTkBaseClass,
            row: int,
        ) -> None:
            row_frame = ctk.CTkFrame(parent, fg_color="transparent")
            row_frame.grid(row=row, column=0, columnspan=3, sticky="ew", padx=10, pady=5)
            label_widget = ctk.CTkLabel(
                row_frame,
                text=label,
                width=170,
                anchor="w",
                justify="left",
            )
            row_frame.bind(
                "<Configure>",
                lambda _event, frame=row_frame, lw=label_widget, cw=control_widget: self.layout_responsive_labeled_entry_row(
                    frame,
                    lw,
                    cw,
                ),
            )
            self.layout_responsive_labeled_entry_row(row_frame, label_widget, control_widget)

        grp_conn = ctk.CTkFrame(sf)
        grp_conn.grid(row=0, column=0, sticky="ew", padx=10, pady=10)
        grp_conn.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(
            grp_conn,
            text=self.tr("settings.section.connection"),
            font=ctk.CTkFont(weight="bold"),
        ).grid(row=0, column=0, sticky="w", padx=10, pady=5)
        add_entry(grp_conn, self.tr("settings.label.supabase_url"), self.var_url, 1)
        add_entry(grp_conn, self.tr("settings.label.anon_key"), self.var_anon, 2)
        edge_row_frame = ctk.CTkFrame(grp_conn, fg_color="transparent")
        edge_row_frame.grid(row=3, column=0, columnspan=3, sticky="ew", padx=10, pady=5)
        edge_label = ctk.CTkLabel(
            edge_row_frame,
            text=self.tr("settings.label.edge_url"),
            width=170,
            anchor="w",
            justify="left",
        )
        edge_entry = ctk.CTkEntry(edge_row_frame, textvariable=self.var_edge)
        edge_button = ctk.CTkButton(
            edge_row_frame,
            text=self.tr("common.button.clear"),
            width=80,
            command=self.on_restore_auto_edge_url,
        )
        edge_row_frame.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_action_row(
                edge_row_frame,
                edge_label,
                edge_entry,
                edge_button,
            ),
        )
        self.layout_responsive_labeled_entry_action_row(
            edge_row_frame,
            edge_label,
            edge_entry,
            edge_button,
        )
        edge_help_label = ctk.CTkLabel(
            grp_conn,
            text=self.tr("settings.edge_url.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        edge_help_label.grid(
            row=4, column=0, columnspan=3, sticky="w", padx=10, pady=(0, 5)
        )
        self.bind_label_wrap(edge_help_label, horizontal_padding=30, min_wraplength=420)
        self.lbl_settings_context = ctk.CTkLabel(
            grp_conn,
            text=build_runtime_context_text(
                self.config_metadata,
                self.cfg.get('SUPABASE_URL', ''),
                self.cfg.get('EDGE_FUNCTION_URL', ''),
                self.tr_map,
            ),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        self.lbl_settings_context.grid(row=5, column=0, columnspan=3, sticky="ew", padx=10, pady=(0, 5))
        self.bind_label_wrap(self.lbl_settings_context, horizontal_padding=30, min_wraplength=420)
        ctk.CTkButton(
            grp_conn,
            text=self.tr("settings.button.open_appdata_dir"),
            command=self.open_appdata_dir,
        ).grid(
            row=6, column=0, sticky="w", padx=10, pady=(0, 5)
        )

        grp_folder = ctk.CTkFrame(sf)
        grp_folder.grid(row=1, column=0, sticky="ew", padx=10, pady=10)
        grp_folder.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(
            grp_folder,
            text=self.tr("settings.section.folders"),
            font=ctk.CTkFont(weight="bold"),
        ).grid(row=0, column=0, sticky="w", padx=10, pady=5)
        add_path(grp_folder, self.tr("settings.label.data_folder"), self.var_plc, 1, self.pick_plc)
        add_path(
            grp_folder,
            self.tr("settings.label.wsl_vhdx_path"),
            self.var_wsl_vhdx_path,
            2,
            self.pick_wsl_vhdx,
        )
        wsl_vhdx_help_label = ctk.CTkLabel(
            grp_folder,
            text=self.tr("settings.wsl_vhdx.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        wsl_vhdx_help_label.grid(
            row=3, column=0, columnspan=3, sticky="w", padx=10, pady=(0, 5)
        )
        self.bind_label_wrap(wsl_vhdx_help_label, horizontal_padding=30, min_wraplength=420)
        folder_help_label = ctk.CTkLabel(
            grp_folder,
            text=self.tr("settings.temp_dir.unused"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        folder_help_label.grid(
            row=4, column=0, columnspan=3, sticky="w", padx=10, pady=(0, 5)
        )
        self.bind_label_wrap(folder_help_label, horizontal_padding=30, min_wraplength=420)

        grp_opt = ctk.CTkFrame(sf)
        grp_opt.grid(row=2, column=0, sticky="ew", padx=10, pady=10)
        grp_opt.grid_columnconfigure(1, weight=1)
        ctk.CTkLabel(
            grp_opt,
            text=self.tr("settings.section.options"),
            font=ctk.CTkFont(weight="bold"),
        ).grid(row=0, column=0, sticky="w", padx=10, pady=5)

        self.smart_sync_switch = ctk.CTkSwitch(
            grp_opt,
            text=self.tr("settings.label.smart_sync"),
            variable=self.var_smart_sync,
        )
        self.smart_sync_switch.grid(row=1, column=0, columnspan=3, sticky="w", padx=10, pady=10)
        self.auto_upload_switch = ctk.CTkSwitch(
            grp_opt,
            text=self.tr("settings.label.auto_upload"),
            variable=self.var_auto_upload,
        )
        self.auto_upload_switch.grid(row=2, column=0, columnspan=3, sticky="w", padx=10, pady=10)
        self.settings_language_menu = ctk.CTkOptionMenu(
            grp_opt,
            variable=self.var_ui_language,
            values=list(SUPPORTED_UI_LANGUAGES),
        )
        add_option_row(
            grp_opt,
            self.tr("settings.label.ui_language"),
            self.settings_language_menu,
            3,
        )

        def on_range_change(choice):
            selected_range_mode = choice
            range_mode_options = self.get_range_mode_options()
            reverse_options = {label: key for key, label in range_mode_options.items()}
            if choice in reverse_options:
                selected_range_mode = reverse_options[choice]
            if selected_range_mode == 'custom':
                self.frame_custom_range.grid(row=5, column=0, columnspan=3, sticky="ew", padx=10, pady=(0, 10))
                layout_custom_range_fields()
            else:
                self.frame_custom_range.grid_forget()

        self.settings_range_menu = ctk.CTkOptionMenu(
            grp_opt,
            variable=self.var_range,
            values=list(self.get_range_mode_options().values()),
            command=on_range_change,
        )
        add_option_row(
            grp_opt,
            self.tr("settings.label.range_mode"),
            self.settings_range_menu,
            4,
        )

        self.frame_custom_range = ctk.CTkFrame(grp_opt, fg_color="#1F2430")
        self.frame_custom_range.grid_columnconfigure(1, weight=1)
        self.frame_custom_range.grid_columnconfigure(4, weight=1)
        custom_range_title = ctk.CTkLabel(
            self.frame_custom_range,
            text=self.tr("settings.custom_range.title"),
            font=ctk.CTkFont(weight="bold"),
        )
        self.custom_start_label = ctk.CTkLabel(
            self.frame_custom_range,
            text=self.tr("settings.label.custom_start_date"),
            anchor="w",
            justify="left",
        )
        self.custom_start_entry = ctk.CTkEntry(
            self.frame_custom_range,
            textvariable=self.var_custom_date_start,
        )
        self.custom_start_button = ctk.CTkButton(
            self.frame_custom_range,
            text=self.tr("settings.button.calendar"),
            width=70,
            command=lambda: self.open_settings_calendar("start"),
        )
        self.custom_end_label = ctk.CTkLabel(
            self.frame_custom_range,
            text=self.tr("settings.label.custom_end_date"),
            anchor="w",
            justify="left",
        )
        self.custom_end_entry = ctk.CTkEntry(
            self.frame_custom_range,
            textvariable=self.var_custom_date_end,
        )
        self.custom_end_button = ctk.CTkButton(
            self.frame_custom_range,
            text=self.tr("settings.button.calendar"),
            width=70,
            command=lambda: self.open_settings_calendar("end"),
        )
        custom_range_help_label = ctk.CTkLabel(
            self.frame_custom_range,
            text=self.tr("settings.custom_range.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        custom_range_help_label.grid(row=2, column=0, columnspan=6, sticky="ew", padx=10, pady=(0, 4))
        self.bind_label_wrap(custom_range_help_label, horizontal_padding=30, min_wraplength=360)
        custom_range_summary_label = ctk.CTkLabel(
            self.frame_custom_range,
            textvariable=self.var_custom_range_summary,
            text_color="#61AFEF",
            justify="left",
            anchor="w",
        )
        custom_range_summary_label.grid(row=3, column=0, columnspan=6, sticky="ew", padx=10, pady=(0, 8))
        self.bind_label_wrap(custom_range_summary_label, horizontal_padding=30, min_wraplength=360)

        def layout_custom_range_fields() -> None:
            frame_width = self.frame_custom_range.winfo_width()
            if frame_width <= 1:
                frame_width = self.frame_custom_range.winfo_reqwidth()
            custom_range_title.grid_forget()
            self.custom_start_label.grid_forget()
            self.custom_start_entry.grid_forget()
            self.custom_start_button.grid_forget()
            self.custom_end_label.grid_forget()
            self.custom_end_entry.grid_forget()
            self.custom_end_button.grid_forget()
            if frame_width < 1000:
                self.frame_custom_range.grid_columnconfigure(0, weight=1)
                self.frame_custom_range.grid_columnconfigure(1, weight=0)
                custom_range_title.grid(row=0, column=0, columnspan=2, sticky="w", padx=10, pady=(8, 4))
                self.custom_start_label.grid(row=1, column=0, columnspan=2, sticky="w", padx=10, pady=(4, 2))
                self.custom_start_entry.grid(row=2, column=0, sticky="ew", padx=(10, 6), pady=2)
                self.custom_start_button.grid(row=2, column=1, sticky="e", padx=(0, 10), pady=2)
                self.custom_end_label.grid(row=3, column=0, columnspan=2, sticky="w", padx=10, pady=(6, 2))
                self.custom_end_entry.grid(row=4, column=0, sticky="ew", padx=(10, 6), pady=2)
                self.custom_end_button.grid(row=4, column=1, sticky="e", padx=(0, 10), pady=2)
                custom_range_help_label.grid_configure(row=5, column=0, columnspan=2)
                custom_range_summary_label.grid_configure(row=6, column=0, columnspan=2)
                return
            for column_index in range(6):
                self.frame_custom_range.grid_columnconfigure(column_index, weight=0)
            self.frame_custom_range.grid_columnconfigure(1, weight=1)
            self.frame_custom_range.grid_columnconfigure(4, weight=1)
            custom_range_title.grid(row=0, column=0, columnspan=6, sticky="w", padx=10, pady=(8, 4))
            self.custom_start_label.grid(row=1, column=0, sticky="w", padx=(10, 5), pady=6)
            self.custom_start_entry.grid(row=1, column=1, sticky="ew", padx=(0, 6), pady=6)
            self.custom_start_button.grid(row=1, column=2, sticky="e", padx=(0, 12), pady=6)
            self.custom_end_label.grid(row=1, column=3, sticky="w", padx=(0, 5), pady=6)
            self.custom_end_entry.grid(row=1, column=4, sticky="ew", padx=(0, 6), pady=6)
            self.custom_end_button.grid(row=1, column=5, sticky="e", padx=(0, 10), pady=6)
            custom_range_help_label.grid_configure(row=2, column=0, columnspan=6)
            custom_range_summary_label.grid_configure(row=3, column=0, columnspan=6)

        self.frame_custom_range.bind("<Configure>", lambda _event: layout_custom_range_fields())
        layout_custom_range_fields()

        if self.get_selected_range_mode() == 'custom':
            self.frame_custom_range.grid(row=5, column=0, columnspan=3, sticky="ew", padx=10, pady=(0, 10))

        self.lbl_settings_dirty = ctk.CTkLabel(
            self.main_frame,
            textvariable=self.var_settings_dirty,
            text_color="gray",
            justify="left",
            anchor="w",
        )
        self.lbl_settings_dirty.grid(row=1, column=0, sticky="ew", pady=(0, 5))
        self.bind_label_wrap(self.lbl_settings_dirty, horizontal_padding=20, min_wraplength=420)

        self.lbl_settings_validation = ctk.CTkLabel(
            self.main_frame,
            textvariable=self.var_settings_validation,
            text_color="gray",
            justify="left",
            anchor="w",
        )
        self.lbl_settings_validation.grid(row=2, column=0, sticky="ew", pady=(0, 5))
        self.bind_label_wrap(self.lbl_settings_validation, horizontal_padding=20, min_wraplength=420)

        self.btn_save_settings = ctk.CTkButton(
            self.main_frame,
            text=self.tr("settings.button.save"),
            command=self.on_save,
        )
        self.btn_save_settings.grid(row=3, column=0, pady=20)

        self.register_settings_dirty_callbacks()
        self.refresh_settings_form_state()

    def show_logs(self):
        if self.current_view == "logs":
            return
        if not self.confirm_leave_data_tasks(self.tr("sidebar.logs")):
            return
        if not self.confirm_leave_settings(self.tr("navigation.logs")):
            return
        self.current_view = "logs"
        self.clear_main()
        self.log_box = ctk.CTkTextbox(self.main_frame, width=600)
        self.log_box.grid(row=0, column=0, sticky="nsew")
        self.main_frame.grid_rowconfigure(0, weight=1)
        
        # Restore history
        if hasattr(self, 'log_history'):
            self.log_box.insert("1.0", "\n".join(self.log_history) + "\n")
            self.log_box.see("end")

    def show_work_log(self):
        if self.current_view == "work_log":
            return
        if not self.confirm_leave_data_tasks(self.tr("sidebar.work_log")):
            return
        if not self.confirm_leave_settings(self.tr("navigation.work_log")):
            return
        work_log_view_state = self.build_work_log_view_state()
        self.current_view = "work_log"
        self.clear_main()
        
        # Header
        ctk.CTkLabel(
            self.main_frame,
            text=self.tr("work_log.title"),
            font=ctk.CTkFont(size=20, weight="bold"),
        ).pack(pady=20)
        
        # File Selection
        frame_file = ctk.CTkFrame(self.main_frame)
        frame_file.pack(fill="x", padx=20, pady=10)
        
        self.lbl_work_log_file = ctk.CTkLabel(
            frame_file,
            text=self.tr("work_log.label.no_file"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        self.lbl_work_log_file.pack(side="left", padx=10, expand=True, fill="x")
        self.bind_label_wrap(self.lbl_work_log_file, horizontal_padding=40, min_wraplength=260)
        
        ctk.CTkButton(
            frame_file,
            text=self.tr("work_log.button.select_excel"),
            command=self.on_select_work_log,
        ).pack(side="right", padx=10)
        
        # Upload Button
        self.btn_upload_work_log = ctk.CTkButton(
            self.main_frame,
            text=self.tr("work_log.button.start_upload"),
            command=self.on_upload_work_log,
            state="disabled",
            fg_color="#2CC985",
        )
        self.btn_upload_work_log.pack(pady=20)
        
        # Log area
        self.work_log_box = ctk.CTkTextbox(self.main_frame, width=600, height=300)
        self.work_log_box.pack(fill="both", expand=True, padx=20, pady=10)
        self.restore_work_log_view_state(work_log_view_state)
        
    def on_select_work_log(self):
        f = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx *.xls *.xlsm")])
        if f:
            self.selected_work_log_path = f
            self.lbl_work_log_file.configure(text=os.path.basename(f), text_color="white")
            self.btn_upload_work_log.configure(state="normal")
            
    def on_upload_work_log(self):
        if self.selected_work_log_path.strip() == "":
            return
            if not self.ensure_local_supabase_ready(self.tr("work_log.action.upload")):
                return
            
        self.btn_upload_work_log.configure(state="disabled")
        path = self.selected_work_log_path
        
        def _run():
            self.log_to_box(
                self.tr("work_log.log.analysis_started", path=os.path.basename(path))
            )
            try:
                core_work_log = load_core_work_log_module()
                core_upload = load_core_upload_module()
                df = core_work_log.parse_work_log_excel(path)
                self.log_to_box(
                    self.tr("work_log.log.analysis_completed", count=len(df))
                )
                
                url = self.cfg['SUPABASE_URL']
                anon = self.cfg['SUPABASE_ANON_KEY']
                
                self.log_to_box(self.tr("work_log.log.uploading"))
                ok = core_upload.upload_work_log_data(url, anon, df, self.log_to_box)
                
                if ok:
                    self.log_to_box(self.tr("work_log.log.completed"))
                    self.show_info("work_log.dialog.success.title", "work_log.dialog.success.body")
                else:
                    self.log_to_box(self.tr("work_log.log.failed"))
                    self.show_error("work_log.dialog.failure.title", "work_log.dialog.failure.body")
                    
            except Exception as e:
                self.log_to_box(self.tr("work_log.log.error", error=e))
                messagebox.showerror(self.tr("dialog.error.title"), str(e))
            finally:
                self.btn_upload_work_log.configure(state="normal")
                
        threading.Thread(target=_run, daemon=True).start()

    def log_to_box(self, msg):
        self.work_log_messages.append(msg)
        if len(self.work_log_messages) > 400:
            self.work_log_messages = self.work_log_messages[-300:]
        self.schedule_gui_callback(0, lambda: self._append_work_log_msg(msg))
        
    def _append_work_log_msg(self, msg):
        if hasattr(self, 'work_log_box') and self.work_log_box.winfo_exists():
            self.work_log_box.insert("end", msg + "\n")
            self.work_log_box.see("end")

    def layout_responsive_labeled_entry_action_row(
        self,
        row_frame: ctk.CTkFrame,
        label_widget: ctk.CTkBaseClass,
        entry_widget: ctk.CTkBaseClass,
        action_widget: ctk.CTkBaseClass,
    ) -> None:
        width = row_frame.winfo_width()
        if width <= 1:
            width = row_frame.winfo_reqwidth()
        layout_mode = "stack" if width < 640 else "split" if width < 980 else "inline"
        layout_signature = WidgetLayoutSignature(
            layout_name="responsive_labeled_entry_action_row",
            mode=layout_mode,
            child_count=3,
            detail="",
        )
        if self.read_widget_layout_signature(row_frame) == layout_signature:
            return
        for column_index in range(3):
            row_frame.grid_columnconfigure(column_index, weight=0)
        self.forget_widget_geometry(label_widget)
        self.forget_widget_geometry(entry_widget)
        self.forget_widget_geometry(action_widget)
        if width < 640:
            row_frame.grid_columnconfigure(0, weight=1)
            label_widget.grid(row=0, column=0, sticky="w", padx=5, pady=(4, 2))
            entry_widget.grid(row=1, column=0, sticky="ew", padx=5, pady=2)
            action_widget.grid(row=2, column=0, sticky="w", padx=5, pady=(2, 4))
            self.write_widget_layout_signature(row_frame, layout_signature)
            return
        if width < 980:
            row_frame.grid_columnconfigure(0, weight=1)
            row_frame.grid_columnconfigure(1, weight=0)
            label_widget.grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(4, 2))
            entry_widget.grid(row=1, column=0, sticky="ew", padx=5, pady=(2, 4))
            action_widget.grid(row=1, column=1, sticky="e", padx=5, pady=(2, 4))
            self.write_widget_layout_signature(row_frame, layout_signature)
            return
        row_frame.grid_columnconfigure(1, weight=1)
        label_widget.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        entry_widget.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        action_widget.grid(row=0, column=2, sticky="e", padx=5, pady=5)
        self.write_widget_layout_signature(row_frame, layout_signature)

    def layout_responsive_labeled_entry_row(
        self,
        row_frame: ctk.CTkFrame,
        label_widget: ctk.CTkBaseClass,
        entry_widget: ctk.CTkBaseClass,
    ) -> None:
        width = row_frame.winfo_width()
        if width <= 1:
            width = row_frame.winfo_reqwidth()
        layout_mode = "stack" if width < 900 else "inline"
        layout_signature = WidgetLayoutSignature(
            layout_name="responsive_labeled_entry_row",
            mode=layout_mode,
            child_count=2,
            detail="",
        )
        if self.read_widget_layout_signature(row_frame) == layout_signature:
            return
        row_frame.grid_columnconfigure(0, weight=0)
        row_frame.grid_columnconfigure(1, weight=0)
        self.forget_widget_geometry(label_widget)
        self.forget_widget_geometry(entry_widget)
        if width < 900:
            row_frame.grid_columnconfigure(0, weight=1)
            label_widget.grid(row=0, column=0, sticky="w", padx=5, pady=(4, 2))
            entry_widget.grid(row=1, column=0, sticky="ew", padx=5, pady=(2, 4))
            self.write_widget_layout_signature(row_frame, layout_signature)
            return
        row_frame.grid_columnconfigure(1, weight=1)
        label_widget.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        entry_widget.grid(row=0, column=1, sticky="ew", padx=5, pady=5)
        self.write_widget_layout_signature(row_frame, layout_signature)

    def layout_responsive_button_row(
        self,
        row_frame: ctk.CTkFrame,
        button_widgets: list[ctk.CTkButton],
    ) -> None:
        width = row_frame.winfo_width()
        if width <= 1:
            width = row_frame.winfo_reqwidth()
        layout_mode = "stack"
        if width >= 640:
            layout_mode = "two_column" if width < 980 and len(button_widgets) >= 3 else "inline"
        layout_signature = WidgetLayoutSignature(
            layout_name="responsive_button_row",
            mode=layout_mode,
            child_count=len(button_widgets),
            detail="",
        )
        if self.read_widget_layout_signature(row_frame) == layout_signature:
            return
        for column_index in range(max(len(button_widgets), 3)):
            row_frame.grid_columnconfigure(column_index, weight=0)
        for button_widget in button_widgets:
            self.forget_widget_geometry(button_widget)
        if width < 640:
            row_frame.grid_columnconfigure(0, weight=1)
            for row_index, button_widget in enumerate(button_widgets):
                button_widget.grid(row=row_index, column=0, sticky="ew", padx=5, pady=4)
            self.write_widget_layout_signature(row_frame, layout_signature)
            return
        if width < 980 and len(button_widgets) >= 3:
            row_frame.grid_columnconfigure(0, weight=1)
            row_frame.grid_columnconfigure(1, weight=1)
            for button_index, button_widget in enumerate(button_widgets):
                row_index = button_index // 2
                column_index = button_index % 2
                if button_index == len(button_widgets) - 1 and len(button_widgets) % 2 == 1:
                    button_widget.grid(row=row_index, column=0, columnspan=2, sticky="ew", padx=5, pady=4)
                    continue
                button_widget.grid(row=row_index, column=column_index, sticky="ew", padx=5, pady=4)
            self.write_widget_layout_signature(row_frame, layout_signature)
            return
        for column_index in range(len(button_widgets)):
            row_frame.grid_columnconfigure(column_index, weight=1)
        for column_index, button_widget in enumerate(button_widgets):
            button_widget.grid(row=0, column=column_index, sticky="ew", padx=5, pady=4)
        self.write_widget_layout_signature(row_frame, layout_signature)

    def layout_cycle_legacy_custom_date_row(self) -> None:
        if not hasattr(self, "legacy_cycle_custom_date_frame") or not self.legacy_cycle_custom_date_frame.winfo_exists():
            return
        width = self.legacy_cycle_custom_date_frame.winfo_width()
        if width <= 1:
            width = self.legacy_cycle_custom_date_frame.winfo_reqwidth()
        layout_mode = "stack" if width < 720 else "inline"
        layout_signature = WidgetLayoutSignature(
            layout_name="cycle_legacy_custom_date_row",
            mode=layout_mode,
            child_count=3,
            detail="",
        )
        if self.read_widget_layout_signature(self.legacy_cycle_custom_date_frame) == layout_signature:
            return
        self.legacy_cycle_custom_date_frame.grid_columnconfigure(0, weight=0)
        self.legacy_cycle_custom_date_frame.grid_columnconfigure(1, weight=0)
        self.legacy_cycle_custom_date_frame.grid_columnconfigure(2, weight=0)
        self.forget_widget_geometry(self.legacy_cycle_custom_date_label)
        self.forget_widget_geometry(self.legacy_cycle_custom_date_entry)
        self.forget_widget_geometry(self.legacy_cycle_custom_date_hint)
        if width < 720:
            self.legacy_cycle_custom_date_frame.grid_columnconfigure(0, weight=1)
            self.legacy_cycle_custom_date_label.grid(row=0, column=0, sticky="w", padx=0, pady=(0, 2))
            self.legacy_cycle_custom_date_entry.grid(row=1, column=0, sticky="ew", padx=0, pady=2)
            self.legacy_cycle_custom_date_hint.grid(row=2, column=0, sticky="w", padx=0, pady=(2, 0))
            self.write_widget_layout_signature(
                self.legacy_cycle_custom_date_frame,
                layout_signature,
            )
            return
        self.legacy_cycle_custom_date_label.grid(row=0, column=0, sticky="w", padx=(0, 5), pady=0)
        self.legacy_cycle_custom_date_entry.grid(row=0, column=1, sticky="w", padx=5, pady=0)
        self.legacy_cycle_custom_date_hint.grid(row=0, column=2, sticky="w", padx=5, pady=0)
        self.write_widget_layout_signature(
            self.legacy_cycle_custom_date_frame,
            layout_signature,
        )

    def layout_cycle_legacy_range_row(self) -> None:
        if not hasattr(self, "legacy_range_row") or not self.legacy_range_row.winfo_exists():
            return
        width = self.legacy_range_row.winfo_width()
        if width <= 1:
            width = self.legacy_range_row.winfo_reqwidth()
        selected_mode = self.get_selected_legacy_cycle_mode()
        layout_mode = "stack" if width < 640 else "split" if width < 980 else "inline"
        layout_signature = WidgetLayoutSignature(
            layout_name="cycle_legacy_range_row",
            mode=layout_mode,
            child_count=3,
            detail=selected_mode,
        )
        if self.read_widget_layout_signature(self.legacy_range_row) == layout_signature:
            return
        self.legacy_range_row.grid_columnconfigure(0, weight=0)
        self.legacy_range_row.grid_columnconfigure(1, weight=0)
        self.legacy_range_row.grid_columnconfigure(2, weight=0)
        self.forget_widget_geometry(self.legacy_range_label)
        self.forget_widget_geometry(self.legacy_cycle_range_menu)
        self.forget_widget_geometry(self.legacy_cycle_custom_date_frame)
        if width < 640:
            self.legacy_range_row.grid_columnconfigure(0, weight=1)
            self.legacy_range_label.grid(row=0, column=0, sticky="w", padx=5, pady=(4, 2))
            self.legacy_cycle_range_menu.grid(row=1, column=0, sticky="ew", padx=5, pady=2)
            if selected_mode == "custom":
                self.legacy_cycle_custom_date_frame.grid(row=2, column=0, sticky="ew", padx=5, pady=(2, 4))
            self.layout_cycle_legacy_custom_date_row()
            self.write_widget_layout_signature(self.legacy_range_row, layout_signature)
            return
        if width < 980:
            self.legacy_range_row.grid_columnconfigure(0, weight=1)
            self.legacy_range_row.grid_columnconfigure(1, weight=0)
            self.legacy_range_label.grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(4, 2))
            self.legacy_cycle_range_menu.grid(row=1, column=0, sticky="w", padx=5, pady=(2, 4))
            if selected_mode == "custom":
                self.legacy_cycle_custom_date_frame.grid(row=1, column=1, sticky="ew", padx=5, pady=(2, 4))
            self.layout_cycle_legacy_custom_date_row()
            self.write_widget_layout_signature(self.legacy_range_row, layout_signature)
            return
        self.legacy_range_row.grid_columnconfigure(2, weight=1)
        self.legacy_range_label.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.legacy_cycle_range_menu.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        if selected_mode == "custom":
            self.legacy_cycle_custom_date_frame.grid(row=0, column=2, sticky="ew", padx=5, pady=5)
        self.layout_cycle_legacy_custom_date_row()
        self.write_widget_layout_signature(self.legacy_range_row, layout_signature)

    def layout_training_mode_row(self) -> None:
        if not hasattr(self, "training_mode_row") or not self.training_mode_row.winfo_exists():
            return
        width = self.training_mode_row.winfo_width()
        if width <= 1:
            width = self.training_mode_row.winfo_reqwidth()
        layout_mode = "stack" if width < 640 else "split" if width < 980 else "inline"
        layout_signature = WidgetLayoutSignature(
            layout_name="training_mode_row",
            mode=layout_mode,
            child_count=3,
            detail="",
        )
        if self.read_widget_layout_signature(self.training_mode_row) == layout_signature:
            return
        self.training_mode_row.grid_columnconfigure(0, weight=0)
        self.training_mode_row.grid_columnconfigure(1, weight=0)
        self.training_mode_row.grid_columnconfigure(2, weight=0)
        self.forget_widget_geometry(self.training_mode_label)
        self.forget_widget_geometry(self.training_mode_menu)
        self.forget_widget_geometry(self.training_mode_status_label)
        if width < 640:
            self.training_mode_row.grid_columnconfigure(0, weight=1)
            self.training_mode_label.grid(row=0, column=0, sticky="w", padx=5, pady=(4, 2))
            self.training_mode_menu.grid(row=1, column=0, sticky="ew", padx=5, pady=2)
            self.training_mode_status_label.grid(row=2, column=0, sticky="w", padx=5, pady=(2, 4))
            self.write_widget_layout_signature(self.training_mode_row, layout_signature)
            return
        if width < 980:
            self.training_mode_row.grid_columnconfigure(0, weight=1)
            self.training_mode_row.grid_columnconfigure(1, weight=0)
            self.training_mode_label.grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(4, 2))
            self.training_mode_menu.grid(row=1, column=0, sticky="w", padx=5, pady=(2, 4))
            self.training_mode_status_label.grid(row=1, column=1, sticky="e", padx=5, pady=(2, 4))
            self.write_widget_layout_signature(self.training_mode_row, layout_signature)
            return
        self.training_mode_row.grid_columnconfigure(2, weight=1)
        self.training_mode_label.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.training_mode_menu.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        self.training_mode_status_label.grid(row=0, column=2, sticky="w", padx=12, pady=5)
        self.write_widget_layout_signature(self.training_mode_row, layout_signature)

    def layout_archive_date_row(self) -> None:
        if not hasattr(self, "archive_date_row") or not self.archive_date_row.winfo_exists():
            return
        width = self.archive_date_row.winfo_width()
        if width <= 1:
            width = self.archive_date_row.winfo_reqwidth()
        layout_mode = "stack" if width < 640 else "split" if width < 980 else "inline"
        layout_signature = WidgetLayoutSignature(
            layout_name="archive_date_row",
            mode=layout_mode,
            child_count=4,
            detail="",
        )
        if self.read_widget_layout_signature(self.archive_date_row) == layout_signature:
            return
        for column_index in range(4):
            self.archive_date_row.grid_columnconfigure(column_index, weight=0)
        self.forget_widget_geometry(self.archive_date_label)
        self.forget_widget_geometry(self.archive_date_entry)
        self.forget_widget_geometry(self.archive_date_hint)
        self.forget_widget_geometry(self.archive_status_label)
        if width < 640:
            self.archive_date_row.grid_columnconfigure(0, weight=1)
            self.archive_date_label.grid(row=0, column=0, sticky="w", padx=5, pady=(4, 2))
            self.archive_date_entry.grid(row=1, column=0, sticky="ew", padx=5, pady=2)
            self.archive_date_hint.grid(row=2, column=0, sticky="w", padx=5, pady=(2, 0))
            self.archive_status_label.grid(row=3, column=0, sticky="w", padx=5, pady=(2, 4))
            self.write_widget_layout_signature(self.archive_date_row, layout_signature)
            return
        if width < 980:
            self.archive_date_row.grid_columnconfigure(0, weight=1)
            self.archive_date_row.grid_columnconfigure(1, weight=0)
            self.archive_date_label.grid(row=0, column=0, columnspan=2, sticky="w", padx=5, pady=(4, 2))
            self.archive_date_entry.grid(row=1, column=0, sticky="ew", padx=5, pady=2)
            self.archive_date_hint.grid(row=1, column=1, sticky="w", padx=5, pady=2)
            self.archive_status_label.grid(row=2, column=0, columnspan=2, sticky="w", padx=5, pady=(2, 4))
            self.write_widget_layout_signature(self.archive_date_row, layout_signature)
            return
        self.archive_date_row.grid_columnconfigure(3, weight=1)
        self.archive_date_label.grid(row=0, column=0, sticky="w", padx=5, pady=5)
        self.archive_date_entry.grid(row=0, column=1, sticky="w", padx=5, pady=5)
        self.archive_date_hint.grid(row=0, column=2, sticky="w", padx=5, pady=5)
        self.archive_status_label.grid(row=0, column=3, sticky="w", padx=12, pady=5)
        self.write_widget_layout_signature(self.archive_date_row, layout_signature)

    def show_cycle_ops(self):
        if self.current_view == "cycle_ops":
            return
        if not self.confirm_leave_data_tasks(self.tr("sidebar.cycle_ops")):
            return
        if not self.confirm_leave_settings(self.tr("navigation.cycle_ops")):
            return
        self.current_view = "cycle_ops"
        self.clear_main()

        scroll_frame = ctk.CTkScrollableFrame(
            self.main_frame,
            label_text=self.tr("cycle_ops.title"),
        )
        scroll_frame.pack(fill="both", expand=True, padx=20, pady=20)

        canonical_frame = ctk.CTkFrame(scroll_frame)
        canonical_frame.pack(fill="x", padx=10, pady=10)

        ctk.CTkLabel(
            canonical_frame,
            text=self.tr("cycle_ops.canonical.title"),
            font=ctk.CTkFont(weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        canonical_help_label = ctk.CTkLabel(
            canonical_frame,
            text=self.tr("cycle_ops.canonical.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        canonical_help_label.pack(fill="x", anchor="w", padx=10)
        self.bind_label_wrap(canonical_help_label, horizontal_padding=30, min_wraplength=420)
        canonical_status_label = ctk.CTkLabel(
            canonical_frame,
            textvariable=self.var_cycle_ops_status,
            text_color="gray",
            justify="left",
            anchor="w",
        )
        canonical_status_label.pack(fill="x", anchor="w", padx=10, pady=(6, 10))
        self.bind_label_wrap(canonical_status_label, horizontal_padding=30, min_wraplength=420)
        self.var_legacy_cycle_range.set(
            self.get_legacy_cycle_mode_label(self.get_selected_legacy_cycle_mode())
        )

        canonical_button_row = ctk.CTkFrame(canonical_frame, fg_color="transparent")
        canonical_button_row.pack(fill="x", padx=10, pady=(0, 10))
        self.btn_run_canonical_refresh = ctk.CTkButton(
            canonical_button_row,
            text=self.tr("cycle_ops.canonical.button.refresh"),
            command=self.on_run_canonical_refresh,
            fg_color="#2CC985",
            width=0,
        )
        self.btn_run_cycle_snapshot = ctk.CTkButton(
            canonical_button_row,
            text=self.tr("cycle_ops.canonical.button.snapshot_sync"),
            command=self.on_run_cycle_snapshot_sync,
            width=0,
        )
        self.btn_run_cycle_health = ctk.CTkButton(
            canonical_button_row,
            text=self.tr("cycle_ops.canonical.button.health"),
            command=self.on_run_cycle_health_check,
            width=0,
        )
        canonical_button_row.bind(
            "<Configure>",
            lambda _event=None: self.layout_responsive_button_row(
                canonical_button_row,
                [
                    self.btn_run_canonical_refresh,
                    self.btn_run_cycle_snapshot,
                    self.btn_run_cycle_health,
                ],
            ),
        )
        self.layout_responsive_button_row(
            canonical_button_row,
            [
                self.btn_run_canonical_refresh,
                self.btn_run_cycle_snapshot,
                self.btn_run_cycle_health,
            ],
        )

        legacy_frame = ctk.CTkFrame(scroll_frame)
        legacy_frame.pack(fill="x", padx=10, pady=(0, 10))
        ctk.CTkLabel(
            legacy_frame,
            text=self.tr("cycle_ops.legacy.title"),
            font=ctk.CTkFont(weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        legacy_help_label = ctk.CTkLabel(
            legacy_frame,
            text=self.tr("cycle_ops.legacy.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        legacy_help_label.pack(fill="x", anchor="w", padx=10)
        self.bind_label_wrap(legacy_help_label, horizontal_padding=30, min_wraplength=420)

        legacy_machine_row = ctk.CTkFrame(legacy_frame, fg_color="transparent")
        legacy_machine_row.pack(fill="x", padx=10, pady=(10, 5))
        legacy_machine_label = ctk.CTkLabel(
            legacy_machine_row,
            text=self.tr("cycle_ops.legacy.label.machine_id"),
            width=180,
            anchor="w",
        )
        legacy_machine_entry = ctk.CTkEntry(legacy_machine_row, textvariable=self.var_legacy_cycle_machine_id)
        legacy_machine_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_row(
                legacy_machine_row,
                legacy_machine_label,
                legacy_machine_entry,
            ),
        )
        self.layout_responsive_labeled_entry_row(
            legacy_machine_row,
            legacy_machine_label,
            legacy_machine_entry,
        )

        self.legacy_range_row = ctk.CTkFrame(legacy_frame, fg_color="transparent")
        self.legacy_range_row.pack(fill="x", padx=10, pady=5)
        self.legacy_range_label = ctk.CTkLabel(
            self.legacy_range_row,
            text=self.tr("cycle_ops.legacy.label.mode"),
            width=180,
            anchor="w",
        )
        self.legacy_cycle_range_menu = ctk.CTkOptionMenu(
            self.legacy_range_row,
            variable=self.var_legacy_cycle_range,
            values=list(self.get_legacy_cycle_mode_options().values()),
            command=self.on_legacy_cycle_range_change,
        )
        self.legacy_cycle_custom_date_frame = ctk.CTkFrame(self.legacy_range_row, fg_color="transparent")
        self.legacy_cycle_custom_date_label = ctk.CTkLabel(
            self.legacy_cycle_custom_date_frame,
            text=self.tr("settings.label.custom_start_date"),
        )
        self.legacy_cycle_custom_date_entry = ctk.CTkEntry(
            self.legacy_cycle_custom_date_frame,
            textvariable=self.var_legacy_cycle_custom_date,
            width=120,
        )
        self.legacy_cycle_custom_date_hint = ctk.CTkLabel(
            self.legacy_cycle_custom_date_frame,
            text=self.tr("common.date.iso_format"),
            text_color="gray",
        )
        self.legacy_cycle_custom_date_frame.bind(
            "<Configure>",
            lambda _event: self.layout_cycle_legacy_custom_date_row(),
        )
        self.legacy_range_row.bind(
            "<Configure>",
            lambda _event: self.layout_cycle_legacy_range_row(),
        )
        self.layout_cycle_legacy_range_row()

        legacy_status_label = ctk.CTkLabel(
            legacy_frame,
            textvariable=self.var_legacy_cycle_status,
            text_color="gray",
            justify="left",
            anchor="w",
        )
        legacy_status_label.pack(fill="x", anchor="w", padx=10, pady=(5, 10))
        self.bind_label_wrap(legacy_status_label, horizontal_padding=30, min_wraplength=420)
        self.btn_run_legacy_cycle = ctk.CTkButton(
            legacy_frame,
            text=self.tr("cycle_ops.legacy.button.backfill"),
            command=self.on_run_legacy_cycle,
            fg_color="#E5C07B",
            text_color="black",
            width=180,
        )
        self.btn_run_legacy_cycle.pack(anchor="w", padx=10, pady=(0, 10))

        outputs_frame = ctk.CTkFrame(scroll_frame)
        outputs_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        progress_frame = ctk.CTkFrame(outputs_frame, fg_color="transparent")
        progress_frame.pack(fill="x", padx=10, pady=(10, 0))
        self.data_progress_bar = ctk.CTkProgressBar(progress_frame, width=400)
        self.data_progress_bar.pack(fill="x", pady=(0, 8))
        self.data_progress_bar.set(0)
        self.data_progress_label = ctk.CTkLabel(
            progress_frame,
            text=self.format_percent_text(0.0),
        )
        self.data_progress_label.pack(anchor="w")
        self.data_log_box = ctk.CTkTextbox(outputs_frame, width=600, height=300)
        self.data_log_box.pack(fill="both", expand=True, padx=10, pady=10)

    # --- Data Management View ---
    def show_data_mgmt(self):
        if self.current_view == "data_mgmt":
            return
        if not self.confirm_leave_data_tasks(self.tr("sidebar.data_mgmt")):
            return
        if not self.confirm_leave_settings(self.tr("navigation.data_mgmt")):
            return
        self.current_view = "data_mgmt"
        self.clear_main()

        scroll_frame = ctk.CTkScrollableFrame(
            self.main_frame,
            label_text=self.tr("data_mgmt.title"),
        )
        scroll_frame.pack(fill="both", expand=True, padx=20, pady=20)

        frame_actions = ctk.CTkFrame(scroll_frame)
        frame_actions.pack(fill="x", padx=10, pady=10)

        data_mgmt_help_label = ctk.CTkLabel(
            frame_actions,
            text=self.tr("data_mgmt.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        data_mgmt_help_label.pack(fill="x", anchor="w", padx=10, pady=(10, 0))
        self.bind_label_wrap(data_mgmt_help_label, horizontal_padding=30, min_wraplength=420)

        training_frame = ctk.CTkFrame(frame_actions)
        training_frame.pack(fill="x", padx=10, pady=(10, 10))

        ctk.CTkLabel(
            training_frame,
            text=self.tr("data_mgmt.training.title"),
            font=ctk.CTkFont(weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        training_help_label = ctk.CTkLabel(
            training_frame,
            text=self.tr("data_mgmt.training.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        training_help_label.pack(fill="x", anchor="w", padx=10)
        self.bind_label_wrap(training_help_label, horizontal_padding=30, min_wraplength=420)
        self.var_training_mode.set(
            self.get_training_mode_label(self.get_selected_training_mode())
        )

        self.training_mode_row = ctk.CTkFrame(training_frame, fg_color="transparent")
        self.training_mode_row.pack(fill="x", padx=10, pady=(10, 5))
        self.training_mode_label = ctk.CTkLabel(self.training_mode_row, text=self.tr("data_mgmt.training.label.mode"))
        self.training_mode_menu = ctk.CTkOptionMenu(
            self.training_mode_row,
            variable=self.var_training_mode,
            values=list(self.get_training_mode_options().values()),
            command=self.on_training_mode_change,
        )
        self.training_mode_status_label = ctk.CTkLabel(
            self.training_mode_row,
            textvariable=self.var_training_status,
            text_color="gray",
        )
        self.training_mode_row.bind("<Configure>", lambda _event=None: self.layout_training_mode_row())
        self.layout_training_mode_row()

        training_fields_frame = ctk.CTkFrame(training_frame, fg_color="transparent")
        training_fields_frame.pack(fill="x", padx=10, pady=(0, 0))
        training_fields_frame.grid_columnconfigure(0, weight=1)

        self.training_plc_row = ctk.CTkFrame(training_fields_frame, fg_color="transparent")
        self.training_plc_row.grid(row=0, column=0, sticky="ew", pady=5)
        self.training_plc_label = ctk.CTkLabel(
            self.training_plc_row,
            text=self.tr("data_mgmt.training.label.raw_csv"),
            width=210,
            anchor="w",
        )
        self.training_plc_entry = ctk.CTkEntry(self.training_plc_row, textvariable=self.var_training_plc_file)
        self.training_plc_button = ctk.CTkButton(
            self.training_plc_row,
            text=self.tr("common.button.select"),
            width=90,
            command=self.pick_training_plc_file,
        )
        self.training_plc_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_action_row(
                self.training_plc_row,
                self.training_plc_label,
                self.training_plc_entry,
                self.training_plc_button,
            ),
        )
        self.layout_responsive_labeled_entry_action_row(
            self.training_plc_row,
            self.training_plc_label,
            self.training_plc_entry,
            self.training_plc_button,
        )

        self.training_spot_row = ctk.CTkFrame(training_fields_frame, fg_color="transparent")
        self.training_spot_row.grid(row=1, column=0, sticky="ew", pady=5)
        self.training_spot_label = ctk.CTkLabel(
            self.training_spot_row,
            text=self.tr("data_mgmt.training.label.spot_csv"),
            width=210,
            anchor="w",
        )
        self.training_spot_entry = ctk.CTkEntry(self.training_spot_row, textvariable=self.var_training_spot_file)
        self.training_spot_button = ctk.CTkButton(
            self.training_spot_row,
            text=self.tr("common.button.select"),
            width=90,
            command=self.pick_training_spot_file,
        )
        self.training_spot_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_action_row(
                self.training_spot_row,
                self.training_spot_label,
                self.training_spot_entry,
                self.training_spot_button,
            ),
        )
        self.layout_responsive_labeled_entry_action_row(
            self.training_spot_row,
            self.training_spot_label,
            self.training_spot_entry,
            self.training_spot_button,
        )

        self.training_base_input_row = ctk.CTkFrame(training_fields_frame, fg_color="transparent")
        self.training_base_input_row.grid(row=2, column=0, sticky="ew", pady=5)
        self.training_base_input_label = ctk.CTkLabel(
            self.training_base_input_row,
            text=self.tr("data_mgmt.training.label.base_input"),
            width=210,
            anchor="w",
        )
        self.training_base_input_entry = ctk.CTkEntry(self.training_base_input_row, textvariable=self.var_training_base_file)
        self.training_base_input_button = ctk.CTkButton(
            self.training_base_input_row,
            text=self.tr("common.button.select"),
            width=90,
            command=self.pick_training_base_file,
        )
        self.training_base_input_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_action_row(
                self.training_base_input_row,
                self.training_base_input_label,
                self.training_base_input_entry,
                self.training_base_input_button,
            ),
        )
        self.layout_responsive_labeled_entry_action_row(
            self.training_base_input_row,
            self.training_base_input_label,
            self.training_base_input_entry,
            self.training_base_input_button,
        )

        self.training_base_output_row = ctk.CTkFrame(training_fields_frame, fg_color="transparent")
        self.training_base_output_row.grid(row=3, column=0, sticky="ew", pady=5)
        self.training_base_output_label = ctk.CTkLabel(
            self.training_base_output_row,
            text=self.tr("data_mgmt.training.label.base_output"),
            width=210,
            anchor="w",
        )
        self.training_base_output_entry = ctk.CTkEntry(self.training_base_output_row, textvariable=self.var_training_base_output)
        self.training_base_output_button = ctk.CTkButton(
            self.training_base_output_row,
            text=self.tr("common.button.save"),
            width=90,
            command=self.pick_training_base_output,
        )
        self.training_base_output_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_action_row(
                self.training_base_output_row,
                self.training_base_output_label,
                self.training_base_output_entry,
                self.training_base_output_button,
            ),
        )
        self.layout_responsive_labeled_entry_action_row(
            self.training_base_output_row,
            self.training_base_output_label,
            self.training_base_output_entry,
            self.training_base_output_button,
        )

        self.training_dataset_output_row = ctk.CTkFrame(training_fields_frame, fg_color="transparent")
        self.training_dataset_output_row.grid(row=4, column=0, sticky="ew", pady=5)
        self.training_dataset_output_label = ctk.CTkLabel(
            self.training_dataset_output_row,
            text=self.tr("data_mgmt.training.label.dataset_output"),
            width=210,
            anchor="w",
        )
        self.training_dataset_output_entry = ctk.CTkEntry(self.training_dataset_output_row, textvariable=self.var_training_dataset_output)
        self.training_dataset_output_button = ctk.CTkButton(
            self.training_dataset_output_row,
            text=self.tr("common.button.save"),
            width=90,
            command=self.pick_training_dataset_output,
        )
        self.training_dataset_output_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_action_row(
                self.training_dataset_output_row,
                self.training_dataset_output_label,
                self.training_dataset_output_entry,
                self.training_dataset_output_button,
            ),
        )
        self.layout_responsive_labeled_entry_action_row(
            self.training_dataset_output_row,
            self.training_dataset_output_label,
            self.training_dataset_output_entry,
            self.training_dataset_output_button,
        )

        self.training_filename_row = ctk.CTkFrame(training_fields_frame, fg_color="transparent")
        self.training_filename_row.grid(row=5, column=0, sticky="ew", pady=5)
        self.training_filename_label = ctk.CTkLabel(
            self.training_filename_row,
            text=self.tr("data_mgmt.training.label.filename_hint"),
            width=210,
            anchor="w",
        )
        self.training_filename_entry = ctk.CTkEntry(self.training_filename_row, textvariable=self.var_training_filename_hint)
        self.training_filename_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_row(
                self.training_filename_row,
                self.training_filename_label,
                self.training_filename_entry,
            ),
        )
        self.layout_responsive_labeled_entry_row(
            self.training_filename_row,
            self.training_filename_label,
            self.training_filename_entry,
        )

        self.btn_run_training_build = ctk.CTkButton(
            training_frame,
            text=self.tr("data_mgmt.training.button.run"),
            command=self.on_run_training_build,
            fg_color="#2CC985",
        )
        self.btn_run_training_build.pack(pady=10)
        self.on_training_mode_change(self.var_training_mode.get())

        archive_frame = ctk.CTkFrame(frame_actions)
        archive_frame.pack(fill="x", padx=10, pady=(10, 10))

        ctk.CTkLabel(
            archive_frame,
            text=self.tr("data_mgmt.archive.title"),
            font=ctk.CTkFont(weight="bold"),
        ).pack(anchor="w", padx=10, pady=(10, 5))
        archive_help_label = ctk.CTkLabel(
            archive_frame,
            text=self.tr("data_mgmt.archive.help"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        archive_help_label.pack(fill="x", anchor="w", padx=10)
        self.bind_label_wrap(archive_help_label, horizontal_padding=30, min_wraplength=420)

        self.archive_date_row = ctk.CTkFrame(archive_frame, fg_color="transparent")
        self.archive_date_row.pack(fill="x", padx=10, pady=(10, 5))
        self.archive_date_label = ctk.CTkLabel(
            self.archive_date_row,
            text=self.tr("data_mgmt.archive.label.before_date"),
            width=180,
            anchor="w",
        )
        self.archive_date_entry = ctk.CTkEntry(
            self.archive_date_row,
            textvariable=self.var_archive_before_date,
            width=140,
        )
        self.archive_date_hint = ctk.CTkLabel(
            self.archive_date_row,
            text=self.tr("common.date.iso_format"),
            text_color="gray",
        )
        self.archive_status_label = ctk.CTkLabel(
            self.archive_date_row,
            textvariable=self.var_archive_status,
            text_color="gray",
        )
        self.archive_date_row.bind("<Configure>", lambda _event=None: self.layout_archive_date_row())
        self.layout_archive_date_row()

        archive_dir_row = ctk.CTkFrame(archive_frame, fg_color="transparent")
        archive_dir_row.pack(fill="x", padx=10, pady=5)
        archive_dir_label = ctk.CTkLabel(
            archive_dir_row,
            text=self.tr("data_mgmt.archive.label.archive_dir"),
            width=180,
            anchor="w",
        )
        archive_dir_entry = ctk.CTkEntry(archive_dir_row, textvariable=self.var_archive_dir)
        archive_dir_button = ctk.CTkButton(
            archive_dir_row,
            text=self.tr("common.button.select"),
            width=90,
            command=self.pick_archive_dir,
        )
        archive_dir_row.bind(
            "<Configure>",
            lambda _event: self.layout_responsive_labeled_entry_action_row(
                archive_dir_row,
                archive_dir_label,
                archive_dir_entry,
                archive_dir_button,
            ),
        )
        self.layout_responsive_labeled_entry_action_row(
            archive_dir_row,
            archive_dir_label,
            archive_dir_entry,
            archive_dir_button,
        )

        archive_dir_help_label = ctk.CTkLabel(
            archive_frame,
            text=self.tr("data_mgmt.archive.help.archive_dir"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        archive_dir_help_label.pack(fill="x", anchor="w", padx=10, pady=(0, 5))
        self.bind_label_wrap(archive_dir_help_label, horizontal_padding=30, min_wraplength=420)
        archive_backup_help_label = ctk.CTkLabel(
            archive_frame,
            text=self.tr("data_mgmt.archive.help.backup_dir"),
            text_color="gray",
            justify="left",
            anchor="w",
        )
        archive_backup_help_label.pack(fill="x", anchor="w", padx=10, pady=(0, 5))
        self.bind_label_wrap(archive_backup_help_label, horizontal_padding=30, min_wraplength=420)

        self.archive_delete_switch = ctk.CTkSwitch(
            archive_frame,
            text=self.tr("data_mgmt.archive.label.delete_after_export"),
            variable=self.var_archive_delete,
        )
        self.archive_delete_switch.pack(anchor="w", padx=10, pady=(0, 10))

        archive_button_row = ctk.CTkFrame(archive_frame, fg_color="transparent")
        archive_button_row.pack(fill="x", padx=10, pady=(0, 10))
        self.btn_archive_preview = ctk.CTkButton(
            archive_button_row,
            text=self.tr("data_mgmt.archive.button.preview"),
            command=self.on_run_archive_preview,
        )
        self.btn_archive_export = ctk.CTkButton(
            archive_button_row,
            text=self.tr("data_mgmt.archive.button.run"),
            command=self.on_run_archive_export,
            fg_color="#2CC985",
        )
        archive_button_row.bind(
            "<Configure>",
            lambda _event=None: self.layout_responsive_button_row(
                archive_button_row,
                [
                    self.btn_archive_preview,
                    self.btn_archive_export,
                ],
            ),
        )
        self.layout_responsive_button_row(
            archive_button_row,
            [
                self.btn_archive_preview,
                self.btn_archive_export,
            ],
        )
        
        outputs_frame = ctk.CTkFrame(scroll_frame)
        outputs_frame.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        progress_frame = ctk.CTkFrame(outputs_frame, fg_color="transparent")
        progress_frame.pack(fill="x", padx=10, pady=(10, 0))
        self.data_progress_bar = ctk.CTkProgressBar(progress_frame, width=400)
        self.data_progress_bar.pack(fill="x", pady=(0, 8))
        self.data_progress_bar.set(0)
        
        self.data_progress_label = ctk.CTkLabel(progress_frame, text=self.format_percent_text(0.0))
        self.data_progress_label.pack(anchor="w")
        
        self.data_log_box = ctk.CTkTextbox(outputs_frame, width=600, height=300)
        self.data_log_box.pack(fill="both", expand=True, padx=10, pady=10)
        
    def on_run_canonical_refresh(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        if not self.ensure_local_supabase_ready(self.tr("cycle_ops.canonical.action.refresh")):
            return
        if not self.can_start_data_task():
            return
        self.disable_data_mgmt_buttons()
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text=self.format_percent_text(0.0))
        self.var_cycle_ops_status.set(self.tr("cycle_ops.canonical.status.running"))
        self.log_to_data_box(self.tr("cycle_ops.canonical.log.started"))

        def _run():
            try:
                result = execute_canonical_refresh(
                    PROJECT_ROOT,
                    self.log_to_data_box,
                    self.update_data_progress,
                )
                self.schedule_gui_callback(
                    0,
                    lambda: self.var_cycle_ops_status.set(
                        self.tr("cycle_ops.canonical.status.completed", cycle_count=result.stats.cycle_count)
                    ),
                )
                self.schedule_gui_callback(
                    0,
                    lambda: self.show_info("dialog.completed.title", "cycle_ops.canonical.dialog.completed"),
                )
            except Exception as error:
                self.log_to_data_box(self.tr("common.log.error", error=error))
                self.schedule_gui_callback(0, lambda: self.var_cycle_ops_status.set(self.tr("cycle_ops.canonical.status.failed")))
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dialog.error.title"), str(error)))
            finally:
                self.schedule_gui_callback(0, self.enable_data_mgmt_buttons)

        threading.Thread(target=_run, daemon=True).start()

    def on_run_cycle_snapshot_sync(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        if not self.ensure_local_supabase_ready(self.tr("cycle_ops.canonical.action.snapshot_sync")):
            return
        if not self.can_start_data_task():
            return
        self.disable_data_mgmt_buttons()
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text=self.format_percent_text(0.0))
        self.var_cycle_ops_status.set(self.tr("cycle_ops.snapshot.status.running"))
        self.log_to_data_box(self.tr("cycle_ops.snapshot.log.started"))

        def _run():
            try:
                result = execute_cycle_snapshot_sync(
                    PROJECT_ROOT,
                    self.log_to_data_box,
                    self.update_data_progress,
                )
                self.schedule_gui_callback(
                    0,
                    lambda: self.var_cycle_ops_status.set(
                        self.tr("cycle_ops.snapshot.status.completed", row_count=result.affected_row_count)
                    ),
                )
                self.schedule_gui_callback(
                    0,
                    lambda: self.show_info("dialog.completed.title", "cycle_ops.snapshot.dialog.completed"),
                )
            except Exception as error:
                self.log_to_data_box(self.tr("common.log.error", error=error))
                self.schedule_gui_callback(0, lambda: self.var_cycle_ops_status.set(self.tr("cycle_ops.snapshot.status.failed")))
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dialog.error.title"), str(error)))
            finally:
                self.schedule_gui_callback(0, self.enable_data_mgmt_buttons)

        threading.Thread(target=_run, daemon=True).start()

    def on_run_cycle_health_check(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        if not self.ensure_local_supabase_ready(self.tr("cycle_ops.health.action.run")):
            return
        if not self.can_start_data_task():
            return
        self.disable_data_mgmt_buttons()
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text=self.format_percent_text(0.0))
        self.var_cycle_ops_status.set(self.tr("cycle_ops.health.status.running"))
        self.log_to_data_box(self.tr("cycle_ops.health.log.started"))

        def _run():
            try:
                report = execute_cycle_health_check(PROJECT_ROOT)
                for line in format_cycle_health_report(report, self.tr_map):
                    self.log_to_data_box(line)
                self.update_data_progress(1.0)
                self.schedule_gui_callback(0, lambda: self.var_cycle_ops_status.set(self.tr("cycle_ops.health.status.completed")))
            except Exception as error:
                self.log_to_data_box(self.tr("common.log.error", error=error))
                self.schedule_gui_callback(0, lambda: self.var_cycle_ops_status.set(self.tr("cycle_ops.health.status.failed")))
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dialog.error.title"), str(error)))
            finally:
                self.schedule_gui_callback(0, self.enable_data_mgmt_buttons)

        threading.Thread(target=_run, daemon=True).start()

    def on_legacy_cycle_range_change(self, choice):
        selected_mode = choice
        legacy_mode_options = self.get_legacy_cycle_mode_options()
        reverse_options = {label: key for key, label in legacy_mode_options.items()}
        if choice in reverse_options:
            selected_mode = reverse_options[choice]
        self.layout_cycle_legacy_range_row()

    def on_run_legacy_cycle(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        if not self.ensure_local_supabase_ready(self.tr("cycle_ops.legacy.action.run")):
            return
        if not self.can_start_data_task():
            return
        try:
            request = normalize_legacy_cycle_request(
                self.var_legacy_cycle_machine_id.get(),
                self.get_selected_legacy_cycle_mode(),
                self.var_legacy_cycle_custom_date.get(),
                self.tr_map,
            )
        except ValueError as error:
            messagebox.showerror(self.tr("dialog.error.title"), str(error))
            return

        self.disable_data_mgmt_buttons()
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text=self.format_percent_text(0.0))
        mode_label = self.get_legacy_cycle_mode_label(request.mode)
        self.var_legacy_cycle_status.set(self.tr("cycle_ops.legacy.status.running", mode=mode_label))
        self.log_to_data_box(
            self.tr(
                "cycle_ops.legacy.log.started",
                machine_id=request.machine_id,
                mode=mode_label,
            )
        )

        def _run():
            try:
                core_cycle = load_core_cycle_module()
                db_settings = resolve_cycle_db_connection_settings(PROJECT_ROOT)
                processor = core_cycle.build_legacy_cycle_processor(
                    db_settings,
                    request.machine_id,
                    self.log_to_data_box,
                    self.update_data_progress,
                )
                status_text = self.tr("cycle_ops.legacy.status.completed", mode=mode_label)
                if request.mode == "incremental":
                    incremental_result = processor.run_incremental()
                    if incremental_result == "requires_full_backfill":
                        status_text = self.tr("cycle_ops.legacy.status.requires_full_backfill")
                else:
                    custom_date = request.custom_date if request.mode == "custom" else None
                    processor.run_range(request.mode, custom_date)
                self.schedule_gui_callback(
                    0,
                    lambda: self.var_legacy_cycle_status.set(status_text),
                )
            except Exception as error:
                self.log_to_data_box(self.tr("common.log.error", error=error))
                self.schedule_gui_callback(
                    0,
                    lambda: self.var_legacy_cycle_status.set(
                        self.tr("cycle_ops.legacy.status.failed", mode=mode_label)
                    ),
                )
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dialog.error.title"), str(error)))
            finally:
                self.schedule_gui_callback(0, self.enable_data_mgmt_buttons)

        threading.Thread(target=_run, daemon=True).start()

    def on_training_mode_change(self, choice):
        selected_mode = choice
        training_mode_options = self.get_training_mode_options()
        reverse_options = {label: key for key, label in training_mode_options.items()}
        if choice in reverse_options:
            selected_mode = reverse_options[choice]
        is_base_mode = selected_mode in ("build-all", "build-base")
        is_v1_mode = selected_mode in ("build-all", "build-v1")

        if is_base_mode:
            self.training_plc_row.grid()
            self.training_spot_row.grid()
            self.training_base_output_row.grid()
            self.training_filename_row.grid()
        else:
            self.training_plc_row.grid_remove()
            self.training_spot_row.grid_remove()
            self.training_base_output_row.grid_remove()
            self.training_filename_row.grid_remove()

        if selected_mode == "build-v1":
            self.training_base_input_row.grid()
        else:
            self.training_base_input_row.grid_remove()

        if is_v1_mode:
            self.training_dataset_output_row.grid()
        else:
            self.training_dataset_output_row.grid_remove()

        self.var_training_status.set(
            self.tr(
                "data_mgmt.training.status.idle",
                mode=self.get_training_mode_label(selected_mode),
            )
        )

    def on_run_training_build(self):
        if not self.can_start_data_task():
            return
        selected_mode = self.get_selected_training_mode()
        mode_label = self.get_training_mode_label(selected_mode)
        request = TrainingBuildRequest(
            mode=selected_mode,
            plc_file_path=self.var_training_plc_file.get(),
            spot_file_path=self.var_training_spot_file.get(),
            training_base_file_path=self.var_training_base_file.get(),
            training_base_output_path=self.var_training_base_output.get(),
            training_dataset_output_path=self.var_training_dataset_output.get(),
            filename_hint=self.var_training_filename_hint.get(),
        )
        self.disable_data_mgmt_buttons()
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text=self.format_percent_text(0.0))
        self.var_training_status.set(self.tr("data_mgmt.training.status.running", mode=mode_label))
        self.log_to_data_box(self.tr("data_mgmt.training.log.started", mode=mode_label))

        def _run():
            try:
                written_paths = execute_training_build(
                    request,
                    self.log_to_data_box,
                    self.update_data_progress,
                    self.tr_map,
                )
                self.log_to_data_box(self.tr("data_mgmt.training.log.completed"))
                for written_path in written_paths:
                    self.log_to_data_box(self.tr("data_mgmt.training.log.output_path", output_path=written_path))
                self.schedule_gui_callback(
                    0,
                    lambda: self.var_training_status.set(
                        self.tr("data_mgmt.training.status.completed", mode=mode_label)
                    ),
                )
                self.schedule_gui_callback(
                    0,
                    lambda: messagebox.showinfo(
                        self.tr("dialog.completed.title"),
                        "\n".join(
                            [self.tr("data_mgmt.training.dialog.completed")]
                            + [str(written_path) for written_path in written_paths]
                        ),
                    ),
                )
            except Exception as error:
                self.log_to_data_box(self.tr("common.log.error", error=error))
                self.schedule_gui_callback(
                    0,
                    lambda: self.var_training_status.set(
                        self.tr("data_mgmt.training.status.failed", mode=mode_label)
                    ),
                )
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dialog.error.title"), str(error)))
            finally:
                self.schedule_gui_callback(0, self.enable_data_mgmt_buttons)

        threading.Thread(target=_run, daemon=True).start()

    def on_run_archive_preview(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        if not self.ensure_local_supabase_ready(self.tr("data_mgmt.archive.action.preview")):
            return
        if not self.can_start_data_task():
            return
        request = ArchiveMetricsRequest(
            before_date=self.var_archive_before_date.get(),
            archive_dir=self.var_archive_dir.get(),
        )
        self.disable_data_mgmt_buttons()
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text=self.format_percent_text(0.0))
        self.var_archive_status.set(self.tr("data_mgmt.archive.status.preview_running"))
        self.log_to_data_box(self.tr("data_mgmt.archive.log.preview_started"))

        def _run():
            try:
                output_path = execute_archive_preview(
                    request,
                    self.log_to_data_box,
                    self.update_data_progress,
                    self.tr_map,
                )
                self.schedule_gui_callback(0, lambda: self.var_archive_status.set(self.tr("data_mgmt.archive.status.preview_completed")))
                self.schedule_gui_callback(
                    0,
                    lambda: messagebox.showinfo(
                        self.tr("dialog.completed.title"),
                        self.tr("data_mgmt.archive.dialog.preview_completed", output_path=output_path),
                    ),
                )
            except Exception as error:
                self.log_to_data_box(self.tr("common.log.error", error=error))
                self.schedule_gui_callback(0, lambda: self.var_archive_status.set(self.tr("data_mgmt.archive.status.preview_failed")))
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dialog.error.title"), str(error)))
            finally:
                self.schedule_gui_callback(0, self.enable_data_mgmt_buttons)

        threading.Thread(target=_run, daemon=True).start()

    def on_run_archive_export(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        if not self.ensure_local_supabase_ready(self.tr("data_mgmt.archive.action.run")):
            return
        if not self.can_start_data_task():
            return
        request = ArchiveMetricsRequest(
            before_date=self.var_archive_before_date.get(),
            archive_dir=self.var_archive_dir.get(),
        )
        delete_after_export = self.var_archive_delete.get()
        if delete_after_export:
            confirmed = self.ask_yes_no(
                "data_mgmt.archive.dialog.delete_confirm.title",
                "data_mgmt.archive.dialog.delete_confirm.body",
                before_date=request.before_date,
            )
            if not confirmed:
                return

        self.disable_data_mgmt_buttons()
        self.data_log_box.delete("1.0", "end")
        self.data_progress_bar.set(0)
        self.data_progress_label.configure(text=self.format_percent_text(0.0))
        self.var_archive_status.set(self.tr("data_mgmt.archive.status.running"))
        self.log_to_data_box(self.tr("data_mgmt.archive.log.started"))

        def _run():
            try:
                if delete_after_export:
                    output_path = execute_archive_export_and_delete(
                        request,
                        self.log_to_data_box,
                        self.update_data_progress,
                        self.tr_map,
                    )
                else:
                    output_path = execute_archive_export(
                        request,
                        self.log_to_data_box,
                        self.update_data_progress,
                        self.tr_map,
                    )
                self.schedule_gui_callback(0, lambda: self.var_archive_status.set(self.tr("data_mgmt.archive.status.completed")))
                self.schedule_gui_callback(
                    0,
                    lambda: messagebox.showinfo(
                        self.tr("dialog.completed.title"),
                        self.tr("data_mgmt.archive.dialog.completed", output_path=output_path),
                    ),
                )
            except Exception as error:
                self.log_to_data_box(self.tr("common.log.error", error=error))
                self.schedule_gui_callback(0, lambda: self.var_archive_status.set(self.tr("data_mgmt.archive.status.failed")))
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dialog.error.title"), str(error)))
            finally:
                self.schedule_gui_callback(0, self.enable_data_mgmt_buttons)

        threading.Thread(target=_run, daemon=True).start()

    def disable_data_mgmt_buttons(self):
        self.is_data_task_running = True
        if hasattr(self, 'btn_run_canonical_refresh') and self.btn_run_canonical_refresh.winfo_exists():
            self.btn_run_canonical_refresh.configure(state="disabled")
        if hasattr(self, 'btn_run_cycle_snapshot') and self.btn_run_cycle_snapshot.winfo_exists():
            self.btn_run_cycle_snapshot.configure(state="disabled")
        if hasattr(self, 'btn_run_cycle_health') and self.btn_run_cycle_health.winfo_exists():
            self.btn_run_cycle_health.configure(state="disabled")
        if hasattr(self, 'btn_run_legacy_cycle') and self.btn_run_legacy_cycle.winfo_exists():
            self.btn_run_legacy_cycle.configure(state="disabled")
        if hasattr(self, 'btn_run_training_build') and self.btn_run_training_build.winfo_exists():
            self.btn_run_training_build.configure(state="disabled")
        if hasattr(self, 'btn_archive_preview') and self.btn_archive_preview.winfo_exists():
            self.btn_archive_preview.configure(state="disabled")
        if hasattr(self, 'btn_archive_export') and self.btn_archive_export.winfo_exists():
            self.btn_archive_export.configure(state="disabled")

    def enable_data_mgmt_buttons(self):
        self.is_data_task_running = False
        if hasattr(self, 'btn_run_canonical_refresh') and self.btn_run_canonical_refresh.winfo_exists():
            self.btn_run_canonical_refresh.configure(state="normal")
        if hasattr(self, 'btn_run_cycle_snapshot') and self.btn_run_cycle_snapshot.winfo_exists():
            self.btn_run_cycle_snapshot.configure(state="normal")
        if hasattr(self, 'btn_run_cycle_health') and self.btn_run_cycle_health.winfo_exists():
            self.btn_run_cycle_health.configure(state="normal")
        if hasattr(self, 'btn_run_legacy_cycle') and self.btn_run_legacy_cycle.winfo_exists():
            self.btn_run_legacy_cycle.configure(state="normal")
        if hasattr(self, 'btn_run_training_build') and self.btn_run_training_build.winfo_exists():
            self.btn_run_training_build.configure(state="normal")
        if hasattr(self, 'btn_archive_preview') and self.btn_archive_preview.winfo_exists():
            self.btn_archive_preview.configure(state="normal")
        if hasattr(self, 'btn_archive_export') and self.btn_archive_export.winfo_exists():
            self.btn_archive_export.configure(state="normal")

    def pick_training_plc_file(self):
        selected_file = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if selected_file:
            self.var_training_plc_file.set(selected_file)
            if self.var_training_filename_hint.get().strip() == "":
                self.var_training_filename_hint.set(os.path.basename(selected_file))
            plc_path = Path(selected_file)
            if self.var_training_base_output.get().strip() == "":
                self.var_training_base_output.set(
                    str(plc_path.with_name(f"{plc_path.stem}_training_base.parquet"))
                )
            if self.var_training_dataset_output.get().strip() == "":
                self.var_training_dataset_output.set(
                    str(plc_path.with_name(f"{plc_path.stem}_training_dataset_v1.parquet"))
                )

    def pick_training_spot_file(self):
        selected_file = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if selected_file:
            self.var_training_spot_file.set(selected_file)

    def pick_training_base_file(self):
        selected_file = filedialog.askopenfilename(filetypes=[("Parquet Files", "*.parquet")])
        if selected_file:
            self.var_training_base_file.set(selected_file)
            if self.var_training_dataset_output.get().strip() == "":
                base_path = Path(selected_file)
                self.var_training_dataset_output.set(
                    str(base_path.with_name(f"{base_path.stem}_training_dataset_v1.parquet"))
                )

    def pick_training_base_output(self):
        selected_file = filedialog.asksaveasfilename(
            defaultextension=".parquet",
            filetypes=[("Parquet Files", "*.parquet")],
        )
        if selected_file:
            self.var_training_base_output.set(selected_file)

    def pick_training_dataset_output(self):
        selected_file = filedialog.asksaveasfilename(
            defaultextension=".parquet",
            filetypes=[("Parquet Files", "*.parquet")],
        )
        if selected_file:
            self.var_training_dataset_output.set(selected_file)

    def pick_archive_dir(self):
        selected_directory = filedialog.askdirectory()
        if selected_directory:
            self.var_archive_dir.set(selected_directory)

    def can_start_data_task(self) -> bool:
        if self.is_uploading:
            self.show_warning("dialog.data_task_running.title", "dialog.data_task_running.upload_block")
            return False
        if self.is_data_task_running:
            self.show_warning("dialog.data_task_running.title", "dialog.data_task_running.already_running")
            return False
        return True
        
    def log_to_data_box(self, msg):
        self.schedule_gui_callback(0, lambda: self._append_data_msg(msg))
        
    def _append_data_msg(self, msg):
        if hasattr(self, 'data_log_box') and self.data_log_box.winfo_exists():
            self.data_log_box.insert("end", str(msg) + "\n")
            self.data_log_box.see("end")
    
    def update_data_progress(self, value):
        """Update progress bar from background thread"""
        def _update():
            if hasattr(self, 'data_progress_bar') and self.data_progress_bar.winfo_exists():
                self.data_progress_bar.set(value)
                self.data_progress_label.configure(text=self.format_percent_text(value))
        self.schedule_gui_callback(0, _update)

    def refresh_runtime_context_labels(self, supabase_url: str, edge_url: str):
        context_text = build_runtime_context_text(
            self.config_metadata,
            supabase_url,
            edge_url,
            self.tr_map,
        )
        if hasattr(self, 'lbl_runtime_context') and self.lbl_runtime_context.winfo_exists():
            self.lbl_runtime_context.configure(text=context_text)
        if hasattr(self, 'lbl_settings_context') and self.lbl_settings_context.winfo_exists():
            self.lbl_settings_context.configure(text=context_text)
        if self.current_view == "dashboard":
            self.refresh_upload_operational_cards()

    def build_current_settings_values(self) -> dict[str, str]:
        custom_date_start, custom_date_end = resolve_custom_range_texts(
            self.cfg.get('CUSTOM_DATE_START', ''),
            self.cfg.get('CUSTOM_DATE_END', ''),
            self.cfg.get('CUSTOM_DATE', ''),
        )
        return {
            'SUPABASE_URL': self.cfg.get('SUPABASE_URL', ''),
            'SUPABASE_ANON_KEY': self.cfg.get('SUPABASE_ANON_KEY', ''),
            'EDGE_FUNCTION_URL': self.cfg.get('EDGE_FUNCTION_URL', ''),
            'PLC_DIR': self.cfg.get('PLC_DIR', ''),
            'WSL_VHDX_PATH': self.cfg.get('WSL_VHDX_PATH', ''),
            'AUTO_UPLOAD': str(self.cfg.get('AUTO_UPLOAD', 'false')).lower(),
            'SMART_SYNC': str(self.cfg.get('SMART_SYNC', 'true')).lower(),
            'UI_LANGUAGE': normalize_language_code(
                self.cfg.get('UI_LANGUAGE', DEFAULT_UI_LANGUAGE)
            ),
            'RANGE_MODE': self.cfg.get('RANGE_MODE', 'yesterday'),
            'CUSTOM_DATE_START': custom_date_start,
            'CUSTOM_DATE_END': custom_date_end,
        }

    def build_upload_profile_values(self, vals: dict[str, str]) -> dict[str, str]:
        custom_date_start, custom_date_end = resolve_custom_range_texts(
            vals.get('CUSTOM_DATE_START', ''),
            vals.get('CUSTOM_DATE_END', ''),
            vals.get('CUSTOM_DATE', ''),
        )
        return {
            'SUPABASE_URL': vals.get('SUPABASE_URL', ''),
            'SUPABASE_ANON_KEY': vals.get('SUPABASE_ANON_KEY', ''),
            'EDGE_FUNCTION_URL': vals.get('EDGE_FUNCTION_URL', ''),
            'PLC_DIR': vals.get('PLC_DIR', ''),
            'SMART_SYNC': str(vals.get('SMART_SYNC', 'true')).lower(),
            'RANGE_MODE': vals.get('RANGE_MODE', 'yesterday'),
            'CUSTOM_DATE_START': custom_date_start,
            'CUSTOM_DATE_END': custom_date_end,
            'MTIME_LAG_MIN': vals.get('MTIME_LAG_MIN', '15'),
            'CHECK_LOCK': vals.get('CHECK_LOCK', 'true'),
        }

    def format_optional_timestamp(self, raw_value: object) -> str:
        return format_optional_timestamp_text(raw_value)

    def collect_retryable_upload_items(
        self,
        items: list[tuple[str, str, str, str]],
        resume_map: dict[str, int],
        failed_retry_set: tuple[core_state.FailedRetryEntry, ...],
    ) -> list[tuple[str, str, str, str]]:
        return list(
            collect_retryable_upload_items_from_state(
                items,
                resume_map,
                failed_retry_set,
            )
        )

    def refresh_upload_action_buttons(self) -> None:
        if not hasattr(self, "btn_start") or not self.btn_start.winfo_exists():
            return
        start_enabled = "normal"
        if self.is_uploading or self.is_upload_preflight_blocked or self.state_health_blocks_upload:
            start_enabled = "disabled"
        retry_enabled = "normal" if (not self.is_uploading and not self.state_health_blocks_upload and self.has_retryable_state) else "disabled"
        pause_enabled = "normal" if self.is_uploading else "disabled"
        pause_text = self.btn_pause.cget("text") if hasattr(self, "btn_pause") else self.tr("dashboard.button.pause")
        self.btn_start.configure(state=start_enabled)
        self.btn_retry_failed.configure(state=retry_enabled)
        self.btn_pause.configure(state=pause_enabled, text=pause_text)

    def refresh_upload_operational_cards(self) -> None:
        if self.current_view != "dashboard":
            return
        if not hasattr(self, "lbl_upload_precheck_summary") or not self.lbl_upload_precheck_summary.winfo_exists():
            return

        self.upload_operational_cards_request_id += 1
        request_id = self.upload_operational_cards_request_id
        self.is_upload_preflight_blocked = True
        self.lbl_upload_precheck_summary.configure(
            text=self.tr("common.status.waiting"),
            text_color="gray",
        )
        self.lbl_upload_precheck_items.configure(text="")
        self.refresh_upload_action_buttons()

        if self.is_upload_operational_cards_refreshing:
            return

        self.is_upload_operational_cards_refreshing = True
        cfg_snapshot = dict(self.cfg)
        preview_scan_result = self.last_preview_scan_result
        local_supabase_status_text = self.var_local_supabase_status.get()
        dashboard_view_generation = self.dashboard_view_generation

        def _run() -> None:
            operational_cards_state: UploadOperationalCardsState | None = None
            try:
                state_health_snapshot = load_state_health_snapshot(True)
                if state_health_snapshot["state"] == "blocked":
                    dashboard_state_snapshot = {
                        "resume": {},
                        "recent_successful_upload_profile": None,
                        "failed_retry_set": (),
                    }
                else:
                    dashboard_state_snapshot = load_upload_dashboard_state_snapshot()
                operational_cards_state = build_upload_operational_cards_state(
                    cfg_snapshot,
                    dashboard_state_snapshot,
                    state_health_snapshot,
                    preview_scan_result,
                    local_supabase_status_text,
                    self.tr_map,
                )
            except Exception as error:
                operational_cards_state = UploadOperationalCardsState(
                    recent_successful_upload_profile=None,
                    failed_retry_set=(),
                    state_health_status_text=self.tr("dashboard.state_store.status.recovery_required"),
                    state_health_status_color="#E06C75",
                    state_health_detail_lines=(str(error),),
                    state_health_blocks_upload=True,
                    is_upload_preflight_blocked=True,
                    has_retryable_state=False,
                    retryable_upload_items=(),
                    preflight_status_text=self.tr("dashboard.upload.status.preflight_blocked"),
                    preflight_status_color="#E06C75",
                    preflight_detail_lines=(str(error),),
                    resume_status_text=self.tr("dashboard.upload.status.resume_empty"),
                    resume_status_color="gray",
                    resume_detail_lines=(),
                    recent_success_status_text=self.tr("dashboard.upload.status.recent_success_empty"),
                    recent_success_status_color="gray",
                    recent_success_detail_lines=(),
                    can_rerun_recent_success=False,
                )

            def _apply() -> None:
                self.is_upload_operational_cards_refreshing = False
                if dashboard_view_generation == self.dashboard_view_generation:
                    self.apply_upload_operational_cards_state(operational_cards_state)
                if request_id != self.upload_operational_cards_request_id:
                    self.refresh_upload_operational_cards()

            self.schedule_gui_callback(0, _apply)

        threading.Thread(target=_run, daemon=True).start()

    def apply_upload_operational_cards_state(
        self,
        operational_cards_state: UploadOperationalCardsState | None,
    ) -> None:
        if self.current_view != "dashboard":
            return
        if not hasattr(self, "lbl_upload_precheck_summary") or not self.lbl_upload_precheck_summary.winfo_exists():
            return
        if operational_cards_state is None:
            return

        self.upload_operational_cards_state = operational_cards_state
        self.recent_successful_upload_profile = operational_cards_state.recent_successful_upload_profile
        self.failed_retry_set = list(operational_cards_state.failed_retry_set)
        self.state_health_blocks_upload = operational_cards_state.state_health_blocks_upload
        self.is_upload_preflight_blocked = operational_cards_state.is_upload_preflight_blocked
        self.has_retryable_state = operational_cards_state.has_retryable_state
        self.retryable_upload_items = list(operational_cards_state.retryable_upload_items)
        self.var_state_store_status.set(operational_cards_state.state_health_status_text)
        if hasattr(self, "lbl_state_store_status") and self.lbl_state_store_status.winfo_exists():
            self.lbl_state_store_status.configure(text_color=operational_cards_state.state_health_status_color)
        if hasattr(self, "lbl_state_store_detail") and self.lbl_state_store_detail.winfo_exists():
            self.lbl_state_store_detail.configure(text="\n".join(operational_cards_state.state_health_detail_lines))

        self.lbl_upload_precheck_summary.configure(
            text=operational_cards_state.preflight_status_text,
            text_color=operational_cards_state.preflight_status_color,
        )
        self.lbl_upload_precheck_items.configure(text="\n".join(operational_cards_state.preflight_detail_lines))
        self.lbl_upload_resume_state.configure(
            text=operational_cards_state.resume_status_text,
            text_color=operational_cards_state.resume_status_color,
        )
        self.lbl_upload_resume_detail.configure(text="\n".join(operational_cards_state.resume_detail_lines))
        self.lbl_recent_success_state.configure(
            text=operational_cards_state.recent_success_status_text,
            text_color=operational_cards_state.recent_success_status_color,
        )
        self.lbl_recent_success_detail.configure(text="\n".join(operational_cards_state.recent_success_detail_lines))
        self.btn_rerun_recent_success.configure(
            state="normal" if operational_cards_state.can_rerun_recent_success else "disabled"
        )

        self.refresh_upload_action_buttons()

    def build_settings_form_values(self) -> dict[str, str]:
        return {
            'SUPABASE_URL': self.var_url.get(),
            'SUPABASE_ANON_KEY': self.var_anon.get(),
            'EDGE_FUNCTION_URL': normalize_edge_url(self.var_edge.get(), self.var_url.get()),
            'PLC_DIR': self.var_plc.get(),
            'WSL_VHDX_PATH': self.var_wsl_vhdx_path.get(),
            'AUTO_UPLOAD': str(self.var_auto_upload.get()).lower(),
            'SMART_SYNC': str(self.var_smart_sync.get()).lower(),
            'UI_LANGUAGE': normalize_language_code(self.var_ui_language.get()),
            'RANGE_MODE': self.get_selected_range_mode(),
            'CUSTOM_DATE_START': self.var_custom_date_start.get(),
            'CUSTOM_DATE_END': self.var_custom_date_end.get(),
        }

    def set_settings_dirty_state(self, is_dirty: bool):
        self.is_settings_dirty = is_dirty
        if is_dirty:
            self.var_settings_dirty.set(self.tr("settings.dirty.pending"))
            if hasattr(self, 'lbl_settings_dirty') and self.lbl_settings_dirty.winfo_exists():
                self.lbl_settings_dirty.configure(text_color="#E5C07B")
            return

        self.var_settings_dirty.set(self.tr("settings.dirty.clean"))
        if hasattr(self, 'lbl_settings_dirty') and self.lbl_settings_dirty.winfo_exists():
            self.lbl_settings_dirty.configure(text_color="gray")

    def validate_settings_form_state(self) -> tuple[bool, str, str]:
        form_values = self.build_settings_form_values()
        ok_cfg, missing = validate_config(form_values)
        if not ok_cfg:
            return (
                False,
                self.tr("settings.validation.required_fields", fields=", ".join(missing)),
                "#E06C75",
            )

        if form_values['RANGE_MODE'] == 'custom':
            custom_date_start = form_values['CUSTOM_DATE_START'].strip()
            custom_date_end = form_values['CUSTOM_DATE_END'].strip()
            if custom_date_start == "" or custom_date_end == "":
                return False, self.tr("settings.validation.custom_range_missing"), "#E06C75"
            try:
                compute_date_window(
                    form_values['RANGE_MODE'],
                    custom_date_start,
                    custom_date_end,
                )
            except Exception:
                return False, self.tr("settings.validation.custom_range_invalid"), "#E06C75"

        if is_edge_url_origin_mismatch(self.var_edge.get(), self.var_url.get()):
            return True, self.tr("settings.validation.edge_host_mismatch"), "#E5C07B"

        if self.is_settings_dirty:
            return True, self.tr("settings.validation.save_available"), "#2CC985"

        return True, self.tr("settings.validation.no_action_needed"), "gray"

    def refresh_settings_dirty_state(self):
        if not hasattr(self, 'var_url'):
            return
        current_values = self.build_current_settings_values()
        form_values = self.build_settings_form_values()
        self.set_settings_dirty_state(form_values != current_values)

    def refresh_settings_form_state(self):
        self.refresh_settings_dirty_state()
        is_valid, validation_text, validation_color = self.validate_settings_form_state()
        self.var_settings_validation.set(validation_text)
        self.refresh_custom_range_summary()
        if hasattr(self, 'lbl_settings_validation') and self.lbl_settings_validation.winfo_exists():
            self.lbl_settings_validation.configure(text_color=validation_color)
        if hasattr(self, 'btn_save_settings') and self.btn_save_settings.winfo_exists():
            self.btn_save_settings.configure(state="normal" if self.is_settings_dirty and is_valid else "disabled")

    def register_settings_dirty_callbacks(self):
        tracked_variables = (
            self.var_url,
            self.var_anon,
            self.var_edge,
            self.var_plc,
            self.var_wsl_vhdx_path,
            self.var_smart_sync,
            self.var_auto_upload,
            self.var_ui_language,
            self.var_range,
            self.var_custom_date_start,
            self.var_custom_date_end,
        )
        for tracked_variable in tracked_variables:
            tracked_variable.trace_add("write", lambda *_: self.refresh_settings_form_state())

    def refresh_custom_range_summary(self):
        if not hasattr(self, 'var_custom_range_summary'):
            return
        if self.get_selected_range_mode() != 'custom':
            self.var_custom_range_summary.set(self.tr("settings.custom_range.summary.default"))
            return

        custom_date_start = self.var_custom_date_start.get().strip()
        custom_date_end = self.var_custom_date_end.get().strip()
        if custom_date_start == "" and custom_date_end == "":
            self.var_custom_range_summary.set(self.tr("settings.custom_range.summary.empty"))
            return
        if custom_date_start == "" or custom_date_end == "":
            self.var_custom_range_summary.set(self.tr("settings.custom_range.summary.partial"))
            return

        try:
            start_date, end_date = compute_date_window('custom', custom_date_start, custom_date_end)
        except Exception as error:
            self.var_custom_range_summary.set(str(error))
            return

        if start_date is None:
            self.var_custom_range_summary.set(self.tr("settings.custom_range.summary.unavailable"))
            return

        day_span = (end_date - start_date).days + 1
        self.var_custom_range_summary.set(
            self.tr(
                "settings.custom_range.summary.selected",
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat(),
                day_span=day_span,
            )
        )

    def parse_settings_calendar_date(self, date_text: str) -> datetime | None:
        cleaned = date_text.strip()
        if cleaned == "":
            return None
        try:
            parsed = core_files.parse_iso_date(cleaned)
        except Exception:
            return None
        return datetime(parsed.year, parsed.month, parsed.day, tzinfo=KST)

    def get_settings_calendar_value(self, target: str) -> str:
        if target == "start":
            return self.var_custom_date_start.get()
        return self.var_custom_date_end.get()

    def set_settings_calendar_value(self, target: str, date_value: str):
        if target == "start":
            self.var_custom_date_start.set(date_value)
            return
        self.var_custom_date_end.set(date_value)

    def close_settings_calendar(self):
        popup = self.settings_calendar_popup
        self.settings_calendar_popup = None
        self.settings_calendar_target = ""
        if popup is not None and popup.winfo_exists():
            popup.destroy()

    def open_settings_calendar(self, target: str):
        selected_datetime = self.parse_settings_calendar_date(self.get_settings_calendar_value(target))
        visible_datetime = selected_datetime
        if visible_datetime is None:
            now = kst_now()
            visible_datetime = datetime(now.year, now.month, now.day, tzinfo=KST)

        self.close_settings_calendar()
        popup = ctk.CTkToplevel(self)
        popup.title(self.tr("settings.calendar.title"))
        popup.geometry("360x360")
        popup.resizable(False, False)
        popup.transient(self)
        popup.grab_set()
        popup.protocol("WM_DELETE_WINDOW", self.close_settings_calendar)

        self.settings_calendar_popup = popup
        self.settings_calendar_target = target
        self.settings_calendar_year = visible_datetime.year
        self.settings_calendar_month = visible_datetime.month
        self.render_settings_calendar()

    def shift_settings_calendar_month(self, month_delta: int):
        month_index = (self.settings_calendar_year * 12 + self.settings_calendar_month - 1) + month_delta
        self.settings_calendar_year = month_index // 12
        self.settings_calendar_month = (month_index % 12) + 1
        self.render_settings_calendar()

    def apply_settings_calendar_date(self, day_number: int):
        selected_datetime = datetime(
            self.settings_calendar_year,
            self.settings_calendar_month,
            day_number,
            tzinfo=KST,
        )
        self.set_settings_calendar_value(
            self.settings_calendar_target,
            selected_datetime.date().isoformat(),
        )
        self.close_settings_calendar()

    def apply_settings_calendar_today(self):
        self.set_settings_calendar_value(
            self.settings_calendar_target,
            kst_now().date().isoformat(),
        )
        self.close_settings_calendar()

    def clear_settings_calendar_value(self):
        self.set_settings_calendar_value(self.settings_calendar_target, "")
        self.close_settings_calendar()

    def render_settings_calendar(self):
        popup = self.settings_calendar_popup
        if popup is None or not popup.winfo_exists():
            return

        for child in popup.winfo_children():
            child.destroy()

        target_name = (
            self.tr("settings.label.custom_start_date")
            if self.settings_calendar_target == "start"
            else self.tr("settings.label.custom_end_date")
        )
        selected_datetime = self.parse_settings_calendar_date(
            self.get_settings_calendar_value(self.settings_calendar_target)
        )
        today_date = kst_now().date()

        header = ctk.CTkFrame(popup, fg_color="transparent")
        header.pack(fill="x", padx=12, pady=(12, 8))
        ctk.CTkLabel(
            header,
            text=self.tr("settings.calendar.select_target", target_name=target_name),
            font=ctk.CTkFont(size=16, weight="bold"),
        ).pack(side="left")

        month_row = ctk.CTkFrame(popup, fg_color="transparent")
        month_row.pack(fill="x", padx=12)
        ctk.CTkButton(
            month_row,
            text=self.tr("settings.calendar.previous_month"),
            width=40,
            command=lambda: self.shift_settings_calendar_month(-1),
        ).pack(side="left")
        ctk.CTkLabel(
            month_row,
            text=self.tr(
                "settings.calendar.month_title",
                year=self.settings_calendar_year,
                month=self.settings_calendar_month,
            ),
            font=ctk.CTkFont(size=15, weight="bold"),
        ).pack(side="left", expand=True)
        ctk.CTkButton(
            month_row,
            text=self.tr("settings.calendar.next_month"),
            width=40,
            command=lambda: self.shift_settings_calendar_month(1),
        ).pack(side="right")

        weekdays_frame = ctk.CTkFrame(popup, fg_color="transparent")
        weekdays_frame.pack(fill="x", padx=12, pady=(10, 4))
        weekday_names = (
            self.tr("settings.calendar.weekday.mon"),
            self.tr("settings.calendar.weekday.tue"),
            self.tr("settings.calendar.weekday.wed"),
            self.tr("settings.calendar.weekday.thu"),
            self.tr("settings.calendar.weekday.fri"),
            self.tr("settings.calendar.weekday.sat"),
            self.tr("settings.calendar.weekday.sun"),
        )
        for column_index, weekday_name in enumerate(weekday_names):
            ctk.CTkLabel(weekdays_frame, text=weekday_name, width=44).grid(
                row=0,
                column=column_index,
                padx=2,
                pady=2,
            )

        days_frame = ctk.CTkFrame(popup, fg_color="transparent")
        days_frame.pack(fill="both", expand=True, padx=12, pady=(0, 8))
        month_matrix = calendar.monthcalendar(self.settings_calendar_year, self.settings_calendar_month)
        for row_index, week in enumerate(month_matrix):
            for column_index, day_number in enumerate(week):
                if day_number == 0:
                    ctk.CTkLabel(days_frame, text="", width=44, height=34).grid(
                        row=row_index,
                        column=column_index,
                        padx=2,
                        pady=2,
                    )
                    continue

                day_date = datetime(
                    self.settings_calendar_year,
                    self.settings_calendar_month,
                    day_number,
                    tzinfo=KST,
                ).date()
                is_selected = selected_datetime is not None and selected_datetime.date() == day_date
                is_today = day_date == today_date

                fg_color = "#2F3340"
                hover_color = "#3A3F4D"
                if is_selected:
                    fg_color = "#2CC985"
                    hover_color = "#26A670"
                elif is_today:
                    fg_color = "#3B8ED0"
                    hover_color = "#2D6FA6"

                ctk.CTkButton(
                    days_frame,
                    text=str(day_number),
                    width=44,
                    height=34,
                    fg_color=fg_color,
                    hover_color=hover_color,
                    command=lambda selected_day=day_number: self.apply_settings_calendar_date(selected_day),
                ).grid(row=row_index, column=column_index, padx=2, pady=2)

        actions = ctk.CTkFrame(popup, fg_color="transparent")
        actions.pack(fill="x", padx=12, pady=(0, 12))
        ctk.CTkButton(
            actions,
            text=self.tr("common.button.today"),
            width=70,
            command=self.apply_settings_calendar_today,
        ).pack(side="left")
        ctk.CTkButton(
            actions,
            text=self.tr("common.button.clear"),
            width=70,
            command=self.clear_settings_calendar_value,
        ).pack(side="left", padx=8)
        ctk.CTkButton(
            actions,
            text=self.tr("common.button.close"),
            width=70,
            command=self.close_settings_calendar,
        ).pack(side="right")

    # --- Logic Adapters ---
    def pick_plc(self):
        d = filedialog.askdirectory()
        if d: self.var_plc.set(d)

    def pick_wsl_vhdx(self):
        selected_path = filedialog.askopenfilename(
            filetypes=[("WSL VHDX", "*.vhdx"), ("All Files", "*.*")],
        )
        if selected_path:
            self.var_wsl_vhdx_path.set(selected_path)
        
    # def pick_temp(self):
    #     d = filedialog.askdirectory()
    #     if d: self.var_temp.set(d)

    def on_restore_auto_edge_url(self):
        self.var_edge.set("")
        self.show_info(
            "dialog.info.title",
            "settings.edge_url.cleared",
        )

    def open_appdata_dir(self):
        try:
            os.startfile(DATA_DIR)
        except Exception as error:
            messagebox.showerror(self.tr("dialog.file_open_failed.title"), str(error))

    def log_to_local_supabase_outputs(self, message: str):
        self.log(message)
        if hasattr(self, 'data_log_box') and self.data_log_box.winfo_exists():
            self.log_to_data_box(message)

    def set_local_supabase_progress_visible(self, is_visible: bool):
        if not hasattr(self, 'local_supabase_progress') or not self.local_supabase_progress.winfo_exists():
            return

        is_mapped = self.local_supabase_progress.winfo_manager() != ""
        if is_visible and not is_mapped:
            self.local_supabase_progress.grid(row=5, column=0, sticky="w", pady=(0, 12))
            self.local_supabase_progress.start()
            return

        if not is_visible and is_mapped:
            self.local_supabase_progress.stop()
            self.local_supabase_progress.grid_remove()

    def set_local_supabase_status_override(self, status_text: str, status_color: str):
        self.local_supabase_status_override = LocalSupabaseStatusOverride(
            status_text=status_text,
            status_color=status_color,
        )

    def clear_local_supabase_status_override(self):
        self.local_supabase_status_override = None

    def resolve_local_supabase_failure_message(self, detail: str) -> str:
        if self.tr("dashboard.local_supabase.runtime.docker_desktop_not_running") in detail:
            return self.tr("dashboard.local_supabase.failure.docker_required")
        if self.tr("dashboard.local_supabase.runtime.wsl_missing") in detail:
            return self.tr("dashboard.local_supabase.failure.startup_script")
        if self.tr("dashboard.local_supabase.runtime.wsl_path_conversion_failed", normalized_path="") in detail:
            return self.tr("dashboard.local_supabase.failure.startup_script")
        if self.tr("dashboard.local_supabase.runtime.project_root_not_found") in detail:
            return self.tr("dashboard.local_supabase.failure.startup_script")
        if self.tr("dashboard.local_supabase.runtime.startup_script_not_found", start_path="").split(":")[0] in detail:
            return self.tr("dashboard.local_supabase.failure.startup_script")
        if self.tr("dashboard.local_supabase.runtime.studio_section_missing", config_path="").split(":")[0] in detail:
            return self.tr("dashboard.local_supabase.failure.startup_script")
        if self.tr("dashboard.local_supabase.runtime.studio_port_invalid", config_path="").split(":")[0] in detail:
            return self.tr("dashboard.local_supabase.failure.startup_script")
        if self.tr("dashboard.local_supabase.runtime.studio_port_not_open") in detail:
            return self.tr("dashboard.local_supabase.failure.studio_port_not_open")
        if self.tr("dashboard.local_supabase.runtime.api_port_not_open") in detail:
            return self.tr("dashboard.local_supabase.failure.api_port_not_open")
        if self.tr("dashboard.local_supabase.runtime.startup_script_failed", exit_code=0).split("0")[0] in detail:
            return self.tr("dashboard.local_supabase.failure.startup_script")
        return self.tr("dashboard.local_supabase.failure.generic")

    def ensure_local_docker_ready(self, runtime: LocalSupabaseRuntime) -> bool:
        docker_result = check_local_docker_ready(runtime, self.tr_map)
        if docker_result.is_ready:
            return True

        detail = docker_result.detail
        self.log_to_local_supabase_outputs(
            self.tr(
                "dashboard.local_supabase.log.docker_precheck_failed",
                detail=detail,
            )
        )
        self.set_local_supabase_status_override(
            self.tr("dashboard.local_supabase.failure.docker_required"),
            "#E06C75",
        )
        self.refresh_local_supabase_button()
        self.show_error(
            "dashboard.local_supabase.dialog.docker_required.title",
            "dashboard.local_supabase.dialog.docker_required.body",
            detail=detail,
        )
        return False

    def show_studio_open_error(
        self,
        runtime: LocalSupabaseRuntime,
        detail: str,
        title_key: str,
    ):
        studio_url = build_local_supabase_studio_url(runtime)
        title_text = self.tr(title_key)
        self.log_to_local_supabase_outputs(
            self.tr(
                "dashboard.local_supabase.log.studio_open_failed",
                title=title_text,
                detail=detail,
            )
        )
        self.set_local_supabase_status_override(self.tr("dashboard.local_supabase.failure.studio_open"), "#E06C75")
        messagebox.showerror(
            title_text,
            self.tr(
                "dashboard.local_supabase.dialog.studio_open_failed.body",
                detail=detail,
                studio_url=studio_url,
            ),
        )

    def refresh_local_supabase_button(self):
        if not hasattr(self, 'btn_start_supabase') or not self.btn_start_supabase.winfo_exists():
            return
        if not hasattr(self, 'btn_open_studio') or not self.btn_open_studio.winfo_exists():
            return
        if not hasattr(self, 'btn_stop_supabase') or not self.btn_stop_supabase.winfo_exists():
            return
        if not hasattr(self, 'lbl_local_supabase_status') or not self.lbl_local_supabase_status.winfo_exists():
            return
        ui_state = self.build_local_supabase_ui_state_for_render()
        self.apply_local_supabase_ui_state(ui_state)

    def build_local_supabase_ui_state_for_render(self) -> LocalSupabaseUiState:
        supabase_url = self.cfg.get('SUPABASE_URL', '')
        snapshot = self.local_supabase_status_snapshot
        has_matching_snapshot = (
            snapshot is not None and snapshot.supabase_url == supabase_url
        )

        if not is_local_supabase_target(supabase_url):
            self.local_supabase_status_snapshot = None
            return build_local_supabase_ui_state(
                supabase_url,
                self.is_supabase_starting,
                self.is_supabase_stopping,
                self.pending_open_studio,
                None,
                False,
                False,
                self.tr_map,
            )

        if not has_matching_snapshot and not self.is_local_supabase_status_refreshing:
            self.request_local_supabase_status_refresh()

        if has_matching_snapshot:
            runtime = snapshot.runtime
            is_ready = snapshot.is_ready
            is_studio_ready = snapshot.is_studio_ready
            ui_state = build_local_supabase_ui_state(
                supabase_url,
                self.is_supabase_starting,
                self.is_supabase_stopping,
                self.pending_open_studio,
                runtime,
                is_ready,
                is_studio_ready,
                self.tr_map,
            )
            if is_ready:
                self.clear_local_supabase_status_override()
            if self.local_supabase_status_override is not None and not ui_state.show_progress and not is_ready:
                return LocalSupabaseUiState(
                    status_text=self.local_supabase_status_override.status_text,
                    status_color=self.local_supabase_status_override.status_color,
                    show_progress=False,
                    start_button_text=ui_state.start_button_text,
                    start_button_enabled=ui_state.start_button_enabled,
                    studio_button_text=ui_state.studio_button_text,
                    studio_button_enabled=ui_state.studio_button_enabled,
                    stop_button_text=ui_state.stop_button_text,
                    stop_button_enabled=ui_state.stop_button_enabled,
                )
            return ui_state

        return build_local_supabase_checking_ui_state(
            self.pending_open_studio,
            self.tr_map,
        )

    def apply_local_supabase_ui_state(self, ui_state: LocalSupabaseUiState) -> None:
        self.var_local_supabase_status.set(ui_state.status_text)
        self.lbl_local_supabase_status.configure(text_color=ui_state.status_color)
        self.set_local_supabase_progress_visible(ui_state.show_progress)
        self.btn_start_supabase.configure(
            state="normal" if ui_state.start_button_enabled else "disabled",
            text=ui_state.start_button_text,
        )
        self.btn_open_studio.configure(
            state="normal" if ui_state.studio_button_enabled else "disabled",
            text=ui_state.studio_button_text,
        )
        self.btn_stop_supabase.configure(
            state="normal" if ui_state.stop_button_enabled else "disabled",
            text=ui_state.stop_button_text,
        )

    def request_local_supabase_status_refresh(self) -> None:
        if self.is_local_supabase_status_refreshing:
            return

        supabase_url = self.cfg.get('SUPABASE_URL', '')
        self.local_supabase_status_request_id += 1
        request_id = self.local_supabase_status_request_id

        if not is_local_supabase_target(supabase_url):
            self.local_supabase_status_snapshot = None
            self.is_local_supabase_status_refreshing = False
            return

        self.is_local_supabase_status_refreshing = True

        def _run() -> None:
            try:
                runtime = resolve_local_supabase_runtime(supabase_url, self.tr_map)
                snapshot = LocalSupabaseStatusSnapshot(
                    supabase_url=supabase_url,
                    runtime=runtime,
                    is_ready=is_local_supabase_stack_ready(runtime),
                    is_studio_ready=is_local_supabase_studio_ready(runtime),
                )
            except Exception:
                snapshot = LocalSupabaseStatusSnapshot(
                    supabase_url=supabase_url,
                    runtime=None,
                    is_ready=False,
                    is_studio_ready=False,
                )

            def _apply() -> None:
                self.is_local_supabase_status_refreshing = False
                if request_id != self.local_supabase_status_request_id:
                    return
                self.local_supabase_status_snapshot = snapshot
                self.refresh_local_supabase_button()
                if self.current_view == "dashboard":
                    self.refresh_upload_operational_cards()

            self.schedule_gui_callback(0, _apply)

        threading.Thread(target=_run, daemon=True).start()

    def translate_wsl_storage_issue(self, issue_code: str) -> str:
        normalized_code = issue_code.strip()
        issue_key = f"dashboard.wsl_storage.issue.{normalized_code}"
        try:
            return self.tr(issue_key)
        except Exception:
            return self.tr("dashboard.wsl_storage.message.error")

    def build_wsl_storage_ui_snapshot(self) -> WslStorageSnapshot:
        supabase_url = self.cfg.get("SUPABASE_URL", "")
        raw_snapshot = self.wsl_storage_raw_snapshot
        has_refresh_error = self.wsl_storage_error_detail.strip() != ""
        default_used_label_text = self.tr("dashboard.wsl_storage.label.used")
        default_available_label_text = self.tr("dashboard.wsl_storage.label.available")
        default_total_label_text = self.tr("dashboard.wsl_storage.label.total")
        default_usage_label_text = self.tr("dashboard.wsl_storage.label.usage")
        if not is_local_supabase_target(supabase_url):
            return WslStorageSnapshot(
                state="unavailable",
                status_text=self.tr("dashboard.wsl_storage.badge.unavailable"),
                status_color=_resolve_wsl_storage_status_color("unavailable"),
                used_label_text=default_used_label_text,
                available_label_text=default_available_label_text,
                total_label_text=default_total_label_text,
                usage_label_text=default_usage_label_text,
                used_text="—",
                available_text="—",
                total_text="—",
                usage_text="—",
                vhdx_text="—",
                host_free_text="—",
                distro_text="—",
                source_text=self.tr("dashboard.wsl_storage.source.unavailable"),
                detail_text=self.tr("dashboard.wsl_storage.message.remote"),
                last_updated_text="—",
                progress_value=None,
                is_refreshing=False,
                is_partial=False,
                is_available=False,
            )

        if raw_snapshot is None and has_refresh_error:
            return WslStorageSnapshot(
                state="error",
                status_text=self.tr("dashboard.wsl_storage.badge.error"),
                status_color=_resolve_wsl_storage_status_color("error"),
                used_label_text=default_used_label_text,
                available_label_text=default_available_label_text,
                total_label_text=default_total_label_text,
                usage_label_text=default_usage_label_text,
                used_text="—",
                available_text="—",
                total_text="—",
                usage_text="—",
                vhdx_text="—",
                host_free_text="—",
                distro_text="—",
                source_text=self.tr("dashboard.wsl_storage.source.unavailable"),
                detail_text="\n".join(
                    [
                        self.tr("dashboard.wsl_storage.message.error"),
                        self.tr("dashboard.wsl_storage.issue.refresh_failed"),
                    ]
                ),
                last_updated_text="—",
                progress_value=None,
                is_refreshing=False,
                is_partial=False,
                is_available=False,
            )

        if raw_snapshot is None:
            badge_state = "refreshing" if self.is_wsl_storage_refreshing else "unavailable"
            detail_key = (
                "dashboard.wsl_storage.message.refreshing"
                if self.is_wsl_storage_refreshing
                else "dashboard.wsl_storage.message.unavailable"
            )
            return WslStorageSnapshot(
                state=badge_state,
                status_text=self.tr(f"dashboard.wsl_storage.badge.{badge_state}"),
                status_color=_resolve_wsl_storage_status_color(badge_state),
                used_label_text=default_used_label_text,
                available_label_text=default_available_label_text,
                total_label_text=default_total_label_text,
                usage_label_text=default_usage_label_text,
                used_text="—",
                available_text="—",
                total_text="—",
                usage_text="—",
                vhdx_text="—",
                host_free_text="—",
                distro_text="—",
                source_text=self.tr("dashboard.wsl_storage.source.unavailable"),
                detail_text=self.tr(detail_key),
                last_updated_text="—",
                progress_value=None,
                is_refreshing=self.is_wsl_storage_refreshing,
                is_partial=False,
                is_available=False,
            )

        raw_state = _normalize_wsl_storage_state(_extract_source_value(raw_snapshot, "state"))
        is_partial = bool(_extract_source_value(raw_snapshot, "is_partial")) or has_refresh_error
        badge_state = raw_state
        if raw_state == "safe" and is_partial:
            badge_state = "partial"
        if badge_state not in {"safe", "warning", "critical", "partial", "error", "unavailable", "refreshing"}:
            badge_state = "error"
        if raw_state not in {"safe", "warning", "critical", "error", "unavailable", "refreshing"}:
            raw_state = "error"

        guest_metrics = _extract_source_value(raw_snapshot, "guest_metrics")
        host_metrics = _extract_source_value(raw_snapshot, "host_metrics")
        used_bytes = _coerce_optional_int(_extract_source_value(guest_metrics, "used_bytes"))
        available_bytes = _coerce_optional_int(_extract_source_value(guest_metrics, "available_bytes"))
        total_bytes = _coerce_optional_int(_extract_source_value(guest_metrics, "total_bytes"))
        usage_ratio = _coerce_optional_float(_extract_source_value(guest_metrics, "usage_ratio"))
        distro_name = str(_extract_source_value(guest_metrics, "distro_name") or "").strip()
        vhdx_bytes = _coerce_optional_int(_extract_source_value(host_metrics, "file_size_bytes"))
        host_free_bytes = _coerce_optional_int(_extract_source_value(host_metrics, "drive_free_bytes"))
        source_name = str(_extract_source_value(host_metrics, "source") or "").strip()
        source_path = str(_extract_source_value(host_metrics, "vhdx_path") or "").strip()
        raw_issues = _extract_source_value(raw_snapshot, "issues")
        issue_messages: list[str] = []
        if isinstance(raw_issues, (list, tuple)):
            for raw_issue in raw_issues:
                issue_code = str(_extract_source_value(raw_issue, "code") or "").strip()
                if issue_code != "":
                    issue_messages.append(self.translate_wsl_storage_issue(issue_code))

        detail_lines: list[str] = []
        if is_partial:
            detail_lines.append(self.tr("dashboard.wsl_storage.message.partial"))
        if raw_state == "error":
            detail_lines.append(self.tr("dashboard.wsl_storage.message.error"))
        elif raw_state == "unavailable":
            detail_lines.append(self.tr("dashboard.wsl_storage.message.unavailable"))
        elif host_metrics is None and issue_messages == []:
            detail_lines.append(self.tr("dashboard.wsl_storage.message.guest_only"))
        if has_refresh_error:
            detail_lines.append(self.tr("dashboard.wsl_storage.issue.refresh_failed"))
        if self.is_wsl_storage_refreshing:
            detail_lines.append(self.tr("dashboard.wsl_storage.message.refreshing"))

        if distro_name != "":
            detail_lines.append(
                f"{self.tr('dashboard.wsl_storage.label.distro')}: {distro_name}"
            )
        if used_bytes is not None:
            detail_lines.append(
                f"{self.tr('dashboard.wsl_storage.label.guest_used')}: {_format_storage_bytes(used_bytes)}"
            )
        if available_bytes is not None:
            detail_lines.append(
                f"{self.tr('dashboard.wsl_storage.label.guest_available')}: {_format_storage_bytes(available_bytes)}"
            )
        if total_bytes is not None:
            detail_lines.append(
                f"{self.tr('dashboard.wsl_storage.label.guest_total')}: {_format_storage_bytes(total_bytes)}"
            )

        source_text = self.tr("dashboard.wsl_storage.source.unavailable")
        translated_source_text = source_text
        if source_name in {"config_override", "registry"}:
            translated_source_text = self.tr(f"dashboard.wsl_storage.source.{source_name}")
            source_text = translated_source_text
        compact_source_path = _format_compact_path(source_path, 52)
        if compact_source_path != "":
            source_text = compact_source_path
        detail_lines.append(
            f"{self.tr('dashboard.wsl_storage.label.source')}: {translated_source_text}"
        )
        if source_path != "":
            detail_lines.append(source_path)
        if issue_messages != []:
            detail_lines.extend(issue_messages)

        collected_at = _extract_source_value(raw_snapshot, "collected_at")
        used_label_text = default_used_label_text
        available_label_text = default_available_label_text
        total_label_text = default_total_label_text
        usage_label_text = default_usage_label_text
        used_text = _format_storage_bytes(used_bytes)
        available_text = _format_storage_bytes(available_bytes)
        total_text = _format_storage_bytes(total_bytes)
        usage_text = _format_storage_ratio(usage_ratio)
        progress_value = None if usage_ratio is None else max(0.0, min(1.0, usage_ratio))

        if vhdx_bytes is not None and host_free_bytes is not None:
            host_total_bytes = vhdx_bytes + host_free_bytes
            host_usage_ratio = None if host_total_bytes <= 0 else vhdx_bytes / host_total_bytes
            used_label_text = self.tr("dashboard.wsl_storage.label.host_vhdx_size")
            available_label_text = self.tr("dashboard.wsl_storage.label.host_drive_free")
            total_label_text = self.tr("dashboard.wsl_storage.label.host_capacity_total")
            usage_label_text = self.tr("dashboard.wsl_storage.label.host_capacity_usage")
            used_text = _format_storage_bytes(vhdx_bytes)
            available_text = _format_storage_bytes(host_free_bytes)
            total_text = _format_storage_bytes(host_total_bytes)
            usage_text = _format_storage_ratio(host_usage_ratio)
            progress_value = None if host_usage_ratio is None else max(0.0, min(1.0, host_usage_ratio))

        return WslStorageSnapshot(
            state=badge_state,
            status_text=self.tr(f"dashboard.wsl_storage.badge.{badge_state}"),
            status_color=_resolve_wsl_storage_status_color(badge_state),
            used_label_text=used_label_text,
            available_label_text=available_label_text,
            total_label_text=total_label_text,
            usage_label_text=usage_label_text,
            used_text=used_text,
            available_text=available_text,
            total_text=total_text,
            usage_text=usage_text,
            vhdx_text=_format_storage_bytes(vhdx_bytes),
            host_free_text=_format_storage_bytes(host_free_bytes),
            distro_text=distro_name if distro_name != "" else "—",
            source_text=source_text,
            detail_text="\n".join(detail_lines),
            last_updated_text=_format_storage_timestamp(collected_at),
            progress_value=progress_value,
            is_refreshing=self.is_wsl_storage_refreshing,
            is_partial=is_partial,
            is_available=guest_metrics is not None,
        )

    def render_wsl_storage_card(self) -> None:
        snapshot = self.build_wsl_storage_ui_snapshot()
        self.wsl_storage_snapshot = snapshot
        if not hasattr(self, "wsl_storage_frame") or not self.wsl_storage_frame.winfo_exists():
            self.rendered_wsl_storage_snapshot = None
            return

        if self.rendered_wsl_storage_snapshot == snapshot:
            return

        badge_fg_color = snapshot.status_color
        badge_text_color = "#101010" if snapshot.status_color == "#E5C07B" else "white"
        progress_value = snapshot.progress_value
        progress_color = snapshot.status_color
        if progress_value is None:
            progress_value = 0
            progress_color = "gray"
        detail_frame_color = "#2F3340"
        if snapshot.state in {"warning", "partial"}:
            detail_frame_color = "#4A4030"
        elif snapshot.state in {"critical", "error"}:
            detail_frame_color = "#4A3034"

        self.lbl_wsl_storage_badge.configure(
            text=snapshot.status_text,
            fg_color=badge_fg_color,
            text_color=badge_text_color,
        )
        self.btn_refresh_wsl_storage.configure(
            state="disabled" if self.is_wsl_storage_refreshing else "normal"
        )
        self.lbl_wsl_storage_used_label.configure(text=snapshot.used_label_text)
        self.lbl_wsl_storage_available_label.configure(text=snapshot.available_label_text)
        self.lbl_wsl_storage_total_label.configure(text=snapshot.total_label_text)
        self.lbl_wsl_storage_usage_label.configure(text=snapshot.usage_label_text)
        self.lbl_wsl_storage_used_value.configure(text=snapshot.used_text)
        self.lbl_wsl_storage_available_value.configure(text=snapshot.available_text)
        self.lbl_wsl_storage_total_value.configure(text=snapshot.total_text)
        self.lbl_wsl_storage_usage_value.configure(text=snapshot.usage_text)
        self.wsl_storage_progress.configure(progress_color=progress_color)
        self.wsl_storage_progress.set(progress_value)
        self.wsl_storage_detail_frame.configure(fg_color=detail_frame_color)
        self.lbl_wsl_storage_detail.configure(
            text=snapshot.detail_text,
            text_color=snapshot.status_color if snapshot.state in {"warning", "critical", "partial", "error"} else "gray",
        )
        self.lbl_wsl_storage_distro_value.configure(text=snapshot.distro_text)
        self.lbl_wsl_storage_source_value.configure(text=snapshot.source_text)
        self.lbl_wsl_storage_vhdx_value.configure(text=snapshot.vhdx_text)
        self.lbl_wsl_storage_host_free_value.configure(text=snapshot.host_free_text)
        self.lbl_wsl_storage_meta_value.configure(text=snapshot.last_updated_text)
        self.rendered_wsl_storage_snapshot = snapshot

    def request_wsl_storage_refresh(self) -> None:
        if self.is_wsl_storage_refreshing:
            return
        if not is_local_supabase_target(self.cfg.get("SUPABASE_URL", "")):
            self.wsl_storage_raw_snapshot = None
            self.wsl_storage_error_detail = ""
            self.is_wsl_storage_refreshing = False
            self.render_wsl_storage_card()
            return

        self.is_wsl_storage_refreshing = True
        self.wsl_storage_error_detail = ""
        self.render_wsl_storage_card()

        def _run() -> None:
            raw_snapshot: object | None = None
            error_detail = ""
            try:
                if core_wsl_storage is None:
                    raise RuntimeError("core.wsl_storage import failed")
                raw_snapshot = core_wsl_storage.collect_wsl_storage_snapshot(self.cfg)
            except Exception as error:
                error_detail = "refresh_failed"

            def _apply() -> None:
                self.is_wsl_storage_refreshing = False
                if error_detail != "":
                    self.wsl_storage_error_detail = error_detail
                else:
                    self.wsl_storage_raw_snapshot = raw_snapshot
                    self.wsl_storage_error_detail = ""
                self.render_wsl_storage_card()

            self.schedule_gui_callback(0, _apply)

        threading.Thread(target=_run, daemon=True).start()

    def ensure_local_supabase_ready(self, action_name: str) -> bool:
        supabase_url = self.cfg.get('SUPABASE_URL', '')
        if not is_local_supabase_target(supabase_url):
            return True

        try:
            runtime = resolve_local_supabase_runtime(supabase_url, self.tr_map)
        except Exception as error:
            messagebox.showerror(self.tr("dashboard.local_supabase.dialog.start_path_error.title"), str(error))
            return False

        if is_local_supabase_stack_ready(runtime):
            self.refresh_local_supabase_button()
            return True

        if self.is_supabase_stopping:
            self.show_info(
                "dashboard.local_supabase.dialog.stopping.title",
                "dashboard.local_supabase.dialog.stopping.body",
            )
            return False

        if self.is_supabase_starting:
            self.show_info(
                "dashboard.local_supabase.dialog.starting.title",
                "dashboard.local_supabase.dialog.starting.body",
            )
            return False

        if not self.ensure_local_docker_ready(runtime):
            return False

        should_start = self.ask_yes_no(
            "dashboard.local_supabase.dialog.required.title",
            "dashboard.local_supabase.dialog.required.body",
            action_name=action_name,
        )
        if not should_start:
            return False

        self.start_local_supabase(runtime, False)
        return False

    def on_start_local_supabase(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        self.refresh_runtime_context_labels(self.cfg.get('SUPABASE_URL', ''), self.cfg.get('EDGE_FUNCTION_URL', ''))
        supabase_url = self.cfg.get('SUPABASE_URL', '')
        if not is_local_supabase_target(supabase_url):
            self.show_info(
                "dashboard.local_supabase.dialog.remote_target.title",
                "dashboard.local_supabase.dialog.remote_target.body",
            )
            self.refresh_local_supabase_button()
            return

        try:
            runtime = resolve_local_supabase_runtime(supabase_url, self.tr_map)
        except Exception as error:
            messagebox.showerror(self.tr("dashboard.local_supabase.dialog.start_path_error.title"), str(error))
            self.refresh_local_supabase_button()
            return

        if is_local_supabase_stack_ready(runtime):
            self.show_info(
                "dashboard.local_supabase.dialog.already_ready.title",
                "dashboard.local_supabase.dialog.already_ready.body",
            )
            self.refresh_local_supabase_button()
            return

        if not self.ensure_local_docker_ready(runtime):
            return

        self.start_local_supabase(runtime, False)

    def on_stop_local_supabase(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        self.refresh_runtime_context_labels(self.cfg.get('SUPABASE_URL', ''), self.cfg.get('EDGE_FUNCTION_URL', ''))
        supabase_url = self.cfg.get('SUPABASE_URL', '')
        if not is_local_supabase_target(supabase_url):
            self.show_info(
                "dashboard.local_supabase.dialog.remote_target.title",
                "dashboard.local_supabase.dialog.remote_target.body",
            )
            self.refresh_local_supabase_button()
            return

        try:
            runtime = resolve_local_supabase_runtime(supabase_url, self.tr_map)
        except Exception as error:
            messagebox.showerror(self.tr("dashboard.local_supabase.dialog.stop_path_error.title"), str(error))
            self.refresh_local_supabase_button()
            return

        if self.is_supabase_starting:
            self.show_info(
                "dashboard.local_supabase.dialog.starting.title",
                "dashboard.local_supabase.dialog.wait_for_start.body",
            )
            return

        if self.is_supabase_stopping:
            self.show_info(
                "dashboard.local_supabase.dialog.stopping.title",
                "dashboard.local_supabase.dialog.stopping.body",
            )
            return

        if not is_any_local_supabase_service_ready(runtime):
            self.show_info(
                "dashboard.local_supabase.dialog.stop_not_needed.title",
                "dashboard.local_supabase.dialog.stop_not_needed.body",
            )
            self.refresh_local_supabase_button()
            return

        should_stop = self.ask_yes_no(
            "dashboard.local_supabase.dialog.stop_confirm.title",
            "dashboard.local_supabase.dialog.stop_confirm.body",
        )
        if not should_stop:
            return

        self.stop_local_supabase(runtime, False)

    def open_local_supabase_studio(self, runtime: LocalSupabaseRuntime):
        studio_url = build_local_supabase_studio_url(runtime)
        try:
            self.log_to_local_supabase_outputs(
                self.tr("dashboard.local_supabase.log.studio_open_attempt", studio_url=studio_url)
            )
            if os.name == 'nt':
                os.startfile(studio_url)
            else:
                webbrowser.open(studio_url)
        except Exception as error:
            raise RuntimeError(
                self.tr("dashboard.local_supabase.runtime.studio_open_failed", error=error)
            ) from error

    def on_open_local_supabase_studio(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        self.refresh_runtime_context_labels(self.cfg.get('SUPABASE_URL', ''), self.cfg.get('EDGE_FUNCTION_URL', ''))
        supabase_url = self.cfg.get('SUPABASE_URL', '')
        if not is_local_supabase_target(supabase_url):
            self.show_info(
                "dashboard.local_supabase.dialog.remote_target.title",
                "dashboard.local_supabase.dialog.remote_target.body",
            )
            self.refresh_local_supabase_button()
            return

        try:
            runtime = resolve_local_supabase_runtime(supabase_url, self.tr_map)
        except Exception as error:
            messagebox.showerror(self.tr("dashboard.local_supabase.dialog.studio_error.title"), str(error))
            self.refresh_local_supabase_button()
            return

        if is_local_supabase_studio_ready(runtime):
            try:
                self.open_local_supabase_studio(runtime)
            except Exception as error:
                self.show_studio_open_error(
                    runtime,
                    str(error),
                    "dashboard.local_supabase.dialog.studio_auto_open_failed.title",
                )
            return

        if is_local_supabase_stack_ready(runtime):
            if not wait_for_tcp_ready(runtime.studio_host, runtime.studio_port, 10, 1.0):
                self.show_studio_open_error(
                    runtime,
                    self.tr("dashboard.local_supabase.runtime.studio_port_not_ready"),
                    "dashboard.local_supabase.dialog.studio_port_wait_failed.title",
                )
                self.refresh_local_supabase_button()
                return
            try:
                self.open_local_supabase_studio(runtime)
            except Exception as error:
                self.show_studio_open_error(
                    runtime,
                    str(error),
                    "dashboard.local_supabase.dialog.studio_auto_open_failed.title",
                )
            return

        if self.is_supabase_stopping:
            self.show_info(
                "dashboard.local_supabase.dialog.stopping.title",
                "dashboard.local_supabase.dialog.wait_for_stop.body",
            )
            return

        if not self.ensure_local_docker_ready(runtime):
            return

        if self.is_supabase_starting:
            self.pending_open_studio = True
            self.refresh_local_supabase_button()
            self.show_info(
                "dashboard.local_supabase.dialog.starting.title",
                "dashboard.local_supabase.dialog.studio_pending.body",
            )
            return

        should_start = self.ask_yes_no(
            "dashboard.local_supabase.dialog.studio_start.title",
            "dashboard.local_supabase.dialog.studio_start.body",
        )
        if not should_start:
            return

        self.start_local_supabase(runtime, True)

    def start_local_supabase(self, runtime: LocalSupabaseRuntime, open_studio_after_start: bool):
        if self.is_supabase_starting or self.is_supabase_stopping:
            return

        if not self.ensure_local_docker_ready(runtime):
            return

        self.clear_local_supabase_status_override()
        self.is_supabase_starting = True
        self.pending_open_studio = open_studio_after_start
        self.pending_close_after_supabase_stop = False
        self.refresh_local_supabase_button()
        self.log_to_local_supabase_outputs(self.tr("dashboard.local_supabase.log.start_requested"))
        self.log_to_local_supabase_outputs(
            self.tr("dashboard.local_supabase.log.project_root", project_root=runtime.project_root)
        )
        if open_studio_after_start:
            self.log_to_local_supabase_outputs(
                self.tr("dashboard.local_supabase.log.studio_auto_open_requested")
            )

        def _run():
            try:
                command = build_wsl_start_command(runtime, self.tr_map)
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )

                if process.stdout is not None:
                    for raw_line in process.stdout:
                        line = raw_line.rstrip()
                        if line != "":
                            self.log_to_local_supabase_outputs(f"[startup] {line}")

                return_code = process.wait()
                if return_code != 0:
                    raise RuntimeError(
                        self.tr(
                            "dashboard.local_supabase.runtime.startup_script_failed",
                            exit_code=return_code,
                        )
                    )

                if not is_local_supabase_stack_ready(runtime):
                    raise RuntimeError(
                        self.tr("dashboard.local_supabase.runtime.api_port_not_open")
                    )

                if self.pending_open_studio and not wait_for_tcp_ready(runtime.studio_host, runtime.studio_port, 20, 1.0):
                    raise RuntimeError(
                        self.tr("dashboard.local_supabase.runtime.studio_port_not_open")
                    )

                self.schedule_gui_callback(
                    0,
                    lambda: self.show_info(
                        "dialog.completed.title",
                        "dashboard.local_supabase.dialog.started.body",
                    ),
                )
                self.log_to_local_supabase_outputs(self.tr("dashboard.local_supabase.log.start_completed"))
                if self.pending_open_studio:
                    try:
                        self.open_local_supabase_studio(runtime)
                    except Exception as error:
                        self.schedule_gui_callback(
                            0,
                            lambda: self.show_studio_open_error(
                                runtime,
                                str(error),
                                "dashboard.local_supabase.dialog.studio_auto_open_failed.title",
                            ),
                        )
            except Exception as error:
                self.log_to_local_supabase_outputs(
                    self.tr("dashboard.local_supabase.log.start_failed", error=error)
                )
                self.set_local_supabase_status_override(
                    self.resolve_local_supabase_failure_message(str(error)),
                    "#E06C75",
                )
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dashboard.local_supabase.dialog.start_failed.title"), str(error)))
            finally:
                self.is_supabase_starting = False
                self.pending_open_studio = False
                self.local_supabase_status_snapshot = None
                self.schedule_gui_callback(0, self.refresh_local_supabase_button)
                self.schedule_gui_callback(0, self.request_wsl_storage_refresh)

        threading.Thread(target=_run, daemon=True).start()

    def stop_local_supabase(self, runtime: LocalSupabaseRuntime, close_application_after_stop: bool):
        if self.is_supabase_starting or self.is_supabase_stopping:
            return

        self.clear_local_supabase_status_override()
        self.is_supabase_stopping = True
        self.pending_close_after_supabase_stop = close_application_after_stop
        self.refresh_local_supabase_button()
        self.log_to_local_supabase_outputs(self.tr("dashboard.local_supabase.log.stop_requested"))
        self.log_to_local_supabase_outputs(
            self.tr("dashboard.local_supabase.log.project_root", project_root=runtime.project_root)
        )

        def _run():
            stop_succeeded = False
            try:
                command = build_wsl_stop_command(runtime, self.tr_map)
                process = subprocess.Popen(
                    command,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                )

                if process.stdout is not None:
                    for raw_line in process.stdout:
                        line = raw_line.rstrip()
                        if line != "":
                            self.log_to_local_supabase_outputs(f"[shutdown] {line}")

                return_code = process.wait()
                if return_code != 0:
                    raise RuntimeError(
                        self.tr(
                            "dashboard.local_supabase.runtime.stop_command_failed",
                            exit_code=return_code,
                        )
                    )

                if wait_for_tcp_ready(runtime.api_host, runtime.api_port, 5, 1.0):
                    raise RuntimeError(
                        self.tr("dashboard.local_supabase.runtime.api_port_not_closed")
                    )

                if wait_for_tcp_ready(runtime.studio_host, runtime.studio_port, 5, 1.0):
                    raise RuntimeError(
                        self.tr("dashboard.local_supabase.runtime.studio_port_not_closed")
                    )

                stop_succeeded = True
                self.set_local_supabase_status_override(self.tr("dashboard.local_supabase.status.stopped"), "gray")
                self.log_to_local_supabase_outputs(self.tr("dashboard.local_supabase.log.stop_completed"))
                if not self.pending_close_after_supabase_stop:
                    self.schedule_gui_callback(
                        0,
                        lambda: self.show_info(
                            "dialog.completed.title",
                            "dashboard.local_supabase.dialog.stopped.body",
                        ),
                    )
            except Exception as error:
                self.log_to_local_supabase_outputs(
                    self.tr("dashboard.local_supabase.log.stop_failed", error=error)
                )
                self.set_local_supabase_status_override(self.tr("dashboard.local_supabase.failure.stop_failed"), "#E06C75")
                self.schedule_gui_callback(0, lambda: messagebox.showerror(self.tr("dashboard.local_supabase.dialog.stop_failed.title"), str(error)))
            finally:
                should_close_application = stop_succeeded and self.pending_close_after_supabase_stop
                self.is_supabase_stopping = False
                self.pending_close_after_supabase_stop = False
                self.local_supabase_status_snapshot = None
                self.schedule_gui_callback(0, self.refresh_local_supabase_button)
                self.schedule_gui_callback(0, self.request_wsl_storage_refresh)
                if should_close_application:
                    self.schedule_gui_callback(0, self.close_application)

        threading.Thread(target=_run, daemon=True).start()


    def on_save(self):
        normalized_edge = normalize_edge_url(self.var_edge.get(), self.var_url.get())
        resolved_edge, edge_state, host_mismatch = _build_edge_runtime_state(
            self.var_url.get(),
            normalized_edge,
            self.tr_map,
        )
        vals = {
            'SUPABASE_URL': self.var_url.get(),
            'SUPABASE_ANON_KEY': self.var_anon.get(),
            'EDGE_FUNCTION_URL': normalized_edge,
            'PLC_DIR': self.var_plc.get(),
            'AUTO_UPLOAD': str(self.var_auto_upload.get()).lower(),
            'SMART_SYNC': str(self.var_smart_sync.get()).lower(),
            'UI_LANGUAGE': normalize_language_code(self.var_ui_language.get()),
            'RANGE_MODE': self.get_selected_range_mode(),
            'CUSTOM_DATE_START': self.var_custom_date_start.get(),
            'CUSTOM_DATE_END': self.var_custom_date_end.get(),
            'MTIME_LAG_MIN': self.cfg.get('MTIME_LAG_MIN', '15'),
            'CHECK_LOCK': self.cfg.get('CHECK_LOCK', 'true'),
            'WSL_VHDX_PATH': self.var_wsl_vhdx_path.get(),
        }
        ok_cfg, missing = validate_config(vals)
        if not ok_cfg:
            self.show_error(
                "settings.dialog.validation_error.title",
                "settings.validation.required_fields",
                fields=", ".join(missing),
            )
            return False
        if host_mismatch:
            self.show_warning(
                "settings.dialog.edge_warning.title",
                "settings.dialog.edge_warning.body",
                resolved_edge=resolved_edge,
                edge_state=edge_state,
            )
        saved_path = save_config(vals, None)
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(saved_path)
        self.local_supabase_status_snapshot = None
        self.reload_translations()
        self.refresh_runtime_context_labels(self.cfg.get('SUPABASE_URL', ''), self.cfg.get('EDGE_FUNCTION_URL', ''))
        self.refresh_sidebar_texts()
        self.refresh_local_supabase_button()
        self.refresh_settings_form_state()
        self.refresh_current_view()
        self.show_info("settings.dialog.saved.title", "settings.dialog.saved.body")
        return True

    def check_log_queue(self):
        """Check queue for new log messages and update GUI in main thread"""
        if not hasattr(self, 'log_history'):
            self.log_history = []

        try:
            while True:
                msg = self.log_queue.get_nowait()
                
                # Add to history
                self.log_history.append(msg)
                if len(self.log_history) > 2000:
                    self.log_history = self.log_history[-1500:] # Keep last 1500
                
                # Update UI if visible
                if hasattr(self, 'log_box') and self.log_box.winfo_exists():
                    self.log_box.insert("end", msg + "\n")
                    self.log_box.see("end")
                    
                    # Prevent infinite growth in widget (Sync with history size roughly)
                    if float(self.log_box.index("end")) > 2500:
                        self.log_box.delete("1.0", "1000.0")
        except queue.Empty:
            pass
        finally:
            # Schedule next check
            self.schedule_gui_callback(100, self.check_log_queue)

    def log(self, msg, level="INFO"):
        """Thread-safe log with timestamp/level, shown in GUI and printed to console."""
        ts = kst_now().isoformat(timespec="seconds")
        full = f"[{level.upper()} {ts}] {msg}"
        # Put message in queue (Thread-safe)
        self.log_queue.put(full)
        print(full) # Always print to console

    def update_dashboard_loop(self, dashboard_view_generation: int) -> None:
        self.dashboard_update_after_id = None
        if dashboard_view_generation != self.dashboard_view_generation:
            return
        if self.current_view != "dashboard":
            return
        if not hasattr(self, 'hero_frame') or not self.hero_frame.winfo_exists():
            return # Dashboard not active

        self.is_dashboard_update_loop_running = True
        try:
            # Update Progress
            total = self.total_files if self.total_files > 0 else 1
            pct = self.processed_count / total
            self.prog_bar.set(pct)
            self.lbl_prog_text.configure(
                text=self.format_progress_summary(
                    pct,
                    self.processed_count,
                    self.total_files,
                )
            )

            if self.is_uploading:
                if not self.pause_event.is_set():
                    self.lbl_big_status.configure(text=self.tr("common.status.paused"), text_color="#E5C07B")
                    self.status_label.configure(text=self.tr("common.status.paused"), text_color="#E5C07B")
                else:
                    self.lbl_big_status.configure(text=self.tr("common.status.uploading"), text_color="#3B8ED0")
                    self.status_label.configure(text=self.tr("common.status.running"), text_color="#2CC985")
            else:
                self.lbl_big_status.configure(
                    text=self.upload_dashboard_status_text,
                    text_color=self.upload_dashboard_status_color,
                )
                self.status_label.configure(
                    text=self.upload_dashboard_status_text,
                    text_color=self.upload_dashboard_status_color,
                )

            # Update Active Tasks List
            with self.progress_lock:
                current_files = set(self.active_progress.keys())
                
                # Remove old
                for task_key in list(self.task_labels.keys()):
                    if task_key not in current_files:
                        self.task_labels[task_key].destroy()
                        del self.task_labels[task_key]
                
                # Add/Update new
                for task_key, progress_state in self.active_progress.items():
                    done, total = progress_state
                    if total > 0:
                        progress_pct = (done / total) * 100.0
                        text = self.tr(
                            "dashboard.active_task.progress",
                            task_key=task_key,
                            percent=int(progress_pct),
                        )
                    else:
                        text = f"{task_key} - {done:,}행 처리"
                    if task_key not in self.task_labels:
                        lbl = ctk.CTkLabel(self.active_tasks_list_frame, text=text, anchor="w")
                        lbl.pack(fill="x", padx=5, pady=2)
                        self.task_labels[task_key] = lbl
                    else:
                        self.task_labels[task_key].configure(text=text)
        finally:
            self.is_dashboard_update_loop_running = False

        if dashboard_view_generation != self.dashboard_view_generation:
            return
        self.schedule_dashboard_update_loop(200)

    def on_preview(self):
        self.show_logs()
        if not self._ensure_preview_state_ready():
            return
        self.log(self.tr("logs.preview.started"))
        # 기존 미리보기 로직을 그대로 쓰고 로그만 GUI로 보냅니다.
        threading.Thread(target=self._run_preview_logic, daemon=True).start()

    def _run_preview_logic(self):
        try:
            vals, config_source, config_metadata = load_config_with_sources(None)
            self.cfg = dict(vals)
            self.config_source = config_source
            self.config_metadata = config_metadata

            plc_dir = str(vals.get('PLC_DIR', '')).strip()
            range_mode = str(vals.get('RANGE_MODE', '')).strip()
            if plc_dir == "":
                raise ValueError("PLC_DIR is required for preview.")
            if range_mode == "":
                raise ValueError("RANGE_MODE is required for preview.")

            custom_date_start, custom_date_end = resolve_custom_range_texts(
                vals.get('CUSTOM_DATE_START', ''),
                vals.get('CUSTOM_DATE_END', ''),
                vals.get('CUSTOM_DATE', ''),
            )
            window_start, window_end = compute_date_window(range_mode, custom_date_start, custom_date_end)
            lag_value = str(vals.get('MTIME_LAG_MIN', '15')).strip()
            try:
                lag_minutes = int(lag_value)
            except ValueError as error:
                raise ValueError(f"MTIME_LAG_MIN must be an integer: {lag_value}") from error
            check_lock = str(vals.get('CHECK_LOCK', 'true')).strip().lower() == 'true'
            processed = load_processed()
            items, excluded = preview_diagnostics(
                plc_dir,
                None,
                window_start,
                window_end,
                lag_minutes,
                range_mode == 'today',
                check_lock,
                processed,
                self.tr_map,
            )
            self.log(self.tr("dashboard.preview.log.target_count", count=len(items)))
            for _, fn, _, _ in items[:20]:
                self.log(f" - {fn}")
            if len(items) > 20:
                self.log("...")

            if excluded:
                self.log(self.tr("dashboard.preview.log.excluded_count", count=len(excluded)))
                for _, fn, reason in excluded[:20]:
                    self.log(f" [X] {fn}: {reason}")
                if len(excluded) > 20:
                    self.log("...")

            self.last_preview_scan_result = build_preview_scan_result(
                vals,
                items,
                excluded,
                time.time(),
                self.tr_map,
            )
            self.schedule_gui_callback(0, self.refresh_upload_operational_cards)
            if items == []:
                self.log(self.tr("dashboard.upload.log.no_targets"))
            self.log(self.tr("logs.preview.completed"))
        except Exception as error:
            self.log(self.tr("logs.preview.failed", error=error), level="ERROR")

    def on_pause(self):
        if not self.is_uploading:
            return
            
        if self.pause_event.is_set():
            # Pause it
            self.pause_event.clear()
            self.btn_pause.configure(text=self.tr("dashboard.button.resume"))
            self.log(self.tr("dashboard.log.pause_requested"))
        else:
            # Resume it
            self.pause_event.set()
            self.btn_pause.configure(text=self.tr("dashboard.button.pause"))
            self.log(self.tr("dashboard.log.resumed"))

    def _apply_upload_button_state(self, is_uploading: bool, pause_enabled: bool, pause_text: str, start_enabled: bool):
        self.is_uploading = is_uploading
        self.btn_pause.configure(state="normal" if pause_enabled else "disabled", text=pause_text)
        self.btn_preview.configure(
            state="normal" if (not is_uploading and not self.state_health_blocks_upload) else "disabled"
        )
        self.btn_start.configure(
            state="normal" if (start_enabled and not self.is_upload_preflight_blocked and not self.state_health_blocks_upload) else "disabled"
        )
        self.btn_retry_failed.configure(
            state="normal" if (not is_uploading and not self.state_health_blocks_upload and self.has_retryable_state) else "disabled"
        )

    def _apply_upload_dashboard_status(self, status_text: str, status_color: str):
        self.upload_dashboard_status_text = status_text
        self.upload_dashboard_status_color = status_color

    def _schedule_upload_dashboard_status(self, status_text: str, status_color: str):
        self.schedule_gui_callback(0, self._apply_upload_dashboard_status, status_text, status_color)

    def _schedule_upload_button_state(self, is_uploading: bool, pause_enabled: bool, pause_text: str, start_enabled: bool):
        self.schedule_gui_callback(0, self._apply_upload_button_state, is_uploading, pause_enabled, pause_text, start_enabled)

    def _show_blocked_upload_state(self, state_health_snapshot: core_state.StateHealthSnapshot) -> None:
        status_text, status_color, detail_lines = format_state_health_view(
            state_health_snapshot,
            lambda key, kwargs: self.tr(key, **kwargs),
        )
        self.state_health_blocks_upload = True
        if hasattr(self, "var_state_store_status"):
            self.var_state_store_status.set(status_text)
        if hasattr(self, "lbl_state_store_status") and self.lbl_state_store_status.winfo_exists():
            self.lbl_state_store_status.configure(text_color=status_color)
        if hasattr(self, "lbl_state_store_detail") and self.lbl_state_store_detail.winfo_exists():
            self.lbl_state_store_detail.configure(text="\n".join(detail_lines), text_color=status_color)
        self.refresh_upload_action_buttons()
        self.log(status_text, level="WARNING")
        for detail_line in detail_lines:
            self.log(detail_line, level="WARNING")
        messagebox.showwarning(status_text, "\n".join(detail_lines))
        self.refresh_upload_operational_cards()

    def _ensure_upload_state_ready(self) -> bool:
        state_health_snapshot = load_state_health_snapshot(False)
        if state_health_snapshot["can_start_upload"]:
            return True
        self._show_blocked_upload_state(state_health_snapshot)
        return False

    def _ensure_preview_state_ready(self) -> bool:
        state_health_snapshot = load_state_health_snapshot(False)
        if state_health_snapshot["can_start_upload"]:
            return True
        self._show_blocked_upload_state(state_health_snapshot)
        return False

    def resolve_upload_candidate_items(
        self,
        vals: dict[str, str],
        window_start: "date | None",
        window_end: "date",
        lag: int,
        include_today: bool,
        check_lock: bool,
        retry_failed_only: bool,
    ) -> tuple[list[tuple[str, str, str, str]], bool]:
        fingerprint = build_upload_selection_fingerprint(vals)
        if can_reuse_preview_scan_for_upload(
            self.last_preview_scan_result,
            fingerprint,
            include_today,
            retry_failed_only,
            time.time(),
        ):
            processed = load_processed()
            reusable_items = collect_reusable_preview_candidate_items(
                self.last_preview_scan_result,
                processed,
                window_start,
                window_end,
                lag,
                include_today,
                check_lock,
            )
            if reusable_items is not None:
                return list(reusable_items), True

        items = list_candidates(
            vals['PLC_DIR'],
            None,
            window_start,
            window_end,
            lag,
            include_today,
            check_lock,
        )
        return items, False

    def start_upload_with_values(self, vals: dict[str, str], retry_failed_only: bool) -> None:
        self.show_dashboard()
        if not self._ensure_upload_state_ready():
            return
        if self.is_data_task_running:
            self.show_warning(
                "dialog.data_task_running.title",
                "dashboard.upload.data_task_blocked",
            )
            return

        self.cfg = dict(vals)
        self.refresh_runtime_context_labels(self.cfg.get('SUPABASE_URL', ''), self.cfg.get('EDGE_FUNCTION_URL', ''))
        self.refresh_local_supabase_button()
        if not self.ensure_local_supabase_ready(self.tr("dashboard.upload.action.start")):
            return

        resolved_edge = compute_edge_url(self.cfg)
        if is_edge_url_origin_mismatch(self.cfg.get('EDGE_FUNCTION_URL', ''), self.cfg.get('SUPABASE_URL', '')):
            self.log(
                self.tr("dashboard.upload.log.edge_blocked"),
                level="WARNING",
            )
            self.show_error(
                "dashboard.upload.edge_blocked.title",
                "dashboard.upload.edge_blocked.body",
                resolved_edge=resolved_edge,
            )
            return

        self.processed_count = 0
        self.total_files = 0
        with self.progress_lock:
            self.active_progress.clear()

        self.pause_event.set()
        dashboard_status_text = self.tr("common.status.uploading")
        if retry_failed_only:
            dashboard_status_text = self.tr("dashboard.upload.status.retrying_failed")
        self._apply_upload_dashboard_status(dashboard_status_text, "#3B8ED0")
        self._apply_upload_button_state(True, True, self.tr("dashboard.button.pause"), False)
        threading.Thread(target=self._run_upload, args=(dict(self.cfg), retry_failed_only), daemon=True).start()

    def on_start(self):
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        self.start_upload_with_values(dict(self.cfg), False)

    def on_retry_failed(self):
        if not self._ensure_upload_state_ready():
            return
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        custom_date_start, custom_date_end = resolve_custom_range_texts(
            self.cfg.get('CUSTOM_DATE_START', ''),
            self.cfg.get('CUSTOM_DATE_END', ''),
            self.cfg.get('CUSTOM_DATE', ''),
        )
        window_start, window_end = compute_date_window(
            self.cfg['RANGE_MODE'],
            custom_date_start,
            custom_date_end,
        )
        try:
            lag = int(self.cfg.get('MTIME_LAG_MIN', '15'))
        except Exception:
            lag = 15
        dashboard_state_snapshot = load_upload_dashboard_state_snapshot()
        retry_items = self.collect_retryable_upload_items(
            list_candidates(
                self.cfg['PLC_DIR'],
                None,
                window_start,
                window_end,
                lag,
                self.cfg['RANGE_MODE'] == 'today',
                self.cfg.get('CHECK_LOCK', 'true') == 'true',
            ),
            dashboard_state_snapshot["resume"],
            dashboard_state_snapshot["failed_retry_set"],
        )
        if retry_items == []:
            self.refresh_upload_operational_cards()
            return
        self.start_upload_with_values(dict(self.cfg), True)

    def on_rerun_recent_success(self):
        if not self._ensure_upload_state_ready():
            return
        self.cfg, self.config_source, self.config_metadata = load_config_with_sources(None)
        self.recent_successful_upload_profile = load_recent_successful_upload_profile()
        if self.recent_successful_upload_profile is None:
            return
        profile_values = self.recent_successful_upload_profile.get("values")
        if not isinstance(profile_values, dict):
            return
        rerun_values = dict(self.cfg)
        rerun_values.update(
            {
                str(key): str(value)
                for key, value in profile_values.items()
            }
        )
        self.start_upload_with_values(rerun_values, False)

    def _run_upload(self, vals: dict[str, str], retry_failed_only: bool):
        final_message = ""
        final_level = "INFO"
        dashboard_status_text = self.tr("common.status.waiting")
        dashboard_status_color = "gray"

        try:
            ok_cfg, missing = validate_config(vals)
            if not ok_cfg:
                final_message = self.tr("dashboard.upload.log.config_error", missing=", ".join(missing))
                final_level = "ERROR"
                dashboard_status_text = self.tr("dashboard.upload.status.config_error")
                dashboard_status_color = "#E06C75"
                return

            anon = vals['SUPABASE_ANON_KEY'].strip()
            edge = compute_edge_url(vals)
            _, edge_state, host_mismatch = _build_edge_runtime_state(
                vals['SUPABASE_URL'],
                vals.get('EDGE_FUNCTION_URL', ''),
                self.tr_map,
            )
            custom_date_start, custom_date_end = resolve_custom_range_texts(
                vals.get('CUSTOM_DATE_START', ''),
                vals.get('CUSTOM_DATE_END', ''),
                vals.get('CUSTOM_DATE', ''),
            )
            window_start, window_end = compute_date_window(vals['RANGE_MODE'], custom_date_start, custom_date_end)
            include_today = (vals['RANGE_MODE'] == 'today')
            try:
                lag = int(vals.get('MTIME_LAG_MIN', '15'))
            except Exception:
                lag = 15
            check_lock = (vals.get('CHECK_LOCK', 'true') == 'true')
            enable_smart_sync = (vals.get('SMART_SYNC', 'true') == 'true')

            self.log(self.tr("dashboard.upload.log.config_path", config_path=self.config_source))
            self.log(_build_source_summary(self.config_metadata, self.tr_map))
            self.log(self.tr("dashboard.upload.log.edge_url", edge_url=edge))
            if is_edge_url_origin_mismatch(vals.get('EDGE_FUNCTION_URL', ''), vals.get('SUPABASE_URL', '')):
                self.log(self.tr("dashboard.upload.log.edge_warning"), level="WARNING")
            if host_mismatch:
                self.log(self.tr("dashboard.upload.log.edge_host_mismatch", edge_state=edge_state))
            if vals['RANGE_MODE'] == 'custom':
                self.log(
                    self.tr(
                        "dashboard.upload.log.range_custom",
                        custom_date_start=custom_date_start,
                        custom_date_end=custom_date_end,
                    )
                )
            else:
                self.log(
                    self.tr(
                        "dashboard.upload.log.range_default",
                        range_mode=self.get_range_mode_label(vals['RANGE_MODE']),
                        window_end=window_end.isoformat(),
                    )
                )

            # Start Upload는 현재 데이터 폴더만 사용합니다.
            pdir = vals['PLC_DIR']
            self.log(self.tr("dashboard.upload.log.scan_folder", folder_path=pdir))
            items, reused_preview = self.resolve_upload_candidate_items(
                vals,
                window_start,
                window_end,
                lag,
                include_today,
                check_lock,
                retry_failed_only,
            )
            if reused_preview:
                self.log(self.tr("dashboard.upload.log.reused_preview", file_count=len(items)))
            if retry_failed_only:
                dashboard_state_snapshot = load_upload_dashboard_state_snapshot()
                items = self.collect_retryable_upload_items(
                    items,
                    dashboard_state_snapshot["resume"],
                    dashboard_state_snapshot["failed_retry_set"],
                )
            self.log(self.tr("dashboard.upload.log.found_files", file_count=len(items)))
            if not items and os.path.exists(pdir):
                self.log(self.tr("dashboard.upload.log.folder_preview", file_names=os.listdir(pdir)[:5]))

            self.total_files = len(items)
            summary_total = len(items)

            if not items:
                final_message = self.tr("dashboard.upload.log.no_targets")
                final_level = "WARNING"
                dashboard_status_text = self.tr("dashboard.upload.status.no_targets")
                dashboard_status_color = "#E5C07B"
                return
            self.last_preview_scan_result = None

            count_lock = threading.Lock()
            core_upload = load_core_upload_module()
            session_items = [
                core_upload.build_upload_session_item(folder, fn, path, kind)
                for folder, fn, path, kind in items
            ]
            session_config = core_upload.UploadSessionConfig(
                edge_url=edge,
                anon_key=anon,
                batch_size=core_upload.DEFAULT_UPLOAD_BATCH_SIZE,
                chunk_size=core_upload.DEFAULT_UPLOAD_CHUNK_SIZE,
                progress_update_interval_seconds=core_upload.DEFAULT_PROGRESS_UPDATE_INTERVAL_SECONDS,
                enable_smart_sync=enable_smart_sync,
                max_workers=core_upload.DEFAULT_UPLOAD_MAX_WORKERS,
            )

            def on_file_progress(folder: str, filename: str, done: int, total: int) -> None:
                task_key = f"{folder}/{filename}"
                with self.progress_lock:
                    self.active_progress[task_key] = (done, total)

            def on_file_complete(folder: str, filename: str, ok: bool) -> None:
                task_key = f"{folder}/{filename}"
                with self.progress_lock:
                    if task_key in self.active_progress:
                        del self.active_progress[task_key]
                with count_lock:
                    self.processed_count += 1

            session_result = core_upload.run_upload_session(
                session_items,
                session_config,
                build_plc=build_records_plc,
                build_temp=None,
                get_resume_offset=get_resume_offset,
                set_resume_offset_fn=set_resume_offset,
                mark_file_completed_fn=mark_file_completed,
                record_file_failure_fn=record_file_failure,
                start_upload_run_fn=start_upload_run,
                finish_upload_run_fn=finish_upload_run,
                retry_failed_only=retry_failed_only,
                recent_successful_upload_profile={
                    "profile_name": kst_now().strftime("%Y-%m-%d %H:%M:%S"),
                    "applied_at": time.time(),
                    "values": self.build_upload_profile_values(vals),
                },
                runtime_config_values=self.build_upload_profile_values(vals),
                log=self.log,
                pause_event=self.pause_event,
                progress_cb=on_file_progress,
                file_complete_cb=on_file_complete,
            )

            if session_result.warning_messages:
                for warning_message in session_result.warning_messages:
                    self.log(warning_message, level="WARNING")

            success_count = session_result.success_count
            failure_count = session_result.failure_count
            self.log(
                self.tr(
                    "dashboard.upload.log.summary",
                    success_count=success_count,
                    failure_count=failure_count,
                    total_count=summary_total,
                )
            )
            if failure_count > 0:
                final_message = self.tr(
                    "dashboard.upload.log.partial_failure",
                    failure_count=failure_count,
                    total_count=session_result.total_count,
                )
                final_level = "WARNING"
                dashboard_status_text = self.tr("dashboard.upload.status.partial_failure")
                dashboard_status_color = "#E5C07B"
            else:
                if session_result.warning_messages:
                    final_message = self.tr("dashboard.upload.log.completed_with_warning")
                    final_level = "WARNING"
                    dashboard_status_text = self.tr("dashboard.upload.status.completed_with_warning")
                    dashboard_status_color = "#E5C07B"
                else:
                    final_message = self.tr("dashboard.upload.log.completed")
                    dashboard_status_text = self.tr("dashboard.upload.status.completed")
                    dashboard_status_color = "#2CC985"
        except Exception as error:
            final_message = self.tr("dashboard.upload.log.failed", error=error)
            final_level = "ERROR"
            dashboard_status_text = self.tr("dashboard.upload.status.failed")
            dashboard_status_color = "#E06C75"
            self.log(self.tr("dashboard.upload.log.unhandled_error", error=error), level="ERROR")
        finally:
            with self.progress_lock:
                self.active_progress.clear()
            self.pause_event.set()
            self._schedule_upload_button_state(False, False, self.tr("dashboard.button.pause"), True)
            self._schedule_upload_dashboard_status(dashboard_status_text, dashboard_status_color)
            self.schedule_gui_callback(0, self.refresh_upload_operational_cards)
            if final_message:
                self.log(final_message, level=final_level)


def list_candidates(
    plc_dir: str,
    temp_dir: str,
    window_start: "date | None",
    window_end: "date",
    lag_min: int,
    include_today: bool,
    check_lock: bool,
):
    # GUI uses quick candidate selection (no content check)
    return core_files.list_candidates(
        plc_dir,
        temp_dir,
        window_start,
        window_end,
        lag_min,
        include_today,
        check_lock,
        quick=True,
    )

if __name__ == '__main__':
    import signal

    app = App()

    def handle_sigint(signum, frame):
        print("\nClosing application...")
        app.close_application()

    signal.signal(signal.SIGINT, handle_sigint)
    app.mainloop()

