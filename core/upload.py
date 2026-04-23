from concurrent.futures import ThreadPoolExecutor, as_completed
from collections.abc import Sequence
from dataclasses import dataclass
import threading
from typing import Callable, Iterable, Optional

import httpx
import numpy as np
import pandas as pd

from .state import build_file_state_key, set_resume_offset


import time

DEFAULT_UPLOAD_BATCH_SIZE: int = 2000
DEFAULT_UPLOAD_CHUNK_SIZE: int = 10000
DEFAULT_UPLOAD_MAX_WORKERS: int = 4
DEFAULT_PROGRESS_UPDATE_INTERVAL_SECONDS: float = 0.25
DEFAULT_PROGRESS_MIN_DELTA_PERCENT: float = 1.0
DEFAULT_UPLOAD_HTTP_TIMEOUT_SECONDS: float = 30.0
DEFAULT_SMART_SYNC_HTTP_TIMEOUT_SECONDS: float = 10.0
DEFAULT_UPLOAD_MAX_RETRIES: int = 3


@dataclass(frozen=True)
class UploadSessionItem:
    folder: str
    filename: str
    path: str
    kind: str


@dataclass(frozen=True)
class UploadSessionConfig:
    edge_url: str
    anon_key: str
    batch_size: int
    chunk_size: int
    progress_update_interval_seconds: float
    enable_smart_sync: bool
    max_workers: int


@dataclass(frozen=True)
class UploadSessionResult:
    run_id: int | None
    total_count: int
    success_count: int
    failure_count: int
    failed_keys: tuple[str, ...]
    failed_items: tuple["FailedUploadItem", ...]
    warning_messages: tuple[str, ...]


@dataclass(frozen=True)
class LatestTimestampResolution:
    latest_timestamp: str | None
    warning_message: str | None


@dataclass(frozen=True)
class FailedUploadItem:
    folder: str
    filename: str
    path: str
    kind: str
    state_key: str
    resume_offset: int
    error_message: str


def create_upload_http_client() -> httpx.Client:
    return httpx.Client()


def _post_upload_batch(
    edge_url: str,
    headers: dict[str, str],
    batch: list[dict[str, object]],
    client: httpx.Client,
    log: Callable[[str], None],
) -> tuple[bool, int]:
    for attempt in range(DEFAULT_UPLOAD_MAX_RETRIES):
        try:
            response = client.post(
                edge_url,
                json=batch,
                headers=headers,
                timeout=DEFAULT_UPLOAD_HTTP_TIMEOUT_SECONDS,
            )
            if response.status_code >= 300:
                if response.status_code >= 500:
                    raise httpx.NetworkError(f"Server Error {response.status_code}")
                log(f"    업로드 실패 ({response.status_code}): {response.text[:200]}")
                return False, 0

            try:
                inserted = int(response.json().get("inserted", 0))
            except Exception:
                inserted = 0
            return True, inserted
        except (httpx.NetworkError, httpx.TimeoutException) as error:
            if attempt < DEFAULT_UPLOAD_MAX_RETRIES - 1:
                wait_time = 2 ** attempt
                log(
                    f"    네트워크 오류({error}). {wait_time}초 후 재시도 "
                    f"({attempt + 1}/{DEFAULT_UPLOAD_MAX_RETRIES})..."
                )
                time.sleep(wait_time)
                continue
            log(f"    업로드 실패 (최대 재시도 초과): {error}")
            return False, 0
        except Exception as error:
            log(f"    업로드 예외: {error}")
            return False, 0
    return False, 0


def _build_upload_headers(anon_key: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {anon_key}",
        "apikey": anon_key,
        "Content-Type": "application/json",
    }


def _load_chunks(
    builder: Callable[[str, str], pd.DataFrame],
    path: str,
    filename: str,
    chunk_size: int,
) -> Iterable[pd.DataFrame]:
    try:
        data_source = builder(path, filename, chunksize=chunk_size)
    except TypeError:
        data_source = builder(path, filename)

    if isinstance(data_source, pd.DataFrame):
        if data_source.empty:
            return ()
        return (data_source,)

    return data_source


def _get_total_row_count(data_source: Iterable[pd.DataFrame]) -> int | None:
    if isinstance(data_source, Sequence):
        return sum(len(df_chunk) for df_chunk in data_source)
    return None


def _filter_chunk_by_latest_timestamp(
    df_chunk: pd.DataFrame,
    latest_timestamp: str,
) -> tuple[pd.DataFrame, int]:
    source_row_count = len(df_chunk)
    if "timestamp" not in df_chunk.columns:
        return df_chunk, source_row_count
    return df_chunk[df_chunk["timestamp"] > latest_timestamp], source_row_count


def _apply_resume_offset(
    df_chunk: pd.DataFrame,
    enable_smart_sync: bool,
    start_idx: int,
    current_global_idx: int,
) -> tuple[pd.DataFrame, int]:
    chunk_len = len(df_chunk)
    if enable_smart_sync or start_idx <= 0:
        return df_chunk, chunk_len

    if current_global_idx + chunk_len <= start_idx:
        return df_chunk.iloc[0:0], chunk_len
    if current_global_idx < start_idx:
        offset = start_idx - current_global_idx
        return df_chunk.iloc[offset:], chunk_len
    return df_chunk, chunk_len


def _should_report_progress(
    total_rows: int | None,
    current_global_idx: int,
    last_progress_report_time: float,
    last_progress_reported_rows: int,
    current_time: float,
    progress_update_interval_seconds: float,
) -> bool:
    if current_global_idx <= 0:
        return True
    if total_rows is None or total_rows <= 0:
        return current_time - last_progress_report_time >= progress_update_interval_seconds

    progress_rows = min(current_global_idx, total_rows)
    progress_percent_delta = ((progress_rows - last_progress_reported_rows) / total_rows) * 100.0
    return (
        progress_rows >= total_rows
        or current_time - last_progress_report_time >= progress_update_interval_seconds
        or progress_percent_delta >= DEFAULT_PROGRESS_MIN_DELTA_PERCENT
    )

def upload_via_edge(
    edge_url: str,
    anon_key: str,
    df: pd.DataFrame,
    client: httpx.Client,
    *,
    log: Callable[[str], None],
    resume_key: Optional[str] = None,
    start_index: int = 0,
    batch_size: int = DEFAULT_UPLOAD_BATCH_SIZE,
    progress_cb: Callable[[int, int], None] | None = None,
    pause_event=None,
    silent: bool = False,
) -> bool:
    """
    Common Edge Function uploader with resume support.
    Optimized for memory usage (batch-wise conversion) and reliability (retry logic).
    """
    if df.empty:
        log("    - 유효한 데이터가 없습니다(빈 데이터프레임)")
        return True

    # Supabase local gateway expects both Authorization and apikey when using sb_secret keys.
    headers = _build_upload_headers(anon_key)

    total = len(df)
    start = max(0, min(start_index, total))

    if start > 0:
        log(f"    - 이전 업로드 위치에서 재개 {start}/{total}")
        if progress_cb:
            try:
                progress_cb(start, total)
            except Exception:
                pass

    total_inserted = 0
    last_progress_report_time = 0.0
    last_progress_reported_rows = start
    for i in range(start, total, batch_size):
        if pause_event:
            pause_event.wait()
            
        # Memory Optimization: Slice DataFrame first, then convert to dict
        # This avoids creating a huge list of dicts for the entire file
        batch_df = df.iloc[i : i + batch_size]
        batch = batch_df.replace({np.nan: None}).to_dict(orient="records")
        
        ok, inserted = _post_upload_batch(edge_url, headers, batch, client, log)
        if not ok:
            return False
        total_inserted += inserted

        current_processed = min(i + len(batch), total)
        if resume_key:
            set_resume_offset(resume_key, current_processed)
        if progress_cb:
            current_time = time.monotonic()
            if _should_report_progress(
                total,
                current_processed,
                last_progress_report_time,
                last_progress_reported_rows,
                current_time,
                DEFAULT_PROGRESS_UPDATE_INTERVAL_SECONDS,
            ):
                try:
                    progress_cb(current_processed, total)
                except Exception:
                    pass
                last_progress_report_time = current_time
                last_progress_reported_rows = current_processed

    if not silent:
        log(f"    {total}건 전송 완료(실제 삽입 {total_inserted}건)")
    return True


def get_latest_timestamp(
    edge_url: str,
    anon_key: str,
    device_id: str,
    log: Callable[[str], None],
    client: httpx.Client,
) -> LatestTimestampResolution:
    """
    Query the Edge Function for the latest timestamp of a given device.
    Returns ISO string or None.
    """
    headers = _build_upload_headers(anon_key)
    params = {"device_id": device_id}
    
    try:
        r = client.get(
            edge_url,
            headers=headers,
            params=params,
            timeout=DEFAULT_SMART_SYNC_HTTP_TIMEOUT_SECONDS,
        )
        if r.status_code == 200:
            data = r.json()
            return LatestTimestampResolution(
                latest_timestamp=data.get("latest_timestamp"),
                warning_message=None,
            )
        return LatestTimestampResolution(
            latest_timestamp=None,
            warning_message=(
                f"Smart Sync 최신 시각 조회 실패(device_id={device_id}, "
                f"status={r.status_code}). 전체 업로드로 진행합니다."
            ),
        )
    except Exception as error:
        return LatestTimestampResolution(
            latest_timestamp=None,
            warning_message=(
                f"Smart Sync 최신 시각 조회 예외(device_id={device_id}): "
                f"{error}. 전체 업로드로 진행합니다."
            ),
        )


def upload_item(
    edge_url: str,
    anon_key: str,
    folder: str,
    filename: str,
    path: str,
    kind: str,
    *,
    build_plc: Callable[[str, str], pd.DataFrame],
    build_temp: Callable[[str, str], pd.DataFrame],
    get_resume_offset: Callable[[str], int],
    set_resume_offset_fn: Callable[[str, int], None],
    mark_file_completed_fn: Callable[[str, str, str, int | None], None],
    record_file_failure_fn: Callable[[str, str, str, int, str, int | None], None],
    log: Callable[[str], None],
    batch_size: int,
    chunk_size: int,
    progress_cb: Callable[[int, int], None] | None = None,
    progress_update_interval_seconds: float,
    enable_smart_sync: bool,
    resolve_latest_timestamp_fn: Callable[[str], str | None] | None,
    pause_event=None,
    latest_timestamp: Optional[str] = None,
    run_id: int | None = None,
) -> bool:
    """
    Upload a single PLC or temperature file with resume support.
    Includes Smart Sync optimization.
    """
    key = build_file_state_key(folder, filename, path)
    start_idx = get_resume_offset(key)

    try:
        latest_ts = latest_timestamp
        if enable_smart_sync and latest_ts is None and resolve_latest_timestamp_fn is not None:
            latest_ts = resolve_latest_timestamp_fn(_device_id_for_kind(kind))
        if enable_smart_sync and latest_ts:
            log(f"- Upload {key}: Smart Sync 활성화 (서버 최신: {latest_ts})")

        builder = build_plc if kind == "plc" else build_temp
        data_source = _load_chunks(builder, path, filename, chunk_size)
        file_total_rows = _get_total_row_count(data_source)

        current_global_idx = 0
        source_rows_seen = 0
        progress_rows_after_resume = 0
        uploaded_any = False
        last_progress_report_time = 0.0
        last_progress_reported_rows = 0

        def notify_progress(processed_rows: int, total_rows: int | None) -> None:
            nonlocal last_progress_report_time, last_progress_reported_rows
            if progress_cb is None:
                return
            current_time = time.monotonic()
            if not _should_report_progress(
                total_rows,
                processed_rows,
                last_progress_report_time,
                last_progress_reported_rows,
                current_time,
                progress_update_interval_seconds,
            ):
                return
            try:
                progress_cb(processed_rows, 0 if total_rows is None else total_rows)
            except Exception:
                return
            last_progress_report_time = current_time
            last_progress_reported_rows = processed_rows

        if start_idx > 0:
            notify_progress(start_idx, file_total_rows)

        with create_upload_http_client() as http_client:
            for df_chunk in data_source:
                if df_chunk.empty:
                    continue

                chunk_source_row_count = len(df_chunk)
                chunk_start_source_rows_seen = source_rows_seen
                resume_rows_remaining = max(0, start_idx - chunk_start_source_rows_seen)
                smart_sync_rows_after_resume = 0

                if latest_ts and "timestamp" in df_chunk.columns:
                    df_chunk, source_row_count = _filter_chunk_by_latest_timestamp(df_chunk, latest_ts)
                    skipped_row_count = source_row_count - len(df_chunk)
                    smart_sync_rows_after_resume = max(0, skipped_row_count - resume_rows_remaining)
                    if skipped_row_count > 0 and not df_chunk.empty:
                        current_global_idx += skipped_row_count
                        set_resume_offset_fn(
                            key,
                            current_global_idx,
                        )
                    if df_chunk.empty:
                        current_global_idx += source_row_count
                        source_rows_seen += chunk_source_row_count
                        progress_rows_after_resume += smart_sync_rows_after_resume
                        if smart_sync_rows_after_resume > 0:
                            notify_progress(start_idx + progress_rows_after_resume, file_total_rows)
                        continue

                df_chunk, _consumed_rows = _apply_resume_offset(
                    df_chunk,
                    enable_smart_sync,
                    start_idx,
                    current_global_idx,
                )
                if df_chunk.empty:
                    current_global_idx += _consumed_rows
                    source_rows_seen += chunk_source_row_count
                    continue

                chunk_progress_base = progress_rows_after_resume + smart_sync_rows_after_resume

                def report_chunk_progress(done_in_chunk: int, total_in_chunk: int) -> None:
                    _ = total_in_chunk
                    notify_progress(
                        start_idx + chunk_progress_base + done_in_chunk,
                        file_total_rows,
                    )

                ok = upload_via_edge(
                    edge_url,
                    anon_key,
                    df_chunk,
                    http_client,
                    log=log,
                    resume_key=None,
                    start_index=0,
                    batch_size=batch_size,
                    progress_cb=report_chunk_progress,
                    pause_event=pause_event,
                    silent=True,
                )
                if not ok:
                    log(f"    - 청크 업로드 실패 (구간: {current_global_idx}~)")
                    failure_offset = max(get_resume_offset(key), 1)
                    record_file_failure_fn(folder, filename, path, failure_offset, "업로드 실패", run_id)
                    return False

                uploaded_any = True
                progress_rows_after_resume += smart_sync_rows_after_resume + len(df_chunk)
                current_global_idx += len(df_chunk)
                source_rows_seen += chunk_source_row_count
                notify_progress(start_idx + progress_rows_after_resume, file_total_rows)
                if not enable_smart_sync:
                    set_resume_offset_fn(key, current_global_idx)

        if not uploaded_any:
            log(f"- Upload {key}: 데이터 없음 또는 모두 최신 상태")
            set_resume_offset_fn(key, 0)
            return True

        log(f"- Upload {key}: 완료")
        if progress_cb is not None:
            try:
                progress_cb(
                    start_idx + progress_rows_after_resume,
                    start_idx + progress_rows_after_resume if file_total_rows is None else file_total_rows,
                )
            except Exception:
                pass

        mark_file_completed_fn(folder, filename, path, run_id)
        return True
    except Exception as error:
        failure_offset = max(get_resume_offset(key), 1)
        record_file_failure_fn(folder, filename, path, failure_offset, str(error), run_id)
        log(f"    - 업로드 예외로 실패 상태 기록: {error}")
        return False


def build_upload_session_item(folder: str, filename: str, path: str, kind: str) -> UploadSessionItem:
    return UploadSessionItem(folder=folder, filename=filename, path=path, kind=kind)


def _device_id_for_kind(kind: str) -> str:
    if kind == "plc":
        return "extruder_plc"
    return "spot_temperature_sensor"


def _notify_progress(
    progress_cb: Callable[[str, str, int, int], None] | None,
    folder: str,
    filename: str,
    done: int,
    total: int,
) -> None:
    if progress_cb is None:
        return
    try:
        progress_cb(folder, filename, done, total)
    except Exception:
        return


def _notify_file_complete(
    file_complete_cb: Callable[[str, str, bool], None] | None,
    folder: str,
    filename: str,
    ok: bool,
) -> None:
    if file_complete_cb is None:
        return
    try:
        file_complete_cb(folder, filename, ok)
    except Exception:
        return


def _resolve_latest_timestamp_cached(
    config: UploadSessionConfig,
    kind: str,
    cache: dict[str, LatestTimestampResolution],
    cache_lock: threading.Lock,
    log: Callable[[str], None],
) -> LatestTimestampResolution:
    if not config.enable_smart_sync:
        return LatestTimestampResolution(latest_timestamp=None, warning_message=None)
    device_id = _device_id_for_kind(kind)
    cache_key = f"{config.edge_url}|{device_id}"
    with cache_lock:
        if cache_key not in cache:
            with create_upload_http_client() as http_client:
                cache[cache_key] = get_latest_timestamp(
                    config.edge_url,
                    config.anon_key,
                    device_id,
                    log,
                    http_client,
                )
        return cache[cache_key]


def run_upload_session(
    items: list[UploadSessionItem],
    config: UploadSessionConfig,
    build_plc: Callable[[str, str], pd.DataFrame],
    build_temp: Callable[[str, str], pd.DataFrame],
    get_resume_offset: Callable[[str], int],
    set_resume_offset_fn: Callable[[str, int], None],
    mark_file_completed_fn: Callable[[str, str, str, int | None], None],
    record_file_failure_fn: Callable[[str, str, str, int, str, int | None], None],
    start_upload_run_fn: Callable[[int, bool, dict[str, str]], int],
    finish_upload_run_fn: Callable[[int, int, int, int, tuple[str, ...], dict[str, object] | None], None],
    retry_failed_only: bool,
    recent_successful_upload_profile: dict[str, object] | None,
    runtime_config_values: dict[str, str],
    log: Callable[[str], None],
    pause_event,
    progress_cb: Callable[[str, str, int, int], None] | None,
    file_complete_cb: Callable[[str, str, bool], None] | None,
) -> UploadSessionResult:
    latest_timestamp_cache: dict[str, LatestTimestampResolution] = {}
    latest_timestamp_lock = threading.Lock()
    failed_keys: list[str] = []
    failed_items: list[FailedUploadItem] = []
    smart_sync_warnings: list[str] = []
    smart_sync_warning_keys: set[str] = set()
    smart_sync_warning_lock = threading.Lock()
    run_id = start_upload_run_fn(len(items), retry_failed_only, runtime_config_values)

    def upload_single(item: UploadSessionItem) -> tuple[bool, str, FailedUploadItem | None]:
        log(f"- 업로드 {item.folder}/{item.filename}")
        latest_timestamp_resolution = _resolve_latest_timestamp_cached(
            config,
            item.kind,
            latest_timestamp_cache,
            latest_timestamp_lock,
            log,
        )
        warning_message = latest_timestamp_resolution.warning_message
        if warning_message is not None:
            warning_key = f"{config.edge_url}|{item.kind}"
            with smart_sync_warning_lock:
                if warning_key not in smart_sync_warning_keys:
                    smart_sync_warning_keys.add(warning_key)
                    smart_sync_warnings.append(warning_message)
                    log(f"경고: {warning_message}")

        def per_file_progress(done: int, total: int) -> None:
            _notify_progress(progress_cb, item.folder, item.filename, done, total)

        ok = False
        try:
            ok = upload_item(
                config.edge_url,
                config.anon_key,
                item.folder,
                item.filename,
                item.path,
                item.kind,
                build_plc=build_plc,
                build_temp=build_temp,
                get_resume_offset=get_resume_offset,
                set_resume_offset_fn=set_resume_offset_fn,
                mark_file_completed_fn=mark_file_completed_fn,
                record_file_failure_fn=record_file_failure_fn,
                log=log,
                batch_size=config.batch_size,
                chunk_size=config.chunk_size,
                progress_cb=per_file_progress,
                progress_update_interval_seconds=config.progress_update_interval_seconds,
                enable_smart_sync=config.enable_smart_sync,
                resolve_latest_timestamp_fn=None,
                pause_event=pause_event,
                latest_timestamp=latest_timestamp_resolution.latest_timestamp,
                run_id=run_id,
            )
            if ok:
                return True, f"{item.folder}/{item.filename}", None

            state_key = build_file_state_key(item.folder, item.filename, item.path)
            failed_item = FailedUploadItem(
                folder=item.folder,
                filename=item.filename,
                path=item.path,
                kind=item.kind,
                state_key=state_key,
                resume_offset=get_resume_offset(state_key),
                error_message="업로드 실패",
            )
            return False, f"{item.folder}/{item.filename}", failed_item
        finally:
            _notify_file_complete(file_complete_cb, item.folder, item.filename, ok)

    success_count = 0
    failure_count = 0
    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        future_to_item = {executor.submit(upload_single, item): item for item in items}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            key = f"{item.folder}/{item.filename}"
            try:
                ok, result_key, failed_item = future.result()
                if ok:
                    success_count += 1
                else:
                    failure_count += 1
                    failed_keys.append(result_key)
                    if failed_item is not None:
                        failed_items.append(failed_item)
                    log(f"업로드 실패: {result_key}")
            except Exception as error:
                failure_count += 1
                failed_keys.append(key)
                state_key = build_file_state_key(item.folder, item.filename, item.path)
                failure_offset = max(get_resume_offset(state_key), 1)
                record_file_failure_fn(item.folder, item.filename, item.path, failure_offset, str(error), run_id)
                failed_items.append(
                    FailedUploadItem(
                        folder=item.folder,
                        filename=item.filename,
                        path=item.path,
                        kind=item.kind,
                        state_key=state_key,
                        resume_offset=failure_offset,
                        error_message=str(error),
                    )
                )
                log(f"업로드 중 예외 발생: {key}: {error}")
    session_result = UploadSessionResult(
        run_id=run_id,
        total_count=len(items),
        success_count=success_count,
        failure_count=failure_count,
        failed_keys=tuple(failed_keys),
        failed_items=tuple(failed_items),
        warning_messages=tuple(smart_sync_warnings),
    )
    finish_upload_run_fn(
        run_id,
        session_result.total_count,
        session_result.success_count,
        session_result.failure_count,
        session_result.warning_messages,
        recent_successful_upload_profile if session_result.failure_count == 0 else None,
    )
    return session_result
def upload_work_log_data(
    supabase_url: str,
    anon_key: str,
    df: pd.DataFrame,
    log: Callable[[str], None]
) -> bool:
    """
    Upload Work Log DataFrame to 'tb_work_log' table via Supabase REST API.
    Includes Smart Filtering to prevent duplicates based on 'start_time'.
    """
    if df.empty:
        log("데이터가 없습니다.")
        return True

    # 1. Prepare Data
    df_upload = df.copy()
    
    # Ensure timestamps are ISO strings
    for col in ["start_time", "end_time"]:
        if col in df_upload.columns:
            df_upload[col] = df_upload[col].apply(
                lambda x: x.isoformat() if pd.notnull(x) and not isinstance(x, str) else x
            )

    # 2. Smart Filter: Check for duplicates (Composite Key)
    # User requested: start_time + machine_id + die_number + production_qty + production_weight + productivity
    try:
        machine_ids = df_upload['machine_id'].unique()
        if len(machine_ids) > 0:
            def quote_in_value(val: object) -> str | None:
                if val is None or pd.isna(val):
                    return None
                s = str(val).replace('"', '\\"')
                return f"\"{s}\""

            quoted_ids = [q for q in (quote_in_value(v) for v in machine_ids) if q]
            m_ids_str = ",".join(quoted_ids)
            query_url = f"{supabase_url}/rest/v1/tb_work_log"
            select_cols = [
                "start_time",
                "machine_id",
                "die_number",
                "production_qty",
                "production_weight",
                "productivity",
                "lot",
                "temper_type",
                "quenching_temp",
                "stretching",
                "total_weight",
                "ram",
                "product_length",
                "actual_unit_weight",
                "defect_bubble",
                "defect_tearing",
                "defect_white_black_line",
                "defect_oxide",
                "defect_scratch",
                "defect_bend",
                "defect_dimension",
                "defect_line",
                "defect_etc",
                "start_cut",
                "end_cut",
                "op_note",
            ]
            params = {
                "select": ",".join(select_cols),
                "machine_id": f"in.({m_ids_str})"
            }
            headers = {
                "apikey": anon_key,
                "Authorization": f"Bearer {anon_key}",
            }
            
            r = httpx.get(query_url, params=params, headers=headers, timeout=10.0)
            if r.status_code == 200:
                existing_data = r.json()
                existing_signatures = set()
                
                def norm(v):
                    if pd.isna(v) or v is None:
                        return None
                    try:
                        f = float(v)
                        return int(f) if f.is_integer() else f
                    except Exception:
                        return v

                def norm_text(v):
                    if v is None or pd.isna(v):
                        return None
                    s = str(v).strip()
                    return s if s else None

                for item in existing_data:
                    ts = item.get('start_time')
                    if ts:
                        try:
                            # Normalize DB time to UTC
                            dt = pd.to_datetime(ts).tz_convert("UTC")
                            
                            # Build signature tuple
                            sig = (
                                norm_text(item.get('machine_id')),
                                dt,
                                norm(item.get('die_number')),
                                norm(item.get('production_qty')),
                                norm(item.get('production_weight')),
                                norm(item.get('productivity')),
                                norm_text(item.get('lot')),
                                norm_text(item.get('temper_type')),
                                norm(item.get('quenching_temp')),
                                norm(item.get('stretching')),
                                norm(item.get('total_weight')),
                                norm(item.get('ram')),
                                norm(item.get('product_length')),
                                norm(item.get('actual_unit_weight')),
                                norm(item.get('defect_bubble')),
                                norm(item.get('defect_tearing')),
                                norm(item.get('defect_white_black_line')),
                                norm(item.get('defect_oxide')),
                                norm(item.get('defect_scratch')),
                                norm(item.get('defect_bend')),
                                norm(item.get('defect_dimension')),
                                norm(item.get('defect_line')),
                                norm(item.get('defect_etc')),
                                norm(item.get('start_cut')),
                                norm(item.get('end_cut')),
                                norm_text(item.get('op_note')),
                            )
                            existing_signatures.add(sig)
                        except Exception:
                            pass

                original_len = len(df_upload)
                
                def is_new(row):
                    t = row.get('start_time')
                    if not t: return True
                    
                    try:
                        # Normalize Row time to UTC
                        # row['start_time'] is already ISO string from step 1?
                        # Wait, step 1 converted to ISO string. pd.to_datetime works on ISO strings.
                        dt = pd.to_datetime(t).tz_convert("UTC")
                        
                        sig = (
                            norm_text(row.get('machine_id')),
                            dt,
                            norm(row.get('die_number')),
                            norm(row.get('production_qty')),
                            norm(row.get('production_weight')),
                            norm(row.get('productivity')),
                            norm_text(row.get('lot')),
                            norm_text(row.get('temper_type')),
                            norm(row.get('quenching_temp')),
                            norm(row.get('stretching')),
                            norm(row.get('total_weight')),
                            norm(row.get('ram')),
                            norm(row.get('product_length')),
                            norm(row.get('actual_unit_weight')),
                            norm(row.get('defect_bubble')),
                            norm(row.get('defect_tearing')),
                            norm(row.get('defect_white_black_line')),
                            norm(row.get('defect_oxide')),
                            norm(row.get('defect_scratch')),
                            norm(row.get('defect_bend')),
                            norm(row.get('defect_dimension')),
                            norm(row.get('defect_line')),
                            norm(row.get('defect_etc')),
                            norm(row.get('start_cut')),
                            norm(row.get('end_cut')),
                            norm_text(row.get('op_note')),
                        )
                        
                        if sig in existing_signatures:
                            return False
                    except Exception:
                        pass
                    
                    return True

                df_upload = df_upload[df_upload.apply(is_new, axis=1)]
                filtered_len = len(df_upload)
                
                if original_len != filtered_len:
                    log(f"중복 제거: {original_len - filtered_len}건 (남은 데이터: {filtered_len}건)")
            else:
                # Fail-Close
                log(f"중복 체크 실패 (서버 오류 {r.status_code}): {r.text[:500]}")
                log("데이터 안전을 위해 업로드를 중단합니다.")
                return False

    except Exception as e:
        # Fail-Close
        log(f"중복 체크 중 치명적 오류: {e}. 업로드를 중단합니다.")
        return False

    if df_upload.empty:
        log("업로드할 새로운 데이터가 없습니다.")
        return True

    # 3. Upload
    table_url = f"{supabase_url}/rest/v1/tb_work_log"
    headers = {
        "apikey": anon_key,
        "Authorization": f"Bearer {anon_key}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal",
    }
    
    # Convert to dict, handling NaN/pd.NA correctly
    # 1. Convert to object to allow None in all columns
    # 2. Replace pd.NA and np.nan with None
    df_clean = df_upload.astype(object).where(pd.notnull(df_upload), None)
    records = df_clean.to_dict(orient="records")

    try:
        r = httpx.post(table_url, json=records, headers=headers, timeout=30.0)
        if r.status_code >= 300:
            log(f"업로드 실패 ({r.status_code}): {r.text}")
            return False
        
        log(f"업로드 성공: {len(records)}건")
        return True
    except Exception as e:
        log(f"업로드 중 오류 발생: {e}")
        return False
