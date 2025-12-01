from typing import Callable, Optional

import httpx
import numpy as np
import pandas as pd

from .state import set_resume_offset


import time

def upload_via_edge(
    edge_url: str,
    anon_key: str,
    df: pd.DataFrame,
    *,
    log: Callable[[str], None],
    resume_key: Optional[str] = None,
    start_index: int = 0,
    batch_size: int = 500,
    progress_cb=None,
) -> bool:
    """
    Common Edge Function uploader with resume support.
    Optimized for memory usage (batch-wise conversion) and reliability (retry logic).
    """
    if df.empty:
        log("    - 유효 데이터 없음(건너뜀)")
        return True

    # Supabase local gateway expects both Authorization and apikey when using sb_secret keys.
    headers = {
        "Authorization": f"Bearer {anon_key}",
        "apikey": anon_key,
        "Content-Type": "application/json",
    }
    
    total = len(df)
    start = max(0, min(start_index, total))

    if start > 0:
        log(f"    - 일부 건 재개 {start}/{total}")
        if progress_cb:
            try:
                progress_cb(start, total)
            except Exception:
                pass

    total_inserted = 0
    for i in range(start, total, batch_size):
        # Memory Optimization: Slice DataFrame first, then convert to dict
        # This avoids creating a huge list of dicts for the entire file
        batch_df = df.iloc[i : i + batch_size]
        batch = batch_df.replace({np.nan: None}).to_dict(orient="records")
        
        # Retry Logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                r = httpx.post(edge_url, json=batch, headers=headers, timeout=30.0)
                if r.status_code >= 300:
                    # Server error (5xx) -> Retry
                    if r.status_code >= 500:
                        raise httpx.NetworkError(f"Server Error {r.status_code}")
                    # Client error (4xx) -> Fail immediately
                    log(f"    업로드 실패 ({r.status_code}): {r.text[:200]}")
                    return False
                
                # Success - Parse inserted count
                try:
                    resp_json = r.json()
                    inserted = int(resp_json.get("inserted", 0))
                    total_inserted += inserted
                except Exception:
                    pass # Fallback if response format is unexpected
                break
            except (httpx.NetworkError, httpx.TimeoutException) as e:
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt # 1s, 2s, 4s
                    log(f"    네트워크 오류({e}). {wait_time}초 후 재시도 ({attempt+1}/{max_retries})...")
                    time.sleep(wait_time)
                else:
                    log(f"    업로드 실패 (최대 재시도 초과): {e}")
                    return False
            except Exception as e:
                log(f"    업로드 예외: {e}")
                return False

        current_processed = min(i + len(batch), total)
        if resume_key:
            set_resume_offset(resume_key, current_processed)
        if progress_cb:
            try:
                progress_cb(current_processed, total)
            except Exception:
                pass

    log(f"    {total}건 전송 완료 (실제 저장: {total_inserted}건)")
    return True


def get_latest_timestamp(edge_url: str, anon_key: str, device_id: str, log: Callable[[str], None]) -> Optional[str]:
    """
    Query the Edge Function for the latest timestamp of a given device.
    Returns ISO string or None.
    """
    headers = {
        "Authorization": f"Bearer {anon_key}",
        "apikey": anon_key,
    }
    params = {"device_id": device_id}
    
    try:
        # Use a separate timeout for this lightweight query
        r = httpx.get(edge_url, headers=headers, params=params, timeout=10.0)
        if r.status_code == 200:
            data = r.json()
            return data.get("latest_timestamp")
        else:
            # If 404 or 500, just return None to proceed with full upload (safe fallback)
            return None
    except Exception:
        return None


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
    log_processed_fn: Callable[[str, str], None],
    log: Callable[[str], None],
    batch_size: int = 500,
    progress_cb=None,
    enable_smart_sync: bool = True,
) -> bool:
    """
    Upload a single PLC or temperature file with resume support.
    Includes Smart Sync optimization.
    """
    key = f"{folder}/{filename}"
    start_idx = get_resume_offset(key)
    
    # 1. Build DataFrame
    df = build_plc(path, filename) if kind == "plc" else build_temp(path, filename)
    if df.empty:
        log(f"- Upload {key}: 데이터 없음")
        log_processed_fn(folder, filename)
        return True

    # 2. Smart Sync: Check latest timestamp on server
    # Prioritize server state over local resume offset for better efficiency
    if enable_smart_sync and "device_id" in df.columns and "timestamp" in df.columns:
        device_id = df["device_id"].iloc[0]
        latest_ts = get_latest_timestamp(edge_url, anon_key, device_id, log)
        
        if latest_ts:
            # Filter rows strictly after latest_ts
            original_len = len(df)
            df = df[df["timestamp"] > latest_ts]
            filtered_len = len(df)
            
            if filtered_len == 0:
                log(f"- Upload {key}: Smart Sync 건너뜀 (서버 최신: {latest_ts})")
                log_processed_fn(folder, filename)
                return True
            elif filtered_len < original_len:
                log(f"- Upload {key}: Smart Sync 적용 (서버 기준 필터링: {original_len} -> {filtered_len}건)")
                # Since we filtered the data, the original resume offset is no longer valid/needed
                # We start from the beginning of this NEW filtered dataframe
                start_idx = 0

    log(f"- Upload {key} (resume {start_idx})")
    
    ok = upload_via_edge(
        edge_url,
        anon_key,
        df,
        log=log,
        resume_key=key,
        start_index=start_idx,
        batch_size=batch_size,
        progress_cb=progress_cb,
    )
    if ok:
        log_processed_fn(folder, filename)
        set_resume_offset_fn(key, 0)
    return ok
