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
    pause_event=None,
    silent: bool = False,
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
        if pause_event:
            pause_event.wait()
            
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

    if not silent:
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
    pause_event=None,
) -> bool:
    """
    Upload a single PLC or temperature file with resume support.
    Includes Smart Sync optimization.
    """
    # 1. Prepare for Chunked Processing
    key = f"{folder}/{filename}"
    start_idx = get_resume_offset(key)
    CHUNK_SIZE = 10000
    
    # Estimate total rows for progress (optional, but good for UX)
    total_rows = 0
    try:
        # Fast line count
        with open(path, 'rb') as f:
            total_rows = sum(1 for _ in f) - 1 # minus header
    except Exception:
        pass
    
    # 2. Smart Sync: Get latest timestamp once
    latest_ts = None
    if enable_smart_sync:
        # We need to know device_id. 
        # For PLC, it is 'extruder_plc'. For Temp, 'spot_temperature_sensor'.
        # This is hardcoded in transform.py, so we can use it here.
        dev_id = "extruder_plc" if kind == "plc" else "spot_temperature_sensor"
        latest_ts = get_latest_timestamp(edge_url, anon_key, dev_id, log)
        if latest_ts:
            log(f"- Upload {key}: Smart Sync 활성화 (서버 최신: {latest_ts})")

    # 3. Process Chunks
    # build_... functions now support chunksize
    builder = build_plc if kind == "plc" else build_temp
    
    # We need to handle the case where builder returns a DF (error/empty) or Generator
    # But we updated transform.py to always return generator if chunksize is set?
    # Actually transform.py returns generator if chunksize is set, else DF.
    # But if error, it returns empty generator or empty DF.
    
    try:
        data_source = builder(path, filename, chunksize=CHUNK_SIZE)
    except TypeError:
        # Fallback if builder doesn't support chunksize (old version?)
        data_source = builder(path, filename)
        
    if isinstance(data_source, pd.DataFrame):
        # It returned a DataFrame (maybe empty or error)
        data_source = [data_source] if not data_source.empty else []

    current_global_idx = 0
    uploaded_any = False
    
    for df_chunk in data_source:
        if df_chunk.empty:
            continue
            
        # Smart Sync Filter
        if latest_ts and "timestamp" in df_chunk.columns:
            df_chunk = df_chunk[df_chunk["timestamp"] > latest_ts]
            if df_chunk.empty:
                current_global_idx += CHUNK_SIZE # Approximate advance
                continue

        # Resume Logic (Row based)
        # If we are strictly using Smart Sync, resume offset is less important, 
        # but if Smart Sync is OFF, we use start_idx.
        # However, mixing Chunking + Row Index Resume is complex if we filter rows.
        # Simplified: If Smart Sync is ON, we ignore local resume offset (trust server).
        # If Smart Sync is OFF, we use local resume offset.
        
        chunk_len = len(df_chunk)
        
        if not enable_smart_sync and start_idx > 0:
            if current_global_idx + chunk_len <= start_idx:
                current_global_idx += chunk_len
                continue
            if current_global_idx < start_idx:
                # Partial overlap
                offset = start_idx - current_global_idx
                df_chunk = df_chunk.iloc[offset:]
                
        # Upload Chunk
        ok = upload_via_edge(
            edge_url,
            anon_key,
            df_chunk,
            log=log,
            resume_key=None, # We handle resume saving manually
            start_index=0, # Chunk is fresh
            batch_size=batch_size,
            progress_cb=None, # We handle progress manually
            pause_event=pause_event,
            silent=True
        )
        
        if not ok:
            log(f"    - Chunk 업로드 실패 (구간: {current_global_idx}~)")
            return False
            
        uploaded_any = True
        current_global_idx += chunk_len
        
        # Update Progress
        if progress_cb and total_rows > 0:
            progress_cb(min(current_global_idx, total_rows), total_rows)
            
        # Save Resume State (approximate)
        set_resume_offset_fn(key, current_global_idx)

    if not uploaded_any:
        log(f"- Upload {key}: 데이터 없음 또는 모두 최신 상태")
    else:
        log(f"- Upload {key}: 완료")
        
    log_processed_fn(folder, filename)
    set_resume_offset_fn(key, 0)
    return True


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
            m_ids_str = ",".join(machine_ids)
            query_url = f"{supabase_url}/rest/v1/tb_work_log"
            params = {
                "select": "start_time,machine_id,die_number,production_qty,production_weight,productivity",
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
                
                for item in existing_data:
                    ts = item.get('start_time')
                    if ts:
                        try:
                            # Normalize DB time to UTC
                            dt = pd.to_datetime(ts).tz_convert("UTC")
                            
                            # Build signature tuple
                            sig = (
                                item.get('machine_id'),
                                dt,
                                item.get('die_number'),
                                item.get('production_qty'),
                                item.get('production_weight'),
                                item.get('productivity')
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
                        
                        # Helper to normalize numeric values (handle None/NaN/float vs int)
                        def norm(v):
                            if pd.isna(v) or v is None: return None
                            try:
                                f = float(v)
                                return int(f) if f.is_integer() else f
                            except:
                                return v

                        sig = (
                            row.get('machine_id'),
                            dt,
                            norm(row.get('die_number')),
                            norm(row.get('production_qty')),
                            norm(row.get('production_weight')),
                            norm(row.get('productivity'))
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
                log(f"중복 체크 실패 (서버 오류 {r.status_code}). 데이터 안전을 위해 업로드를 중단합니다.")
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
