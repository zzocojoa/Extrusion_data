from datetime import datetime, timezone, timedelta
from typing import Any

import pandas as pd


KST = timezone(timedelta(hours=9))
PLC_DEVICE_ID = "extruder_plc"
INTEGRATED_PLC_DEVICE_ID = "extruder_integrated"


def parse_plc_date_from_filename(name: str) -> datetime | None:
    """
    Extract PLC file date from filename like 'YYMMDD...csv' and return KST date.
    """
    import re

    m = re.match(r"^(\d{2})(\d{2})(\d{2})", name)
    if not m:
        return None
    y, mo, d = m.groups()
    try:
        return datetime(int("20" + y), int(mo), int(d), tzinfo=KST)
    except Exception:
        return None


def build_records_plc(file_path: str, filename: str, chunksize: int | None = None) -> pd.DataFrame | Any:
    """
    Read a PLC CSV and normalize into the unified metrics schema.
    If chunksize is set, returns a generator of DataFrames.
    """
    try:
        # 1. Prepare Reader
        kwargs = {}
        if chunksize:
            kwargs['chunksize'] = chunksize
            
        try:
            reader = pd.read_csv(file_path, **kwargs)
        except UnicodeDecodeError:
            reader = pd.read_csv(file_path, encoding="cp949", **kwargs)

        # Helper to process a single DF
        def process_df(df: pd.DataFrame, colmap_cache: dict | None = None) -> tuple[pd.DataFrame, dict]:
            # Candidate column names mapping (same as before)
            candidates: dict[str, list[str]] = {
                "time": ["시간", "시각", "Time"],
                "main_pressure": ["메인압력", "메인 압력"],
                "billet_length": ["빌렛길이", "빌렛 길이"],
                "container_temp_front": ["콘테이너온도 앞쪽", "콘테이너 온도 앞쪽"],
                "container_temp_rear": ["콘테이너온도 뒤쪽", "콘테이너 온도 뒤쪽"],
                "production_counter": ["생산카운터", "생산 카운터", "생산카운트", "생산 카운트"],
                "current_speed": ["현재속도", "현재 속도"],
                "extrusion_end_position": ["압출종료 위치", "압출 종료 위치", "압출종료위치"],
            }

            # Determine colmap if not provided
            if colmap_cache:
                colmap = colmap_cache
            else:
                colmap = {}
                for key, names in candidates.items():
                    for n in names:
                        if n in df.columns:
                            colmap[key] = n
                            break
                
                # Heuristics
                cols = list(df.columns)
                if "container_temp_rear" not in colmap:
                    for cname in cols:
                        if ("쪽" in cname) or ("면" in cname):
                            colmap["container_temp_rear"] = cname
                            break
                if "container_temp_rear" not in colmap and "container_temp_front" in colmap:
                    try:
                        front_idx = cols.index(colmap["container_temp_front"])
                    except ValueError:
                        front_idx = -1
                    if front_idx >= 0:
                        used = set(colmap.values())
                        for cname in cols[front_idx + 1 :]:
                            if cname in used:
                                continue
                            s = df[cname]
                            if getattr(s.dtype, "kind", None) in ("i", "u", "f", "c"):
                                colmap["container_temp_rear"] = cname
                                break
                if "production_counter" not in colmap:
                    for cname in cols:
                        if ("생산" in cname) and (("카운터" in cname) or ("카운트" in cname)):
                            colmap["production_counter"] = cname
                            break

            # --- [Modified] Integrated Log Detection & Date Parsing ---
            is_integrated = False
            if 'Date' in df.columns and 'Time' in df.columns and 'Mold1' in df.columns:
                is_integrated = True
            
            if is_integrated:
                # 통합 로그는 고정된 Date/Time 열을 그대로 벡터 연산으로 결합한다.
                try:
                    date_series = df["Date"].astype(str, copy=False)
                    time_series = df["Time"].astype(str, copy=False)
                    timestamp_text = date_series.str.cat(time_series, sep=" ")
                    timestamp_series = pd.to_datetime(
                        timestamp_text,
                        errors="coerce",
                    ).dt.strftime("%Y-%m-%dT%H:%M:%S.%f+09:00")
                except Exception:
                    return pd.DataFrame(), colmap
            elif "time" in colmap:
                # 레거시 PLC는 파일 날짜 접두사를 시간 열에 벡터 방식으로 붙인다.
                date_prefix = f"20{filename[0:2]}-{filename[2:4]}-{filename[4:6]}T"
                time_series = df[colmap["time"]].astype(str, copy=False)
                timestamp_series = date_prefix + time_series + "+09:00"
            else:
                # No time info found
                return pd.DataFrame(), colmap

            out = pd.DataFrame()
            out["timestamp"] = timestamp_series
            out["device_id"] = INTEGRATED_PLC_DEVICE_ID if is_integrated else PLC_DEVICE_ID
            
            # Map standard columns
            for key in [
                "main_pressure",
                "billet_length",
                "container_temp_front",
                "container_temp_rear",
                "production_counter",
                "current_speed",
                "extrusion_end_position",
            ]:
                if key in colmap:
                    out[key] = df[colmap[key]]
                    
            # Map integrated columns if present
            if is_integrated:
                # Explicit mapping for integrated log
                val_map = {
                    "Temperature": "temperature", # Integrated log has 'Temperature' column
                    "Mold1": "mold_1",
                    "Mold2": "mold_2",
                    "Mold3": "mold_3",
                    "Mold4": "mold_4",
                    "Mold5": "mold_5",
                    "Mold6": "mold_6",
                    "Billet_Temp": "billet_temp",
                    "At_Pre": "at_pre",
                    "At_Temp": "at_temp",
                    "DIE_ID": "die_id",
                    "Billet_CycleID": "billet_cycle_id"
                }
                for src, dest in val_map.items():
                    if src in df.columns:
                        out[dest] = df[src]
            
            return out, colmap

        # 2. Handle Chunking vs Full
        if chunksize:
            def generator():
                colmap = None
                for chunk in reader:
                    processed, colmap = process_df(chunk, colmap)
                    if not processed.empty:
                        yield processed
            return generator()
        else:
            # Full read (reader is DataFrame)
            processed, _ = process_df(reader)
            return processed

    except Exception as error:
        raise ValueError(f"PLC CSV 변환 실패: path={file_path}, filename={filename}") from error


def build_records_temp(file_path: str, filename: str, chunksize: int | None = None) -> pd.DataFrame | Any:
    """
    Read a temperature CSV and normalize into the unified metrics schema.
    If chunksize is set, returns a generator of DataFrames.
    """
    import re
    import numpy as np

    try:
        kwargs = {'header': 0, 'low_memory': False}
        if chunksize:
            kwargs['chunksize'] = chunksize

        try:
            reader = pd.read_csv(file_path, **kwargs)
        except UnicodeDecodeError:
            reader = pd.read_csv(file_path, encoding="cp949", **kwargs)

        def process_df(df: pd.DataFrame) -> pd.DataFrame:
            # normalize columns (strip spaces, remove bracket chars), build lower-case map
            df.columns = df.columns.str.strip().str.replace(r"\[|\]", "", regex=True)
            lower_map = {c.lower(): c for c in df.columns}

            def pick(*cands: str) -> str | None:
                for c in cands:
                    key = c.lower()
                    if key in lower_map:
                        return lower_map[key]
                return None

            dt_col = pick("datetime", "date_time", "날짜시간", "일시")
            date_col = pick("date", "날짜", "일자")
            time_col = pick("time", "시간", "시각")
            temp_main = pick("temperature", "온도", "temp")
            if temp_main is None:
                # Skip if no temp column
                return pd.DataFrame()

            if dt_col is not None:
                dt = pd.to_datetime(df[dt_col], errors="coerce")
            elif date_col is not None and time_col is not None:
                tstr = df[time_col].astype(str)
                has_ms = tstr.str.count(":") >= 3
                tconv = tstr
                if has_ms.any():
                    parts = tstr.str.rsplit(":", n=1, expand=True)
                    tconv = parts[0] + "." + parts[1]
                dt = pd.to_datetime(
                    df[date_col].astype(str) + " " + tconv, errors="coerce"
                )
            else:
                return pd.DataFrame()

            out = pd.DataFrame()
            out["timestamp"] = dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%f+09:00")
            out["device_id"] = "spot_temperature_sensor"
            out["temperature"] = pd.to_numeric(df[temp_main], errors="coerce")
            out.replace({np.nan: None}, inplace=True)
            return out

        if chunksize:
            def generator():
                for chunk in reader:
                    processed = process_df(chunk)
                    if not processed.empty:
                        yield processed
            return generator()
        else:
            return process_df(reader)

    except Exception as error:
        raise ValueError(f"온도 CSV 변환 실패: path={file_path}, filename={filename}") from error
