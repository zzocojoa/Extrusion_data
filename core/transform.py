from datetime import datetime, timezone, timedelta
from typing import Any

import pandas as pd


KST = timezone(timedelta(hours=9))


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


def build_records_plc(file_path: str, filename: str) -> pd.DataFrame:
    """
    Read a PLC CSV and normalize into the unified metrics schema for all_metrics.
    Includes extrusion end position when present.
    """
    try:
        try:
            df = pd.read_csv(file_path)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, encoding="cp949")

        # Candidate column names for flexible mapping (Korean / spacing variations)
        candidates: dict[str, list[str]] = {
            # time column
            "time": ["시간", "시각", "Time"],
            # main pressure
            "main_pressure": ["메인압력", "메인 압력"],
            # billet length
            "billet_length": ["빌렛길이", "빌렛 길이"],
            # container temperatures (front/rear)
            "container_temp_front": [
                "콘테이너온도 앞쪽",
                "콘테이너 온도 앞쪽",
            ],
            "container_temp_rear": [
                "콘테이너온도 뒤쪽",
                "콘테이너 온도 뒤쪽",
            ],
            # production counter (various spellings)
            "production_counter": [
                "생산카운터",
                "생산 카운터",
                "생산카운트",
                "생산 카운트",
            ],
            # current speed
            "current_speed": ["현재속도", "현재 속도"],
            # extrusion end position
            "extrusion_end_position": [
                "압출종료 위치",
                "압출 종료 위치",
                "압출종료위치",
            ],
        }

        colmap: dict[str, str] = {}
        for key, names in candidates.items():
            for n in names:
                if n in df.columns:
                    colmap[key] = n
                    break

        # Fallback heuristics for commonly varying column names
        cols = list(df.columns)
        if "container_temp_rear" not in colmap:
            # 1) name-based: contains "쪽" or "면"
            for cname in cols:
                if ("쪽" in cname) or ("면" in cname):
                    colmap["container_temp_rear"] = cname
                    break
        if "container_temp_rear" not in colmap and "container_temp_front" in colmap:
            # 2) order-based: first numeric column after front
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

        if "time" not in colmap:
            raise ValueError("필수 컬럼 누락(시간)")

        date_str = f"20{filename[0:2]}-{filename[2:4]}-{filename[4:6]}"
        df["timestamp"] = df[colmap["time"]].apply(
            lambda t: f"{date_str}T{t}+09:00"
        )
        out = pd.DataFrame()
        out["timestamp"] = df["timestamp"]
        out["device_id"] = "extruder_plc"
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
        return out
    except Exception:
        return pd.DataFrame()


def build_records_temp(file_path: str, filename: str) -> pd.DataFrame:
    """
    Read a temperature CSV and normalize into the unified metrics schema.
    """
    import re
    import numpy as np

    try:
        try:
            df = pd.read_csv(file_path, header=0, low_memory=False)
        except UnicodeDecodeError:
            df = pd.read_csv(file_path, header=0, encoding="cp949", low_memory=False)

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
            raise ValueError("온도(Temperature) 컬럼을 찾을 수 없습니다")

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
            raise ValueError("날짜/시간 컬럼을 찾을 수 없습니다")

        out = pd.DataFrame()
        out["timestamp"] = dt.dt.strftime("%Y-%m-%dT%H:%M:%S.%f+09:00")
        out["device_id"] = "spot_temperature_sensor"
        out["temperature"] = pd.to_numeric(df[temp_main], errors="coerce")
        out.replace({np.nan: None}, inplace=True)
        return out
    except Exception:
        return pd.DataFrame()
