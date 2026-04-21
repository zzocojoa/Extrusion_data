import os
import re
from datetime import date, datetime, timedelta, timezone
from os import DirEntry
from typing import List, Tuple

import pandas as pd

from .state import build_file_state_lookup_keys, load_processed

KST = timezone(timedelta(hours=9))


def kst_now() -> datetime:
    return datetime.now(KST)


def is_locked(path: str) -> bool:
    try:
        if os.name == "nt":
            import msvcrt

            with open(path, "rb") as fh:
                try:
                    msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
                    msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
                    return False
                except OSError:
                    return True
        return False
    except Exception:
        return True


def file_mtime_kst(path: str) -> datetime:
    ts = os.path.getmtime(path)
    return datetime.fromtimestamp(ts, timezone.utc).astimezone(KST)


def parse_plc_date_from_filename(name: str) -> datetime | None:
    # 1. Legacy PLC: YYMMDD...
    m = re.match(r"^(\d{2})(\d{2})(\d{2})", name)
    if m:
        y, mo, d = m.groups()
        try:
            return datetime(int("20" + y), int(mo), int(d), tzinfo=KST)
        except Exception:
            pass
            
    # 2. Integrated Log: Factory_Integrated_Log_YYYYMMDD_...
    m2 = re.match(r"Factory_Integrated_Log_(\d{4})(\d{2})(\d{2})_", name)
    if m2:
        y, mo, d = m2.groups()
        try:
            return datetime(int(y), int(mo), int(d), tzinfo=KST)
        except Exception:
            pass

    return None


def parse_temp_end_date_from_filename(name: str) -> datetime | None:
    m = re.search(r"__([0-9]{4}-[0-9]{2}-[0-9]{2})", name)
    if m:
        date_str = m.group(1)
    else:
        matches = list(re.finditer(r"([0-9]{4}-[0-9]{2}-[0-9]{2})", name))
        if not matches:
            return None
        date_str = matches[-1].group(1)
    try:
        y, mo, d = map(int, date_str.split("-"))
        return datetime(y, mo, d, tzinfo=KST)
    except Exception:
        return None


def parse_iso_date(date_text: str) -> date:
    parsed = datetime.strptime(date_text.strip(), "%Y-%m-%d")
    return parsed.date()


def resolve_custom_range_texts(
    custom_date_start: str,
    custom_date_end: str,
    legacy_custom_date: str,
) -> tuple[str, str]:
    cleaned_start = custom_date_start.strip()
    cleaned_end = custom_date_end.strip()
    cleaned_legacy = legacy_custom_date.strip()

    if cleaned_start != "" or cleaned_end != "":
        return cleaned_start, cleaned_end

    if cleaned_legacy == "":
        return "", ""

    return cleaned_legacy, cleaned_legacy


def compute_date_window(
    mode: str,
    custom_date_start: str,
    custom_date_end: str,
) -> tuple[date | None, date]:
    today = kst_now().date()
    if mode == "today":
        return None, today
    if mode == "twodays":
        return None, today - timedelta(days=2)
    if mode == "custom":
        start_date = parse_iso_date(custom_date_start)
        end_date = parse_iso_date(custom_date_end)
        if start_date > end_date:
            raise ValueError("custom 시작일은 종료일보다 늦을 수 없습니다.")
        return start_date, end_date
    return None, today - timedelta(days=1)


def within_date_window(file_date: datetime, start_date: date | None, end_date: date) -> bool:
    file_day = file_date.date()
    if start_date is not None and file_day < start_date:
        return False
    return file_day <= end_date


def stable_enough(path: str, lag_minutes: int) -> bool:
    last = file_mtime_kst(path)
    return last <= (kst_now() - timedelta(minutes=lag_minutes))


def _iter_sorted_csv_entries(folder: str) -> list[DirEntry[str]]:
    try:
        with os.scandir(folder) as entry_iterator:
            csv_entries = [
                entry
                for entry in entry_iterator
                if entry.is_file() and entry.name.lower().endswith(".csv")
            ]
    except OSError:
        return []
    csv_entries.sort(key=lambda entry: entry.name)
    return csv_entries


def _is_processed_file(processed: set[str], folder: str, filename: str, file_path: str) -> bool:
    lookup_keys = build_file_state_lookup_keys(folder, filename, file_path)
    for lookup_key in lookup_keys:
        if lookup_key in processed:
            return True
    return False


def _read_preview_sample_dataframe(path: str, sample_rows: int) -> pd.DataFrame:
    if sample_rows <= 0:
        raise ValueError("sample_rows must be positive.")
    try:
        return pd.read_csv(path, encoding="utf-8-sig", nrows=sample_rows)
    except UnicodeDecodeError:
        return pd.read_csv(path, encoding="cp949", nrows=sample_rows)


def _sample_has_non_empty_rows(dataframe: pd.DataFrame) -> bool:
    if dataframe.empty:
        return False
    return bool(dataframe.notna().any(axis=1).any())


def preview_has_data(kind: str, path: str, sample_rows: int) -> bool:
    sample = _read_preview_sample_dataframe(path, sample_rows)
    if not _sample_has_non_empty_rows(sample):
        return False

    columns = [str(column).strip() for column in sample.columns]
    if kind == "plc":
        column_set = set(columns)
        has_integrated_columns = {"Date", "Time", "Mold1"}.issubset(column_set)
        has_legacy_time_column = any(column in column_set for column in ("시간", "시각", "Time"))
        return has_integrated_columns or has_legacy_time_column

    if kind == "temp":
        normalized_columns = {
            re.sub(r"\[|\]", "", column).strip().lower()
            for column in columns
        }
        has_datetime_column = any(
            column in normalized_columns
            for column in ("datetime", "date_time", "날짜시간", "일시")
        )
        has_date_column = any(
            column in normalized_columns
            for column in ("date", "날짜", "일자")
        )
        has_time_column = any(
            column in normalized_columns
            for column in ("time", "시간", "시각")
        )
        has_temperature_column = any(
            column in normalized_columns
            for column in ("temperature", "온도", "temp")
        )
        return has_temperature_column and (has_datetime_column or (has_date_column and has_time_column))

    raise ValueError(f"Unsupported preview validation kind: {kind}")


def list_candidates(
    plc_dir: str,
    temp_dir: str | None,
    window_start: date | None,
    window_end: date,
    lag_min: int,
    include_today: bool,
    check_lock: bool,
    quick: bool,
) -> List[Tuple[str, str, str, str]]:
    """
    Return list of (folder, filename, full_path, kind) for PLC and temp files.
    """
    items: List[Tuple[str, str, str, str]] = []
    processed = load_processed()

    # PLC
    if os.path.isdir(plc_dir):
        for entry in _iter_sorted_csv_entries(plc_dir):
            fn = entry.name
            fdate = parse_plc_date_from_filename(fn)
            if not fdate:
                continue
            if not within_date_window(fdate, window_start, window_end):
                continue
            path = entry.path
            if _is_processed_file(processed, plc_dir, fn, path):
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(path, lag_min):
                    continue
                if check_lock and is_locked(path):
                    continue
            items.append((plc_dir, fn, path, "plc"))

    # Temperature (Removed)
    # logic removed for single folder refactor

    return items
