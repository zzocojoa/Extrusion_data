import os
import re
from datetime import datetime, timedelta, timezone
from typing import List, Tuple

from .state import load_processed

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
    m = re.match(r"^(\d{2})(\d{2})(\d{2})", name)
    if not m:
        return None
    y, mo, d = m.groups()
    try:
        return datetime(int("20" + y), int(mo), int(d), tzinfo=KST)
    except Exception:
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


def within_cutoff(file_date: datetime, cutoff_date: datetime) -> bool:
    return file_date.date() <= cutoff_date.date()


def stable_enough(path: str, lag_minutes: int) -> bool:
    last = file_mtime_kst(path)
    return last <= (kst_now() - timedelta(minutes=lag_minutes))


def compute_cutoff(mode: str, custom_date: str) -> datetime:
    today = kst_now().date()
    if mode == "today":
        return datetime(today.year, today.month, today.day, tzinfo=KST)
    if mode == "twodays":
        d = today - timedelta(days=2)
        return datetime(d.year, d.month, d.day, tzinfo=KST)
    if mode == "custom" and custom_date:
        y, m, d = map(int, custom_date.split("-"))
        return datetime(y, m, d, tzinfo=KST)
    d = today - timedelta(days=1)
    return datetime(d.year, d.month, d.day, tzinfo=KST)


def list_candidates(
    plc_dir: str,
    temp_dir: str,
    cutoff: datetime,
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
        for fn in sorted(os.listdir(plc_dir)):
            if not fn.lower().endswith(".csv"):
                continue
            fdate = parse_plc_date_from_filename(fn)
            if not fdate or not within_cutoff(fdate, cutoff):
                continue
            path = os.path.join(plc_dir, fn)
            if f"{plc_dir}/{fn}" in processed or fn in processed:
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(path, lag_min):
                    continue
                if check_lock and is_locked(path):
                    continue
            items.append((plc_dir, fn, path, "plc"))

    # Temperature
    if os.path.isdir(temp_dir):
        for fn in sorted(os.listdir(temp_dir)):
            if not fn.lower().endswith(".csv"):
                continue
            fdate = parse_temp_end_date_from_filename(fn)
            if not fdate:
                try:
                    fdate = file_mtime_kst(os.path.join(temp_dir, fn))
                except Exception:
                    fdate = None
            if not fdate or not within_cutoff(fdate, cutoff):
                continue
            path = os.path.join(temp_dir, fn)
            if f"{temp_dir}/{fn}" in processed or fn in processed:
                continue
            if fdate.date() == kst_now().date() and include_today:
                if not stable_enough(path, lag_min):
                    continue
                if check_lock and is_locked(path):
                    continue
            items.append((temp_dir, fn, path, "temp"))

    return items

