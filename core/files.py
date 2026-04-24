import os
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from os import DirEntry
from typing import List, Literal, Tuple

import pandas as pd

from .state import build_file_state_lookup_keys, load_processed

KST = timezone(timedelta(hours=9))

PreflightStage = Literal["candidate", "preview"]
PreflightIssueCode = Literal[
    "candidate_kind_unsupported",
    "candidate_file_date_missing",
    "candidate_out_of_window",
    "candidate_already_processed",
    "candidate_not_stable",
    "candidate_locked",
    "preview_kind_unsupported",
    "preview_read_empty",
    "preview_read_error",
    "preview_sample_empty",
    "preview_schema_mismatch",
]


@dataclass(frozen=True)
class UploadPreflightIssue:
    stage: PreflightStage
    code: PreflightIssueCode
    detail: str


@dataclass(frozen=True)
class UploadPreflightItem:
    folder: str
    filename: str
    path: str
    kind: str
    file_date: datetime | None
    is_candidate: bool
    has_preview_data: bool | None
    issues: tuple[UploadPreflightIssue, ...]


@dataclass(frozen=True)
class UploadPreflightSummary:
    total_count: int
    candidate_count: int
    blocked_count: int
    preview_ready_count: int
    preview_blocked_count: int
    issue_counts: dict[PreflightIssueCode, int]


@dataclass(frozen=True)
class UploadPreflightPlan:
    items: tuple[UploadPreflightItem, ...]
    summary: UploadPreflightSummary


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


def _build_preflight_issue(
    stage: PreflightStage,
    code: PreflightIssueCode,
    detail: str,
) -> UploadPreflightIssue:
    return UploadPreflightIssue(stage=stage, code=code, detail=detail)


def _is_supported_file_kind(kind: str) -> bool:
    return kind in ("plc", "temp")


def _parse_file_date_for_kind(kind: str, filename: str) -> datetime | None:
    if kind == "plc":
        return parse_plc_date_from_filename(filename)
    if kind == "temp":
        return parse_temp_end_date_from_filename(filename)
    return None


def _preview_schema_matches(kind: str, columns: list[str]) -> bool:
    if kind == "plc":
        column_set = set(columns)
        has_integrated_columns = {"Date", "Time", "Mold1"}.issubset(column_set)
        has_legacy_time_column = any(column in column_set for column in ("?쒓컙", "?쒓컖", "Time"))
        return has_integrated_columns or has_legacy_time_column

    if kind == "temp":
        normalized_columns = {
            re.sub(r"\[|\]", "", column).strip().lower()
            for column in columns
        }
        has_datetime_column = any(
            column in normalized_columns
            for column in ("datetime", "date_time", "?좎쭨?쒓컙", "?쇱떆")
        )
        has_date_column = any(
            column in normalized_columns
            for column in ("date", "?좎쭨", "?쇱옄")
        )
        has_time_column = any(
            column in normalized_columns
            for column in ("time", "?쒓컙", "?쒓컖")
        )
        has_temperature_column = any(
            column in normalized_columns
            for column in ("temperature", "?⑤룄", "temp")
        )
        return has_temperature_column and (has_datetime_column or (has_date_column and has_time_column))

    return False


def _evaluate_candidate_preflight(
    folder: str,
    filename: str,
    path: str,
    kind: str,
    processed: set[str],
    window_start: date | None,
    window_end: date,
    lag_min: int,
    include_today: bool,
    check_lock: bool,
) -> tuple[datetime | None, tuple[UploadPreflightIssue, ...]]:
    issues: list[UploadPreflightIssue] = []
    if not _is_supported_file_kind(kind):
        issues.append(
            _build_preflight_issue(
                "candidate",
                "candidate_kind_unsupported",
                f"지원하지 않는 파일 종류입니다: {kind}",
            )
        )
        return None, tuple(issues)

    file_date = _parse_file_date_for_kind(kind, filename)
    if file_date is None:
        issues.append(
            _build_preflight_issue(
                "candidate",
                "candidate_file_date_missing",
                f"파일명에서 날짜를 해석할 수 없습니다: {filename}",
            )
        )
        return None, tuple(issues)

    if not within_date_window(file_date, window_start, window_end):
        issues.append(
            _build_preflight_issue(
                "candidate",
                "candidate_out_of_window",
                f"업로드 기간 밖의 파일입니다: {filename}",
            )
        )

    if _is_processed_file(processed, folder, filename, path):
        issues.append(
            _build_preflight_issue(
                "candidate",
                "candidate_already_processed",
                f"이미 처리된 파일입니다: {filename}",
            )
        )

    if file_date.date() == kst_now().date() and include_today:
        if not stable_enough(path, lag_min):
            issues.append(
                _build_preflight_issue(
                    "candidate",
                    "candidate_not_stable",
                    f"파일 안정화 대기 중입니다: {filename}",
                )
            )
        if check_lock and is_locked(path):
            issues.append(
                _build_preflight_issue(
                    "candidate",
                    "candidate_locked",
                    f"파일이 잠겨 있습니다: {filename}",
                )
            )

    return file_date, tuple(issues)


def _evaluate_preview_preflight(
    kind: str,
    path: str,
    sample_rows: int,
) -> tuple[bool, tuple[UploadPreflightIssue, ...]]:
    if not _is_supported_file_kind(kind):
        return False, (
            _build_preflight_issue(
                "preview",
                "preview_kind_unsupported",
                f"지원하지 않는 미리보기 종류입니다: {kind}",
            ),
        )

    try:
        sample = _read_preview_sample_dataframe(path, sample_rows)
    except pd.errors.EmptyDataError:
        return False, (
            _build_preflight_issue(
                "preview",
                "preview_read_empty",
                f"미리보기 샘플을 읽었지만 데이터가 없습니다: {path}",
            ),
        )
    except (OSError, UnicodeDecodeError, pd.errors.ParserError) as error:
        return False, (
            _build_preflight_issue(
                "preview",
                "preview_read_error",
                f"미리보기 샘플 읽기에 실패했습니다: {error}",
            ),
        )

    if not _sample_has_non_empty_rows(sample):
        return False, (
            _build_preflight_issue(
                "preview",
                "preview_sample_empty",
                f"미리보기 샘플에 유효한 행이 없습니다: {path}",
            ),
        )

    columns = [str(column).strip() for column in sample.columns]
    if not _preview_schema_matches(kind, columns):
        return False, (
            _build_preflight_issue(
                "preview",
                "preview_schema_mismatch",
                f"미리보기 스키마가 예상 형식과 다릅니다: {path}",
            ),
        )

    return True, ()


def build_file_preflight(
    folder: str,
    filename: str,
    path: str,
    kind: str,
    processed: set[str],
    window_start: date | None,
    window_end: date,
    lag_min: int,
    include_today: bool,
    check_lock: bool,
    sample_rows: int | None,
) -> UploadPreflightItem:
    file_date, candidate_issues = _evaluate_candidate_preflight(
        folder,
        filename,
        path,
        kind,
        processed,
        window_start,
        window_end,
        lag_min,
        include_today,
        check_lock,
    )

    preview_has_rows: bool | None = None
    preview_issues: tuple[UploadPreflightIssue, ...] = ()
    if sample_rows is not None:
        preview_has_rows, preview_issues = _evaluate_preview_preflight(kind, path, sample_rows)

    return UploadPreflightItem(
        folder=folder,
        filename=filename,
        path=path,
        kind=kind,
        file_date=file_date,
        is_candidate=len(candidate_issues) == 0,
        has_preview_data=preview_has_rows,
        issues=candidate_issues + preview_issues,
    )


def summarize_preflight_items(items: tuple[UploadPreflightItem, ...]) -> UploadPreflightSummary:
    candidate_count = 0
    preview_ready_count = 0
    preview_blocked_count = 0
    issue_counts: dict[PreflightIssueCode, int] = {}

    for item in items:
        if item.is_candidate:
            candidate_count += 1
        if item.has_preview_data is True:
            preview_ready_count += 1
        if item.has_preview_data is False:
            preview_blocked_count += 1
        for issue in item.issues:
            issue_counts[issue.code] = issue_counts.get(issue.code, 0) + 1

    total_count = len(items)
    return UploadPreflightSummary(
        total_count=total_count,
        candidate_count=candidate_count,
        blocked_count=total_count - candidate_count,
        preview_ready_count=preview_ready_count,
        preview_blocked_count=preview_blocked_count,
        issue_counts=issue_counts,
    )


def build_upload_preflight_plan(
    folder: str,
    kind: str,
    window_start: date | None,
    window_end: date,
    lag_min: int,
    include_today: bool,
    check_lock: bool,
    sample_rows: int | None,
) -> UploadPreflightPlan:
    if not os.path.isdir(folder):
        empty_items: tuple[UploadPreflightItem, ...] = ()
        return UploadPreflightPlan(
            items=empty_items,
            summary=summarize_preflight_items(empty_items),
        )

    processed = load_processed()
    items = tuple(
        build_file_preflight(
            folder,
            entry.name,
            entry.path,
            kind,
            processed,
            window_start,
            window_end,
            lag_min,
            include_today,
            check_lock,
            sample_rows,
        )
        for entry in _iter_sorted_csv_entries(folder)
    )
    return UploadPreflightPlan(
        items=items,
        summary=summarize_preflight_items(items),
    )


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
