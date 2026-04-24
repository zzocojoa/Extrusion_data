from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Final, Literal

import psycopg2
from psycopg2.extensions import connection as PsycopgConnection

from core.archive_metrics import (
    DbConnectionSettings,
    create_connection,
    load_archive_environment,
    resolve_db_connection_settings,
)

SupabaseDeleteMode = Literal["selected_dates", "all_dates"]

SUPABASE_DELETE_MODE_SELECTED = "selected_dates"
SUPABASE_DELETE_MODE_ALL = "all_dates"
SUPABASE_METRIC_DATE_ROWS_QUERY: Final[str] = """
        SELECT
            ("timestamp" AT TIME ZONE 'Asia/Seoul')::date AS kst_date,
            COUNT(*) AS row_count,
            MIN("timestamp") AS min_timestamp,
            MAX("timestamp") AS max_timestamp
        FROM public.all_metrics
        GROUP BY 1
        ORDER BY 1 ASC
    """
SUPABASE_METRIC_DATE_ROWS_LOCKED_QUERY: Final[str] = f"""
        LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE;

{SUPABASE_METRIC_DATE_ROWS_QUERY}
    """

__all__ = [
    "SupabaseDeleteMode",
    "SupabaseDeletePreview",
    "SupabaseDeleteRequest",
    "SupabaseDeleteResult",
    "SupabaseDeleteSummary",
    "SupabaseMetricDateRow",
    "build_supabase_delete_preview",
    "execute_supabase_delete",
    "load_supabase_mgmt_rows",
]


@dataclass(frozen=True)
class SupabaseMetricDateRow:
    kst_date: date
    row_count: int
    min_timestamp: str
    max_timestamp: str


@dataclass(frozen=True)
class SupabaseDeleteSummary:
    date_count: int
    row_count: int
    min_timestamp: str
    max_timestamp: str


@dataclass(frozen=True)
class SupabaseDeleteRequest:
    selection_mode: SupabaseDeleteMode
    dates: tuple[date, ...]


@dataclass(frozen=True)
class SupabaseDeletePreview:
    request: SupabaseDeleteRequest
    date_rows: tuple[SupabaseMetricDateRow, ...]
    summary: SupabaseDeleteSummary


@dataclass(frozen=True)
class SupabaseDeleteResult:
    request: SupabaseDeleteRequest
    date_rows: tuple[SupabaseMetricDateRow, ...]
    summary: SupabaseDeleteSummary


def _format_date_text(date_value: date) -> str:
    return date_value.isoformat()


def _format_date_tuple(date_values: tuple[date, ...]) -> str:
    return "(" + ", ".join(_format_date_text(date_value) for date_value in date_values) + ")"


def normalize_supabase_metric_timestamp_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)


def _timestamp_sort_key(timestamp_text: str) -> datetime:
    try:
        return datetime.fromisoformat(timestamp_text)
    except ValueError as exc:
        raise ValueError(
            f"Supabase metric timestamp has invalid ISO format. timestamp={timestamp_text}"
        ) from exc


def _resolve_supabase_db_connection_settings(project_root: Path) -> DbConnectionSettings:
    try:
        environment_values = load_archive_environment(project_root)
        return resolve_db_connection_settings(project_root, environment_values)
    except ValueError as exc:
        raise ValueError(
            f"Supabase DB configuration could not be loaded. project_root={project_root}"
        ) from exc


def _open_supabase_connection(project_root: Path) -> PsycopgConnection:
    db_settings = _resolve_supabase_db_connection_settings(project_root)
    try:
        return create_connection(db_settings)
    except psycopg2.Error as exc:
        raise RuntimeError(
            "Supabase DB connection failed. "
            f"project_root={project_root}, host={db_settings.host}, port={db_settings.port}, "
            f"dbname={db_settings.dbname}, user={db_settings.user}"
        ) from exc


def _build_supabase_metric_date_row(
    query_row: tuple[object, object, object, object],
) -> SupabaseMetricDateRow:
    kst_date_raw, row_count_raw, min_timestamp_raw, max_timestamp_raw = query_row
    if type(kst_date_raw) is not date:
        raise TypeError(
            "Supabase metric grouping query returned an invalid date value. "
            f"value={kst_date_raw!r}"
        )
    if row_count_raw is None:
        raise ValueError(
            "Supabase metric grouping query returned a missing row_count. "
            f"kst_date={kst_date_raw.isoformat()}"
        )

    row_count = int(row_count_raw)
    if row_count < 0:
        raise ValueError(
            "Supabase metric grouping query returned a negative row_count. "
            f"kst_date={kst_date_raw.isoformat()}, row_count={row_count}"
        )

    min_timestamp = normalize_supabase_metric_timestamp_text(min_timestamp_raw)
    max_timestamp = normalize_supabase_metric_timestamp_text(max_timestamp_raw)
    if row_count > 0 and (min_timestamp == "" or max_timestamp == ""):
        raise ValueError(
            "Supabase metric grouping query returned an empty timestamp range. "
            f"kst_date={kst_date_raw.isoformat()}, row_count={row_count}"
        )

    return SupabaseMetricDateRow(
        kst_date=kst_date_raw,
        row_count=row_count,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )


def _query_supabase_metric_date_rows_by_query(
    connection: PsycopgConnection,
    query: str,
) -> tuple[SupabaseMetricDateRow, ...]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        query_rows: list[tuple[object, object, object, object]] = cursor.fetchall()

    if len(query_rows) == 0:
        return ()
    return tuple(_build_supabase_metric_date_row(query_row) for query_row in query_rows)


def _query_supabase_metric_date_rows(
    connection: PsycopgConnection,
) -> tuple[SupabaseMetricDateRow, ...]:
    return _query_supabase_metric_date_rows_by_query(
        connection,
        SUPABASE_METRIC_DATE_ROWS_QUERY,
    )


def _query_supabase_metric_date_rows_with_delete_lock(
    connection: PsycopgConnection,
) -> tuple[SupabaseMetricDateRow, ...]:
    return _query_supabase_metric_date_rows_by_query(
        connection,
        SUPABASE_METRIC_DATE_ROWS_LOCKED_QUERY,
    )


def _build_supabase_delete_summary(
    date_rows: tuple[SupabaseMetricDateRow, ...],
) -> SupabaseDeleteSummary:
    if len(date_rows) == 0:
        return SupabaseDeleteSummary(
            date_count=0,
            row_count=0,
            min_timestamp="",
            max_timestamp="",
        )

    row_count: int = 0
    min_timestamp: str = date_rows[0].min_timestamp
    max_timestamp: str = date_rows[0].max_timestamp
    for date_row in date_rows:
        row_count += date_row.row_count
        if _timestamp_sort_key(date_row.min_timestamp) < _timestamp_sort_key(min_timestamp):
            min_timestamp = date_row.min_timestamp
        if _timestamp_sort_key(date_row.max_timestamp) > _timestamp_sort_key(max_timestamp):
            max_timestamp = date_row.max_timestamp

    return SupabaseDeleteSummary(
        date_count=len(date_rows),
        row_count=row_count,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )


def _normalize_supabase_delete_dates(
    request: SupabaseDeleteRequest,
) -> tuple[date, ...]:
    if request.selection_mode == SUPABASE_DELETE_MODE_ALL:
        if len(request.dates) != 0:
            raise ValueError(
                "Supabase delete request for all_dates must not include dates. "
                f"mode={request.selection_mode}, dates={_format_date_tuple(request.dates)}"
            )
        return ()

    if request.selection_mode != SUPABASE_DELETE_MODE_SELECTED:
        raise ValueError(
            f"Supabase delete request mode is invalid. mode={request.selection_mode}"
        )

    if len(request.dates) == 0:
        raise ValueError(
            "Supabase delete request for selected_dates requires at least one date. "
            f"mode={request.selection_mode}"
        )

    for requested_date in request.dates:
        if type(requested_date) is not date:
            raise TypeError(
                "Supabase delete request contains an invalid date value. "
                f"value={requested_date!r}"
            )

    if len(set(request.dates)) != len(request.dates):
        raise ValueError(
            "Supabase delete request contains duplicate dates. "
            f"dates={_format_date_tuple(request.dates)}"
        )

    return tuple(sorted(request.dates))


def _select_supabase_date_rows_for_request(
    all_date_rows: tuple[SupabaseMetricDateRow, ...],
    request: SupabaseDeleteRequest,
    requested_dates: tuple[date, ...],
) -> tuple[SupabaseMetricDateRow, ...]:
    if request.selection_mode == SUPABASE_DELETE_MODE_ALL:
        if len(all_date_rows) == 0:
            raise ValueError(
                "Supabase delete request for all_dates found no current rows. "
                f"mode={request.selection_mode}, available_date_count=0"
            )
        return all_date_rows

    rows_by_date: dict[date, SupabaseMetricDateRow] = {
        date_row.kst_date: date_row for date_row in all_date_rows
    }
    missing_dates: tuple[date, ...] = tuple(
        requested_date
        for requested_date in requested_dates
        if requested_date not in rows_by_date
    )
    if len(missing_dates) != 0:
        raise ValueError(
            "Supabase delete request includes dates that are not present in the current snapshot. "
            f"mode={request.selection_mode}, requested_dates={_format_date_tuple(requested_dates)}, "
            f"missing_dates={_format_date_tuple(missing_dates)}, available_date_count={len(all_date_rows)}"
        )

    return tuple(rows_by_date[requested_date] for requested_date in requested_dates)


def _delete_supabase_date_rows(
    connection: PsycopgConnection,
    request: SupabaseDeleteRequest,
    requested_dates: tuple[date, ...],
) -> tuple[SupabaseMetricDateRow, ...]:
    if request.selection_mode == SUPABASE_DELETE_MODE_ALL:
        query = """
            WITH deleted_rows AS (
                DELETE FROM public.all_metrics
                RETURNING
                    "timestamp",
                    ("timestamp" AT TIME ZONE 'Asia/Seoul')::date AS kst_date
            )
            SELECT
                kst_date,
                COUNT(*) AS row_count,
                MIN("timestamp") AS min_timestamp,
                MAX("timestamp") AS max_timestamp
            FROM deleted_rows
            GROUP BY kst_date
            ORDER BY kst_date ASC
        """
        params: tuple[object, ...] = ()
    else:
        query = """
            WITH deleted_rows AS (
                DELETE FROM public.all_metrics
                WHERE ("timestamp" AT TIME ZONE 'Asia/Seoul')::date = ANY(%s)
                RETURNING
                    "timestamp",
                    ("timestamp" AT TIME ZONE 'Asia/Seoul')::date AS kst_date
            )
            SELECT
                kst_date,
                COUNT(*) AS row_count,
                MIN("timestamp") AS min_timestamp,
                MAX("timestamp") AS max_timestamp
            FROM deleted_rows
            GROUP BY kst_date
            ORDER BY kst_date ASC
        """
        params: tuple[object, ...] = (list(requested_dates),)

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        query_rows: list[tuple[object, object, object, object]] = cursor.fetchall()

    if len(query_rows) == 0:
        return ()
    return tuple(_build_supabase_metric_date_row(query_row) for query_row in query_rows)


def _query_supabase_remaining_delete_row_count(
    connection: PsycopgConnection,
    request: SupabaseDeleteRequest,
    requested_dates: tuple[date, ...],
) -> int:
    if request.selection_mode == SUPABASE_DELETE_MODE_ALL:
        query = """
            SELECT COUNT(*)
            FROM public.all_metrics
        """
        params: tuple[object, ...] = ()
    else:
        query = """
            SELECT COUNT(*)
            FROM public.all_metrics
            WHERE ("timestamp" AT TIME ZONE 'Asia/Seoul')::date = ANY(%s)
        """
        params = (list(requested_dates),)

    with connection.cursor() as cursor:
        cursor.execute(query, params)
        query_row: tuple[object, ...] | None = cursor.fetchone()

    if query_row is None:
        raise ValueError(
            "Supabase delete remaining-row query returned no result. "
            f"mode={request.selection_mode}, selected_dates={_format_date_tuple(requested_dates)}"
        )
    if len(query_row) != 1:
        raise ValueError(
            "Supabase delete remaining-row query returned an invalid row shape. "
            f"mode={request.selection_mode}, column_count={len(query_row)}"
        )

    remaining_row_count = int(query_row[0])
    if remaining_row_count < 0:
        raise ValueError(
            "Supabase delete remaining-row query returned a negative count. "
            f"mode={request.selection_mode}, remaining_row_count={remaining_row_count}"
        )
    return remaining_row_count


def load_supabase_mgmt_rows(project_root: Path) -> tuple[SupabaseMetricDateRow, ...]:
    connection = _open_supabase_connection(project_root)
    try:
        try:
            return _query_supabase_metric_date_rows(connection)
        except (psycopg2.Error, TypeError, ValueError) as exc:
            raise RuntimeError(
                f"Supabase metric grouping query failed. project_root={project_root}"
            ) from exc
    finally:
        connection.close()


def build_supabase_delete_preview(
    date_rows: tuple[SupabaseMetricDateRow, ...],
    request: SupabaseDeleteRequest,
) -> SupabaseDeletePreview:
    requested_dates: tuple[date, ...] = _normalize_supabase_delete_dates(request)
    selected_date_rows: tuple[SupabaseMetricDateRow, ...] = _select_supabase_date_rows_for_request(
        date_rows,
        request,
        requested_dates,
    )
    return SupabaseDeletePreview(
        request=request,
        date_rows=selected_date_rows,
        summary=_build_supabase_delete_summary(selected_date_rows),
    )


def execute_supabase_delete(
    project_root: Path,
    preview: SupabaseDeletePreview,
) -> SupabaseDeleteResult:
    request: SupabaseDeleteRequest = preview.request
    requested_dates: tuple[date, ...] = _normalize_supabase_delete_dates(request)
    connection: PsycopgConnection = _open_supabase_connection(project_root)
    try:
        connection.set_session(
            isolation_level="SERIALIZABLE",
            autocommit=False,
        )

        current_all_date_rows: tuple[SupabaseMetricDateRow, ...] = (
            _query_supabase_metric_date_rows_with_delete_lock(connection)
        )
        current_selected_date_rows: tuple[SupabaseMetricDateRow, ...] = _select_supabase_date_rows_for_request(
            current_all_date_rows,
            request,
            requested_dates,
        )
        current_summary: SupabaseDeleteSummary = _build_supabase_delete_summary(current_selected_date_rows)
        if current_selected_date_rows != preview.date_rows or current_summary != preview.summary:
            raise ValueError(
                "Supabase delete preview does not match the current transaction snapshot. "
                f"mode={request.selection_mode}, selected_date_count={len(requested_dates)}, "
                f"preview_date_count={preview.summary.date_count}, current_date_count={len(current_selected_date_rows)}, "
                f"preview_row_count={preview.summary.row_count}, current_row_count={current_summary.row_count}, "
                f"selected_dates={_format_date_tuple(requested_dates)}"
            )

        deleted_date_rows: tuple[SupabaseMetricDateRow, ...] = _delete_supabase_date_rows(
            connection,
            request,
            requested_dates,
        )
        deleted_summary: SupabaseDeleteSummary = _build_supabase_delete_summary(deleted_date_rows)
        if deleted_date_rows != preview.date_rows or deleted_summary != preview.summary:
            raise ValueError(
                "Supabase delete result does not match the preview. "
                f"mode={request.selection_mode}, selected_date_count={len(requested_dates)}, "
                f"preview_date_count={preview.summary.date_count}, deleted_date_count={len(deleted_date_rows)}, "
                f"preview_row_count={preview.summary.row_count}, deleted_row_count={deleted_summary.row_count}, "
                f"selected_dates={_format_date_tuple(requested_dates)}"
            )

        remaining_row_count: int = _query_supabase_remaining_delete_row_count(
            connection,
            request,
            requested_dates,
        )
        if remaining_row_count != 0:
            raise ValueError(
                "Supabase delete left rows in the target range after deletion. "
                f"mode={request.selection_mode}, selected_date_count={len(requested_dates)}, "
                f"remaining_row_count={remaining_row_count}, "
                f"selected_dates={_format_date_tuple(requested_dates)}"
            )

        connection.commit()
        return SupabaseDeleteResult(
            request=request,
            date_rows=deleted_date_rows,
            summary=deleted_summary,
        )
    except psycopg2.Error as exc:
        connection.rollback()
        raise RuntimeError(
            "Supabase delete failed with a database error. "
            f"project_root={project_root}, mode={request.selection_mode}, "
            f"selected_date_count={len(requested_dates)}, selected_dates={_format_date_tuple(requested_dates)}, "
            f"pgcode={exc.pgcode}, pgerror={exc.pgerror}"
        ) from exc
    except (TypeError, ValueError):
        connection.rollback()
        raise
    finally:
        connection.close()
