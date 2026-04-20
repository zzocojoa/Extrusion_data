from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from core.archive_metrics import (
    DbConnectionSettings,
    create_connection,
    load_archive_environment,
    resolve_db_connection_settings,
)


CANONICAL_SOURCE_MODE = "canonical_snapshot"
CANONICAL_ALGORITHM_VERSION = "aligned_metrics_v1"
LEGACY_SOURCE_MODE = "legacy_backfill"
LEGACY_ALGORITHM_VERSION = "pressure_threshold_v1"
UNMAPPED_MACHINE_ID = "[unmapped]"
@dataclass(frozen=True)
class CanonicalCycleStats:
    row_count: int
    cycle_count: int
    min_timestamp: str
    max_timestamp: str


@dataclass(frozen=True)
class CanonicalRefreshResult:
    stats: CanonicalCycleStats


@dataclass(frozen=True)
class CycleSnapshotSyncResult:
    affected_row_count: int
    latest_cycle_end: str


@dataclass(frozen=True)
class CycleHealthReport:
    cache_row_count: int
    cache_cycle_count: int
    cache_min_timestamp: str
    cache_max_timestamp: str
    snapshot_row_count: int
    snapshot_canonical_count: int
    snapshot_legacy_count: int
    snapshot_unmapped_count: int
    snapshot_latest_end: str
    snapshot_latest_update: str


def _normalize_timestamp_text(raw_value: object) -> str:
    if raw_value is None:
        return ""
    return str(raw_value)


def resolve_cycle_db_connection_settings(project_root: Path) -> DbConnectionSettings:
    environment_values = load_archive_environment(project_root)
    return resolve_db_connection_settings(project_root, environment_values)


def _read_canonical_cycle_stats(connection) -> CanonicalCycleStats:
    query = """
        SELECT
            COUNT(*) AS row_count,
            COUNT(DISTINCT (die_id, session_id, calc_cycle_id)) FILTER (
                WHERE calc_cycle_id IS NOT NULL
            ) AS cycle_count,
            MIN("timestamp") AS min_timestamp,
            MAX("timestamp") AS max_timestamp
        FROM public.mv_optimized_metrics_work_log_cache
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        stats_row = cursor.fetchone()
    if stats_row is None:
        raise ValueError("canonical cycle 통계를 읽지 못했습니다.")
    row_count_raw, cycle_count_raw, min_timestamp_raw, max_timestamp_raw = stats_row
    return CanonicalCycleStats(
        row_count=int(row_count_raw or 0),
        cycle_count=int(cycle_count_raw or 0),
        min_timestamp=_normalize_timestamp_text(min_timestamp_raw),
        max_timestamp=_normalize_timestamp_text(max_timestamp_raw),
    )


def execute_canonical_refresh(
    project_root: Path,
    log_callback: Callable[[str], None],
    progress_callback: Callable[[float], None],
) -> CanonicalRefreshResult:
    db_settings = resolve_cycle_db_connection_settings(project_root)
    progress_callback(0.0)
    connection = create_connection(db_settings)
    try:
        connection.autocommit = True
        with connection.cursor() as cursor:
            log_callback("정식 cycle refresh: aligned metrics view 갱신")
            cursor.execute("REFRESH MATERIALIZED VIEW public.view_optimized_aligned_metrics")
            progress_callback(0.25)

            log_callback("정식 cycle refresh: work log effective ranges 갱신")
            cursor.execute("REFRESH MATERIALIZED VIEW public.mv_work_log_effective_ranges")
            progress_callback(0.5)

            log_callback("정식 cycle refresh: optimized metrics work log 갱신")
            cursor.execute("REFRESH MATERIALIZED VIEW public.mv_optimized_metrics_work_log")
            progress_callback(0.75)

            log_callback("정식 cycle refresh: cache 전체 갱신")
            cursor.execute("SELECT public.refresh_mv_optimized_metrics_work_log_cache_full()")
            progress_callback(0.9)

        stats = _read_canonical_cycle_stats(connection)
        progress_callback(1.0)
        log_callback("정식 cycle refresh 완료")
        log_callback(f"cache row 수: {stats.row_count}")
        log_callback(f"cache cycle 수: {stats.cycle_count}")
        log_callback(f"cache timestamp 범위: {stats.min_timestamp} -> {stats.max_timestamp}")
        return CanonicalRefreshResult(stats=stats)
    finally:
        connection.close()


def execute_cycle_snapshot_sync(
    project_root: Path,
    log_callback: Callable[[str], None],
    progress_callback: Callable[[float], None],
) -> CycleSnapshotSyncResult:
    db_settings = resolve_cycle_db_connection_settings(project_root)
    progress_callback(0.0)
    connection = create_connection(db_settings)
    try:
        with connection.cursor() as cursor:
            log_callback("cycle snapshot 동기화 시작")
            cursor.execute(
                """
                DELETE FROM public.tb_cycle_log
                WHERE source_mode = %s
                  AND algorithm_version = %s
                """,
                (
                    CANONICAL_SOURCE_MODE,
                    CANONICAL_ALGORITHM_VERSION,
                ),
            )
            progress_callback(0.2)
            cursor.execute(
                """
                WITH cycle_bounds AS (
                    SELECT
                        cache.die_id,
                        cache.session_id,
                        cache.calc_cycle_id,
                        MIN(cache."timestamp") AS start_time,
                        MAX(cache."timestamp") AS end_time,
                        MAX(cache.production_counter)::bigint AS production_counter,
                        MAX(cache.main_pressure)::double precision AS max_pressure
                    FROM public.mv_optimized_metrics_work_log_cache AS cache
                    WHERE cache.calc_cycle_id IS NOT NULL
                    GROUP BY cache.die_id, cache.session_id, cache.calc_cycle_id
                ),
                resolved AS (
                    SELECT
                        COALESCE(work_log.machine_id, %s) AS machine_id,
                        cycle_bounds.start_time,
                        cycle_bounds.end_time,
                        cycle_bounds.production_counter,
                        exact_range.work_log_id,
                        EXTRACT(EPOCH FROM (cycle_bounds.end_time - cycle_bounds.start_time))::double precision AS duration_sec,
                        cycle_bounds.max_pressure,
                        TRUE AS is_valid,
                        FALSE AS is_test_run,
                        %s::text AS source_mode,
                        %s::text AS algorithm_version
                    FROM cycle_bounds
                    LEFT JOIN LATERAL (
                        SELECT ranges.work_log_id
                        FROM public.mv_work_log_effective_ranges AS ranges
                        WHERE ranges.period @> cycle_bounds.start_time
                        ORDER BY ranges.range_start DESC
                        LIMIT 1
                    ) AS exact_range ON TRUE
                    LEFT JOIN public.tb_work_log AS work_log
                        ON work_log.id = exact_range.work_log_id
                )
                INSERT INTO public.tb_cycle_log (
                    machine_id,
                    start_time,
                    end_time,
                    production_counter,
                    work_log_id,
                    duration_sec,
                    max_pressure,
                    is_valid,
                    is_test_run,
                    source_mode,
                    algorithm_version
                )
                SELECT
                    machine_id,
                    start_time,
                    end_time,
                    production_counter,
                    work_log_id,
                    duration_sec,
                    max_pressure,
                    is_valid,
                    is_test_run,
                    source_mode,
                    algorithm_version
                FROM resolved
                ON CONFLICT (
                    machine_id,
                    start_time,
                    end_time,
                    source_mode,
                    algorithm_version
                )
                DO UPDATE
                SET
                    production_counter = EXCLUDED.production_counter,
                    work_log_id = EXCLUDED.work_log_id,
                    duration_sec = EXCLUDED.duration_sec,
                    max_pressure = EXCLUDED.max_pressure,
                    is_valid = EXCLUDED.is_valid,
                    is_test_run = EXCLUDED.is_test_run,
                    source_mode = EXCLUDED.source_mode,
                    algorithm_version = EXCLUDED.algorithm_version,
                    updated_at = now()
                """,
                (
                    UNMAPPED_MACHINE_ID,
                    CANONICAL_SOURCE_MODE,
                    CANONICAL_ALGORITHM_VERSION,
                ),
            )
            affected_row_count = cursor.rowcount
            progress_callback(0.8)

            cursor.execute(
                """
                SELECT COALESCE(MAX(end_time)::text, '')
                FROM public.tb_cycle_log
                WHERE source_mode = %s
                """,
                (CANONICAL_SOURCE_MODE,),
            )
            latest_cycle_end_raw = cursor.fetchone()
            latest_cycle_end = ""
            if latest_cycle_end_raw is not None:
                latest_cycle_end = str(latest_cycle_end_raw[0] or "")
        connection.commit()
        progress_callback(1.0)
        log_callback("cycle snapshot 동기화 완료")
        log_callback(f"영향 row 수: {affected_row_count}")
        log_callback(f"최신 cycle 종료 시각: {latest_cycle_end}")
        return CycleSnapshotSyncResult(
            affected_row_count=affected_row_count,
            latest_cycle_end=latest_cycle_end,
        )
    finally:
        connection.close()


def execute_cycle_health_check(project_root: Path) -> CycleHealthReport:
    db_settings = resolve_cycle_db_connection_settings(project_root)
    connection = create_connection(db_settings)
    try:
        canonical_stats = _read_canonical_cycle_stats(connection)
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    COUNT(*) AS snapshot_row_count,
                    COUNT(*) FILTER (WHERE source_mode = %s) AS snapshot_canonical_count,
                    COUNT(*) FILTER (WHERE source_mode = %s) AS snapshot_legacy_count,
                    COUNT(*) FILTER (WHERE work_log_id IS NULL) AS snapshot_unmapped_count,
                    COALESCE(MAX(end_time)::text, '') AS snapshot_latest_end,
                    COALESCE(MAX(updated_at)::text, '') AS snapshot_latest_update
                FROM public.tb_cycle_log
                """,
                (
                    CANONICAL_SOURCE_MODE,
                    LEGACY_SOURCE_MODE,
                ),
            )
            stats_row = cursor.fetchone()
        if stats_row is None:
            raise ValueError("cycle snapshot 통계를 읽지 못했습니다.")
        return CycleHealthReport(
            cache_row_count=canonical_stats.row_count,
            cache_cycle_count=canonical_stats.cycle_count,
            cache_min_timestamp=canonical_stats.min_timestamp,
            cache_max_timestamp=canonical_stats.max_timestamp,
            snapshot_row_count=int(stats_row[0] or 0),
            snapshot_canonical_count=int(stats_row[1] or 0),
            snapshot_legacy_count=int(stats_row[2] or 0),
            snapshot_unmapped_count=int(stats_row[3] or 0),
            snapshot_latest_end=str(stats_row[4] or ""),
            snapshot_latest_update=str(stats_row[5] or ""),
        )
    finally:
        connection.close()
