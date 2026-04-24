from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterator, Mapping

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import dotenv_values

from core.config import load_config_with_sources


KST = timezone(timedelta(hours=9))
DEFAULT_DB_HOST = "127.0.0.1"
DEFAULT_DB_NAME = "postgres"
DEFAULT_DB_USER = "postgres"
DEFAULT_ARCHIVE_CHUNK_SIZE = 100_000


@dataclass(frozen=True)
class DbConnectionSettings:
    host: str
    port: int
    user: str
    password: str
    dbname: str


@dataclass(frozen=True)
class ArchiveExportResult:
    output_path: Path
    row_count: int
    chunk_count: int
    min_timestamp: str
    max_timestamp: str


@dataclass(frozen=True)
class ArchiveStats:
    row_count: int
    min_timestamp: str
    max_timestamp: str


def load_archive_environment(project_root: Path) -> dict[str, str]:
    runtime_root = resolve_runtime_project_root(project_root)
    env_path = runtime_root / ".env"
    loaded_values = dotenv_values(env_path)
    merged_values: dict[str, str] = {}

    for key, value in loaded_values.items():
        if value is None:
            continue
        merged_values[key] = value

    config_values, _config_path, _metadata = load_config_with_sources(None)
    for key in {
        "DB_PASSWORD",
        "DB_HOST",
        "DB_PORT",
        "DB_USER",
        "DB_NAME",
    }:
        config_value = config_values.get(key, "").strip()
        if config_value != "":
            merged_values[key] = config_value

    for key, value in os.environ.items():
        if key in {
            "ARCHIVE_DIR",
            "DB_PASSWORD",
            "DB_HOST",
            "DB_PORT",
            "DB_USER",
            "DB_NAME",
        }:
            merged_values[key] = value

    return merged_values


def build_runtime_project_root_candidates(project_root: Path) -> tuple[Path, ...]:
    candidate_values: list[Path] = [project_root.resolve()]
    if getattr(sys, "frozen", False):
        executable_dir = Path(sys.executable).resolve().parent
        candidate_values.append(executable_dir)
        candidate_values.append(executable_dir.parent)

    unique_candidates: list[Path] = []
    seen_candidates: set[str] = set()
    for candidate in candidate_values:
        normalized_candidate = str(candidate).lower()
        if normalized_candidate in seen_candidates:
            continue
        seen_candidates.add(normalized_candidate)
        unique_candidates.append(candidate)
    return tuple(unique_candidates)


def resolve_runtime_project_root(project_root: Path) -> Path:
    for candidate in build_runtime_project_root_candidates(project_root):
        if (candidate / ".env").is_file():
            return candidate
        if (candidate / "supabase" / "config.toml").is_file():
            return candidate
    return project_root.resolve()


def read_local_db_port(project_root: Path) -> int:
    import tomllib

    config_path: Path | None = None
    for candidate in build_runtime_project_root_candidates(project_root):
        candidate_config_path = candidate / "supabase" / "config.toml"
        if candidate_config_path.is_file():
            config_path = candidate_config_path
            break

    if config_path is None:
        raise ValueError(
            "supabase/config.toml을 찾을 수 없습니다. "
            f"project_root={project_root}, candidates={build_runtime_project_root_candidates(project_root)}"
        )

    with config_path.open("rb") as file_handle:
        config_values = tomllib.load(file_handle)

    db_section = config_values.get("db")
    if not isinstance(db_section, dict):
        raise ValueError(f"db 설정을 찾을 수 없습니다: {config_path}")

    port_value = db_section.get("port")
    if not isinstance(port_value, int):
        raise ValueError(f"db.port 설정이 올바르지 않습니다: {config_path}")

    return port_value


def resolve_archive_dir(
    explicit_archive_dir: str | None,
    environment_values: Mapping[str, str],
) -> Path:
    if explicit_archive_dir is not None and explicit_archive_dir.strip() != "":
        return Path(explicit_archive_dir).expanduser().resolve()

    archive_dir = environment_values.get("ARCHIVE_DIR")
    if archive_dir is None or archive_dir.strip() == "":
        raise ValueError("ARCHIVE_DIR가 설정되지 않았습니다. --archive-dir 또는 .env를 확인하세요.")

    return Path(archive_dir).expanduser().resolve()


def resolve_db_connection_settings(
    project_root: Path,
    environment_values: Mapping[str, str],
) -> DbConnectionSettings:
    password = environment_values.get("DB_PASSWORD")
    if password is None or password.strip() == "":
        raise ValueError("DB_PASSWORD가 설정되지 않았습니다. .env 또는 os.environ을 확인하세요.")

    host = environment_values.get("DB_HOST", DEFAULT_DB_HOST).strip()
    user = environment_values.get("DB_USER", DEFAULT_DB_USER).strip()
    dbname = environment_values.get("DB_NAME", DEFAULT_DB_NAME).strip()
    port_text = environment_values.get("DB_PORT")
    port = int(port_text) if port_text is not None and port_text.strip() != "" else read_local_db_port(project_root)

    return DbConnectionSettings(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )


def parse_archive_before_date(date_text: str) -> datetime:
    try:
        archive_date = date.fromisoformat(date_text)
    except ValueError as exc:
        raise ValueError(f"before-date 형식이 올바르지 않습니다: {date_text}") from exc

    return datetime.combine(archive_date, time.min, tzinfo=KST)


def build_archive_output_path(archive_dir: Path, cutoff_datetime: datetime) -> Path:
    cutoff_date_text = cutoff_datetime.date().isoformat()
    target_dir = archive_dir / f"before_date={cutoff_date_text}"
    return target_dir / f"all_metrics_before_{cutoff_date_text}.parquet"


def create_connection(db_settings: DbConnectionSettings):
    return psycopg2.connect(
        host=db_settings.host,
        port=db_settings.port,
        user=db_settings.user,
        password=db_settings.password,
        dbname=db_settings.dbname,
    )


def read_all_metrics_chunks(
    db_settings: DbConnectionSettings,
    before_datetime: datetime,
    chunk_size: int,
) -> Iterator[pd.DataFrame]:
    query = """
        SELECT *
        FROM public.all_metrics
        WHERE "timestamp" < %s
        ORDER BY "timestamp" ASC
    """
    connection = create_connection(db_settings)
    try:
        chunk_iterator = pd.read_sql_query(
            query,
            connection,
            params=(before_datetime,),
            chunksize=chunk_size,
        )
        for chunk_frame in chunk_iterator:
            yield chunk_frame
    finally:
        connection.close()


def _build_archive_stats_from_row(stats_row: tuple[object, object, object]) -> ArchiveStats:
    row_count_raw, min_timestamp_raw, max_timestamp_raw = stats_row
    row_count = int(row_count_raw)
    if row_count == 0:
        return ArchiveStats(
            row_count=0,
            min_timestamp="",
            max_timestamp="",
        )
    return ArchiveStats(
        row_count=row_count,
        min_timestamp=_normalize_timestamp_text(min_timestamp_raw),
        max_timestamp=_normalize_timestamp_text(max_timestamp_raw),
    )


def query_all_metrics_archive_stats(
    connection,
    before_datetime: datetime,
) -> ArchiveStats:
    query = """
        SELECT
            COUNT(*) AS row_count,
            MIN("timestamp") AS min_timestamp,
            MAX("timestamp") AS max_timestamp
        FROM public.all_metrics
        WHERE "timestamp" < %s
    """
    with connection.cursor() as cursor:
        cursor.execute(query, (before_datetime,))
        stats_row = cursor.fetchone()
    if stats_row is None:
        raise ValueError("all_metrics 통계를 읽지 못했습니다.")
    return _build_archive_stats_from_row(stats_row)


def query_all_metrics_archive_stats_with_delete_lock(
    connection,
    before_datetime: datetime,
) -> ArchiveStats:
    query = """
        SELECT
            COUNT(*) AS row_count,
            MIN("timestamp") AS min_timestamp,
            MAX("timestamp") AS max_timestamp
        FROM public.all_metrics
        WHERE "timestamp" < %s
    """
    with connection.cursor() as cursor:
        cursor.execute("LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE")
        cursor.execute(query, (before_datetime,))
        stats_row = cursor.fetchone()
    if stats_row is None:
        raise ValueError("락 획득 후 all_metrics 통계를 읽지 못했습니다.")
    return _build_archive_stats_from_row(stats_row)


def read_all_metrics_archive_stats(
    db_settings: DbConnectionSettings,
    before_datetime: datetime,
) -> ArchiveStats:
    connection = create_connection(db_settings)
    try:
        return query_all_metrics_archive_stats(connection, before_datetime)
    finally:
        connection.close()


def _normalize_timestamp_text(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return str(value)


def export_all_metrics_to_parquet(
    db_settings: DbConnectionSettings,
    before_datetime: datetime,
    output_path: Path,
    chunk_size: int,
) -> ArchiveExportResult:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    total_rows = 0
    chunk_count = 0
    min_timestamp = ""
    max_timestamp = ""
    writer: pq.ParquetWriter | None = None

    try:
        for chunk_frame in read_all_metrics_chunks(db_settings, before_datetime, chunk_size):
            if chunk_frame.empty:
                continue

            chunk_count += 1
            total_rows += len(chunk_frame)
            if "timestamp" in chunk_frame.columns:
                chunk_min = _normalize_timestamp_text(chunk_frame["timestamp"].iloc[0])
                chunk_max = _normalize_timestamp_text(chunk_frame["timestamp"].iloc[-1])
                if min_timestamp == "":
                    min_timestamp = chunk_min
                max_timestamp = chunk_max

            table = pa.Table.from_pandas(chunk_frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(
                    output_path,
                    table.schema,
                    compression="zstd",
                )
            writer.write_table(table)
    finally:
        if writer is not None:
            writer.close()

    if total_rows == 0:
        if output_path.exists():
            output_path.unlink()
        raise ValueError("아카이브할 all_metrics 데이터가 없습니다.")

    return ArchiveExportResult(
        output_path=output_path,
        row_count=total_rows,
        chunk_count=chunk_count,
        min_timestamp=min_timestamp,
        max_timestamp=max_timestamp,
    )


def read_parquet_archive_stats(output_path: Path) -> ArchiveStats:
    parquet_file = pq.ParquetFile(output_path)
    timestamp_table = parquet_file.read(columns=["timestamp"])
    timestamp_series = timestamp_table.to_pandas()["timestamp"]
    if timestamp_series.empty:
        return ArchiveStats(
            row_count=0,
            min_timestamp="",
            max_timestamp="",
        )
    return ArchiveStats(
        row_count=parquet_file.metadata.num_rows,
        min_timestamp=_normalize_timestamp_text(timestamp_series.iloc[0]),
        max_timestamp=_normalize_timestamp_text(timestamp_series.iloc[-1]),
    )


def validate_archive_stats_match(
    source_stats: ArchiveStats,
    archive_stats: ArchiveStats,
) -> None:
    if source_stats.row_count != archive_stats.row_count:
        raise ValueError(
            "아카이브 row 수가 원본과 다릅니다. "
            f"source={source_stats.row_count}, archive={archive_stats.row_count}"
        )
    if source_stats.min_timestamp != archive_stats.min_timestamp:
        raise ValueError(
            "아카이브 최소 timestamp가 원본과 다릅니다. "
            f"source={source_stats.min_timestamp}, archive={archive_stats.min_timestamp}"
        )
    if source_stats.max_timestamp != archive_stats.max_timestamp:
        raise ValueError(
            "아카이브 최대 timestamp가 원본과 다릅니다. "
            f"source={source_stats.max_timestamp}, archive={archive_stats.max_timestamp}"
        )


def delete_archived_all_metrics(
    db_settings: DbConnectionSettings,
    before_datetime: datetime,
    expected_stats: ArchiveStats,
) -> ArchiveStats:
    connection = create_connection(db_settings)
    try:
        connection.set_session(
            isolation_level="SERIALIZABLE",
            autocommit=False,
        )
        current_stats = query_all_metrics_archive_stats_with_delete_lock(connection, before_datetime)
        validate_archive_stats_match(expected_stats, current_stats)

        delete_query = """
            WITH deleted_rows AS (
                DELETE FROM public.all_metrics
                WHERE "timestamp" < %s
                RETURNING "timestamp"
            )
            SELECT
                COUNT(*) AS row_count,
                MIN("timestamp") AS min_timestamp,
                MAX("timestamp") AS max_timestamp
            FROM deleted_rows
        """
        with connection.cursor() as cursor:
            cursor.execute(delete_query, (before_datetime,))
            deleted_row = cursor.fetchone()
        if deleted_row is None:
            raise ValueError("삭제 결과를 읽지 못했습니다.")

        deleted_stats = _build_archive_stats_from_row(deleted_row)
        validate_archive_stats_match(expected_stats, deleted_stats)
        remaining_stats = query_all_metrics_archive_stats(connection, before_datetime)
        if remaining_stats.row_count != 0:
            raise ValueError(
                "삭제 이후 cutoff 이전 row가 남아 있습니다. "
                f"before_datetime={before_datetime.isoformat()}, remaining_row_count={remaining_stats.row_count}"
            )
        connection.commit()
        return deleted_stats
    except psycopg2.Error as exc:
        connection.rollback()
        raise RuntimeError(
            "아카이브 삭제가 데이터베이스 오류로 실패했습니다. "
            f"host={db_settings.host}, port={db_settings.port}, dbname={db_settings.dbname}, "
            f"before_datetime={before_datetime.isoformat()}, pgcode={exc.pgcode}, pgerror={exc.pgerror}"
        ) from exc
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
