from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import TypedDict
from unittest.mock import patch

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.archive_metrics import DbConnectionSettings
import core.cycle_processing as cycle_processing


UTC = timezone.utc
KST = timezone(timedelta(hours=9))


class SmokeCheck(TypedDict):
    name: str
    ok: bool
    detail: str


class ProcessChunkMetrics(TypedDict):
    cycle_count: int
    cycle_summaries: list[dict[str, object]]


class RunRangeMetrics(TypedDict):
    read_sql_windows: list[list[str]]
    upsert_batch_sizes: list[int]
    progress_points: list[float]
    commit_count: int


class IncrementalScenarioMetrics(TypedDict):
    name: str
    last_processed: str
    query_start: str
    result: str
    error: str | None
    upsert_batch_sizes: list[int]
    upserted_cycles: list[dict[str, object]]
    progress_points: list[float]
    commit_count: int


class IncrementalChunkUpsertScenarioMetrics(TypedDict):
    name: str
    last_processed: str
    total_metric_rows: int
    total_upserted_cycles: int
    upsert_batch_sizes: list[int]
    upserted_cycles: list[dict[str, object]]
    commit_count: int


class IncrementalMetrics(TypedDict):
    scenarios: list[IncrementalScenarioMetrics]
    chunk_upsert_scenarios: list[IncrementalChunkUpsertScenarioMetrics]


class SmokeReport(TypedDict):
    checks: list[SmokeCheck]
    process_chunk: ProcessChunkMetrics
    run_range: RunRangeMetrics
    incremental: IncrementalMetrics


def build_check(name: str, ok: bool, detail: str) -> SmokeCheck:
    return {
        "name": name,
        "ok": ok,
        "detail": detail,
    }


def build_db_settings() -> DbConnectionSettings:
    return DbConnectionSettings(
        host="127.0.0.1",
        port=5432,
        user="postgres",
        password="postgres",
        dbname="postgres",
    )


def build_processor(log_messages: list[str], progress_points: list[float]) -> cycle_processing.CycleProcessor:
    return cycle_processing.CycleProcessor(
        db_settings=build_db_settings(),
        machine_id="MACHINE-A",
        log_callback=log_messages.append,
        progress_callback=progress_points.append,
        source_mode="legacy",
        algorithm_version="smoke-v1",
    )


def build_metrics_frame(
    timestamps: list[str],
    pressures: list[float],
    production_counters: list[int],
) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(timestamps, utc=True),
            "main_pressure": pressures,
            "production_counter": production_counters,
        }
    )


def build_process_chunk_metrics_frame() -> pd.DataFrame:
    return build_metrics_frame(
        [
            "2026-04-21T00:00:00+09:00",
            "2026-04-21T00:00:10+09:00",
            "2026-04-21T00:00:20+09:00",
            "2026-04-21T00:00:30+09:00",
            "2026-04-21T00:00:40+09:00",
            "2026-04-21T00:00:50+09:00",
            "2026-04-21T00:01:00+09:00",
            "2026-04-21T00:01:10+09:00",
            "2026-04-21T00:01:20+09:00",
        ],
        [0.0, 40.0, 120.0, 120.0, 0.0, 40.0, 80.0, 80.0, 0.0],
        [0, 0, 0, 0, 11, 0, 0, 0, 12],
    )


def build_work_log_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "work_log_id": [101],
            "machine_id": ["MACHINE-A"],
            "start_time": pd.to_datetime(["2026-04-20T14:00:00+00:00"], utc=True),
            "end_time": pd.to_datetime(["2026-04-20T16:00:00+00:00"], utc=True),
            "die_id": [501],
        }
    )


def summarize_cycle(cycle: tuple[object, ...]) -> dict[str, object]:
    return {
        "start_time": cycle[1].isoformat(),
        "end_time": cycle[2].isoformat(),
        "production_counter": cycle[3],
        "work_log_id": cycle[4],
        "duration_sec": cycle[5],
        "max_pressure": cycle[6],
        "is_valid": cycle[7],
    }


def run_process_chunk_smoke() -> tuple[list[SmokeCheck], ProcessChunkMetrics]:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    cycles = processor.process_chunk(
        build_process_chunk_metrics_frame(),
        build_work_log_frame(),
    )
    cycle_summaries = [summarize_cycle(cycle) for cycle in cycles]
    checks = [
        build_check(
            "process_chunk_cycle_count",
            len(cycles) == 2,
            json.dumps(cycle_summaries, ensure_ascii=False),
        ),
        build_check(
            "process_chunk_validity_split",
            len(cycles) == 2 and bool(cycles[0][7]) and not bool(cycles[1][7]),
            json.dumps(cycle_summaries, ensure_ascii=False),
        ),
        build_check(
            "process_chunk_work_log_mapping",
            len(cycles) == 2 and cycles[0][4] == 101 and cycles[1][4] == 101,
            json.dumps(cycle_summaries, ensure_ascii=False),
        ),
    ]
    metrics: ProcessChunkMetrics = {
        "cycle_count": len(cycles),
        "cycle_summaries": cycle_summaries,
    }
    return checks, metrics


class FakeCursor:
    def __init__(self, start_time: datetime, end_time: datetime):
        self.start_time = start_time
        self.end_time = end_time

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        _ = exc_type
        _ = exc
        _ = traceback
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        _ = query
        _ = params

    def fetchone(self) -> tuple[datetime, datetime]:
        return self.start_time, self.end_time


class FakeConnection:
    def __init__(self, start_time: datetime, end_time: datetime):
        self.start_time = start_time
        self.end_time = end_time
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.start_time, self.end_time)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


def build_range_chunk_frame(timestamps: list[str], pressures: list[float], counter: int) -> pd.DataFrame:
    return build_metrics_frame(
        timestamps,
        pressures,
        [0, 0, 0, 0, counter],
    )


def run_range_smoke() -> tuple[list[SmokeCheck], RunRangeMetrics]:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    start_time = datetime(2026, 4, 21, 0, 0, tzinfo=KST)
    end_time = datetime(2026, 4, 21, 1, 40, tzinfo=KST)
    fake_connection = FakeConnection(start_time, end_time)
    read_sql_windows: list[list[str]] = []
    upsert_batch_sizes: list[int] = []
    original_read_sql = cycle_processing.pd.read_sql
    original_get_db_connection = processor.get_db_connection
    original_load_work_logs = processor.load_work_logs
    original_upsert_cycles = processor._upsert_cycles

    def fake_read_sql(query: str, connection: object, params: tuple[object, ...] | None = None) -> pd.DataFrame:
        _ = query
        _ = connection
        if params is None:
            raise ValueError("params must not be None")
        range_start = params[0]
        range_end = params[1]
        if not isinstance(range_start, datetime) or not isinstance(range_end, datetime):
            raise TypeError("range params must be datetime")
        read_sql_windows.append([range_start.isoformat(), range_end.isoformat()])
        if range_start == start_time and range_end == datetime(2026, 4, 21, 1, 0, tzinfo=KST):
            return build_range_chunk_frame(
                [
                    "2026-04-21T00:00:00+09:00",
                    "2026-04-21T00:00:10+09:00",
                    "2026-04-21T00:00:20+09:00",
                    "2026-04-21T00:00:30+09:00",
                    "2026-04-21T00:00:40+09:00",
                ],
                [0.0, 40.0, 120.0, 120.0, 0.0],
                21,
            )
        if range_start == datetime(2026, 4, 21, 1, 0, tzinfo=KST) and range_end == datetime(2026, 4, 21, 1, 40, 1, tzinfo=KST):
            return build_range_chunk_frame(
                [
                    "2026-04-21T01:00:00+09:00",
                    "2026-04-21T01:10:00+09:00",
                    "2026-04-21T01:20:00+09:00",
                    "2026-04-21T01:30:00+09:00",
                    "2026-04-21T01:40:00+09:00",
                ],
                [0.0, 50.0, 130.0, 130.0, 0.0],
                22,
            )
        raise ValueError(f"unexpected read_sql window: {range_start.isoformat()} -> {range_end.isoformat()}")

    def fake_get_db_connection() -> FakeConnection:
        return fake_connection

    def fake_load_work_logs(start_from: object) -> pd.DataFrame:
        _ = start_from
        return build_work_log_frame()

    def fake_upsert_cycles(cursor: object, cycles: list[tuple[object, ...]]) -> None:
        _ = cursor
        upsert_batch_sizes.append(len(cycles))

    cycle_processing.pd.read_sql = fake_read_sql
    processor.get_db_connection = fake_get_db_connection
    processor.load_work_logs = fake_load_work_logs
    processor._upsert_cycles = fake_upsert_cycles

    try:
        processor.run_range("all", None)
    finally:
        cycle_processing.pd.read_sql = original_read_sql
        processor.get_db_connection = original_get_db_connection
        processor.load_work_logs = original_load_work_logs
        processor._upsert_cycles = original_upsert_cycles

    checks = [
        build_check(
            "run_range_chunk_windows",
            read_sql_windows
            == [
                [start_time.isoformat(), datetime(2026, 4, 21, 1, 0, tzinfo=KST).isoformat()],
                [
                    datetime(2026, 4, 21, 1, 0, tzinfo=KST).isoformat(),
                    datetime(2026, 4, 21, 1, 40, 1, tzinfo=KST).isoformat(),
                ],
            ],
            json.dumps(read_sql_windows, ensure_ascii=False),
        ),
        build_check(
            "run_range_upsert_batch_sizes",
            upsert_batch_sizes == [1, 1],
            json.dumps(upsert_batch_sizes, ensure_ascii=False),
        ),
        build_check(
            "run_range_progress_bounds",
            len(progress_points) >= 4 and min(progress_points) >= 0.0 and max(progress_points) <= 1.0 and progress_points[-1] == 1.0,
            json.dumps(progress_points, ensure_ascii=False),
        ),
        build_check(
            "run_range_commit_count",
            fake_connection.commit_count == 2 and fake_connection.rollback_count == 0 and fake_connection.closed,
            f"commits={fake_connection.commit_count}, rollbacks={fake_connection.rollback_count}, closed={fake_connection.closed}",
        ),
    ]
    metrics: RunRangeMetrics = {
        "read_sql_windows": read_sql_windows,
        "upsert_batch_sizes": upsert_batch_sizes,
        "progress_points": progress_points,
        "commit_count": fake_connection.commit_count,
    }
    return checks, metrics


class FakeIncrementalCursor:
    def __init__(self, last_processed: datetime | None):
        self.last_processed = last_processed
        self.executed_queries: list[str] = []

    def __enter__(self) -> "FakeIncrementalCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        _ = exc_type
        _ = exc
        _ = traceback
        return None

    def execute(self, query: str, params: tuple[object, ...] | None = None) -> None:
        _ = params
        self.executed_queries.append(query)

    def fetchone(self) -> tuple[datetime | None]:
        return (self.last_processed,)


class FakeIncrementalConnection:
    def __init__(self, last_processed: datetime | None):
        self.last_processed = last_processed
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> FakeIncrementalCursor:
        return FakeIncrementalCursor(self.last_processed)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


def run_incremental_scenario(
    name: str,
    last_processed: datetime,
    metric_chunks: list[pd.DataFrame],
) -> IncrementalScenarioMetrics:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    fake_connection = FakeIncrementalConnection(last_processed)
    query_starts: list[str] = []
    upsert_batches: list[list[tuple[object, ...]]] = []

    def fake_read_sql(
        query: str,
        connection: object,
        params: tuple[object, ...] | None = None,
        chunksize: int | None = None,
    ) -> object:
        _ = query
        _ = connection
        if params is None:
            raise ValueError("incremental metrics query params must not be None")
        if chunksize != cycle_processing.METRICS_CHUNK_SIZE:
            raise ValueError(f"unexpected chunksize: {chunksize}")
        query_start = params[0]
        if not isinstance(query_start, datetime):
            raise TypeError("incremental metrics query_start must be datetime")
        query_starts.append(query_start.isoformat())
        return iter(metric_chunks)

    def fake_get_db_connection() -> FakeIncrementalConnection:
        return fake_connection

    def fake_load_work_logs(start_from: object) -> pd.DataFrame:
        if start_from != last_processed - timedelta(minutes=1):
            raise ValueError(f"unexpected work log start: {start_from}")
        return build_work_log_frame()

    def fake_upsert_cycles(cursor: object, cycles: list[tuple[object, ...]]) -> None:
        _ = cursor
        upsert_batches.append(cycles)

    with (
        patch.object(cycle_processing.pd, "read_sql", new=fake_read_sql),
        patch.object(processor, "get_db_connection", new=fake_get_db_connection),
        patch.object(processor, "load_work_logs", new=fake_load_work_logs),
        patch.object(processor, "_upsert_cycles", new=fake_upsert_cycles),
    ):
        result = "failed"
        error_message: str | None = None
        try:
            result = processor.run_incremental()
        except Exception as error:
            error_message = f"{type(error).__name__}: {error}"

    upserted_cycles = [summarize_cycle(cycle) for batch in upsert_batches for cycle in batch]
    query_start = query_starts[0] if query_starts else ""
    return {
        "name": name,
        "last_processed": last_processed.isoformat(),
        "query_start": query_start,
        "result": result,
        "error": error_message,
        "upsert_batch_sizes": [len(batch) for batch in upsert_batches],
        "upserted_cycles": upserted_cycles,
        "progress_points": progress_points,
        "commit_count": fake_connection.commit_count,
    }


def run_incremental_chunk_upsert_scenario(
    name: str,
    last_processed: datetime,
    metric_chunks: list[pd.DataFrame],
) -> IncrementalChunkUpsertScenarioMetrics:
    log_messages: list[str] = []
    progress_points: list[float] = []
    _ = log_messages
    _ = progress_points
    processor = build_processor(log_messages, progress_points)
    fake_connection = FakeIncrementalConnection(last_processed)
    upsert_batches: list[list[tuple[object, ...]]] = []

    def fake_upsert_cycles(cursor: object, cycles: list[tuple[object, ...]]) -> None:
        _ = cursor
        upsert_batches.append(cycles)

    with patch.object(processor, "_upsert_cycles", new=fake_upsert_cycles):
        total_metric_rows, total_upserted_cycles = processor._upsert_incremental_cycles_by_chunk(
            cursor=object(),
            metrics_chunk_iter=iter(metric_chunks),
            work_log_frame=build_work_log_frame(),
            last_processed=last_processed,
        )

    upserted_cycles = [summarize_cycle(cycle) for batch in upsert_batches for cycle in batch]
    return {
        "name": name,
        "last_processed": last_processed.isoformat(),
        "total_metric_rows": total_metric_rows,
        "total_upserted_cycles": total_upserted_cycles,
        "upsert_batch_sizes": [len(batch) for batch in upsert_batches],
        "upserted_cycles": upserted_cycles,
        "commit_count": fake_connection.commit_count,
    }


def run_incremental_smoke() -> tuple[list[SmokeCheck], IncrementalMetrics]:
    boundary_closed = run_incremental_scenario(
        "boundary_closed_in_next_chunk",
        datetime(2026, 4, 20, 14, 0, 0, tzinfo=UTC),
        [
            build_metrics_frame(
                [
                    "2026-04-20T14:00:00+00:00",
                    "2026-04-20T14:00:10+00:00",
                    "2026-04-20T14:00:20+00:00",
                    "2026-04-20T14:00:30+00:00",
                ],
                [0.0, 40.0, 120.0, 120.0],
                [0, 0, 0, 0],
            ),
            build_metrics_frame(
                [
                    "2026-04-20T14:00:40+00:00",
                    "2026-04-20T14:00:50+00:00",
                ],
                [0.0, 0.0],
                [31, 31],
            ),
        ],
    )
    final_open_cycle = run_incremental_scenario(
        "final_open_cycle_not_upserted",
        datetime(2026, 4, 20, 14, 10, 0, tzinfo=UTC),
        [
            build_metrics_frame(
                [
                    "2026-04-20T14:10:00+00:00",
                    "2026-04-20T14:10:10+00:00",
                    "2026-04-20T14:10:20+00:00",
                    "2026-04-20T14:10:30+00:00",
                ],
                [0.0, 40.0, 120.0, 120.0],
                [0, 0, 0, 0],
            ),
        ],
    )
    overlap_filtered = run_incremental_scenario(
        "overlap_does_not_reupsert_processed_cycle",
        datetime(2026, 4, 20, 14, 1, 0, tzinfo=UTC),
        [
            build_metrics_frame(
                [
                    "2026-04-20T14:00:00+00:00",
                    "2026-04-20T14:00:10+00:00",
                    "2026-04-20T14:00:20+00:00",
                    "2026-04-20T14:00:30+00:00",
                    "2026-04-20T14:00:40+00:00",
                    "2026-04-20T14:00:50+00:00",
                    "2026-04-20T14:01:00+00:00",
                    "2026-04-20T14:01:10+00:00",
                    "2026-04-20T14:01:20+00:00",
                    "2026-04-20T14:01:30+00:00",
                    "2026-04-20T14:01:40+00:00",
                ],
                [0.0, 40.0, 120.0, 120.0, 0.0, 0.0, 0.0, 50.0, 130.0, 130.0, 0.0],
                [0, 0, 0, 0, 41, 41, 41, 41, 41, 41, 42],
            ),
        ],
    )
    chunked_multi_commit = run_incremental_chunk_upsert_scenario(
        "chunk_batches_commit_independently",
        datetime(2026, 4, 20, 13, 59, 0, tzinfo=UTC),
        [
            build_metrics_frame(
                [
                    "2026-04-20T14:00:00+00:00",
                    "2026-04-20T14:00:10+00:00",
                    "2026-04-20T14:00:20+00:00",
                    "2026-04-20T14:00:30+00:00",
                    "2026-04-20T14:00:40+00:00",
                ],
                [0.0, 40.0, 120.0, 120.0, 0.0],
                [0, 0, 0, 0, 51],
            ),
            build_metrics_frame(
                [
                    "2026-04-20T14:01:00+00:00",
                    "2026-04-20T14:01:10+00:00",
                    "2026-04-20T14:01:20+00:00",
                    "2026-04-20T14:01:30+00:00",
                    "2026-04-20T14:01:40+00:00",
                ],
                [0.0, 50.0, 130.0, 130.0, 0.0],
                [0, 0, 0, 0, 52],
            ),
        ],
    )
    chunked_overlap_filtered = run_incremental_chunk_upsert_scenario(
        "chunk_overlap_does_not_reupsert_processed_cycle",
        datetime(2026, 4, 20, 14, 1, 0, tzinfo=UTC),
        [
            build_metrics_frame(
                [
                    "2026-04-20T14:00:00+00:00",
                    "2026-04-20T14:00:10+00:00",
                    "2026-04-20T14:00:20+00:00",
                    "2026-04-20T14:00:30+00:00",
                    "2026-04-20T14:00:40+00:00",
                    "2026-04-20T14:01:00+00:00",
                ],
                [0.0, 40.0, 120.0, 120.0, 0.0, 0.0],
                [0, 0, 0, 0, 61, 61],
            ),
            build_metrics_frame(
                [
                    "2026-04-20T14:01:10+00:00",
                    "2026-04-20T14:01:20+00:00",
                    "2026-04-20T14:01:30+00:00",
                    "2026-04-20T14:01:40+00:00",
                ],
                [50.0, 130.0, 130.0, 0.0],
                [61, 61, 61, 62],
            ),
        ],
    )
    chunked_open_cycle = run_incremental_chunk_upsert_scenario(
        "chunk_final_open_cycle_not_upserted",
        datetime(2026, 4, 20, 14, 10, 0, tzinfo=UTC),
        [
            build_metrics_frame(
                [
                    "2026-04-20T14:10:00+00:00",
                    "2026-04-20T14:10:10+00:00",
                    "2026-04-20T14:10:20+00:00",
                    "2026-04-20T14:10:30+00:00",
                ],
                [0.0, 40.0, 120.0, 120.0],
                [0, 0, 0, 0],
            ),
        ],
    )
    checks = [
        build_check(
            "incremental_boundary_cycle_count",
            boundary_closed["result"] == "completed"
            and boundary_closed["upsert_batch_sizes"] == [1]
            and len(boundary_closed["upserted_cycles"]) == 1,
            json.dumps(boundary_closed, ensure_ascii=False),
        ),
        build_check(
            "incremental_final_open_cycle_not_upserted",
            final_open_cycle["result"] == "completed"
            and final_open_cycle["upsert_batch_sizes"] == []
            and final_open_cycle["commit_count"] == 0,
            json.dumps(final_open_cycle, ensure_ascii=False),
        ),
        build_check(
            "incremental_overlap_filters_processed_cycle",
            overlap_filtered["result"] == "completed"
            and overlap_filtered["query_start"] == "2026-04-20T14:00:00+00:00"
            and overlap_filtered["upsert_batch_sizes"] == [1]
            and len(overlap_filtered["upserted_cycles"]) == 1
            and overlap_filtered["upserted_cycles"][0]["end_time"] == "2026-04-20T14:01:40+00:00",
            json.dumps(overlap_filtered, ensure_ascii=False),
        ),
        build_check(
            "incremental_chunk_collects_upsert_batches",
            chunked_multi_commit["upsert_batch_sizes"] == [1, 1]
            and [cycle["end_time"] for cycle in chunked_multi_commit["upserted_cycles"]]
            == ["2026-04-20T14:00:40+00:00", "2026-04-20T14:01:40+00:00"],
            json.dumps(chunked_multi_commit, ensure_ascii=False),
        ),
        build_check(
            "incremental_chunk_overlap_filters_processed_cycle",
            chunked_overlap_filtered["upsert_batch_sizes"] == [1]
            and chunked_overlap_filtered["total_upserted_cycles"] == 1
            and len(chunked_overlap_filtered["upserted_cycles"]) == 1
            and chunked_overlap_filtered["upserted_cycles"][0]["end_time"] == "2026-04-20T14:01:40+00:00",
            json.dumps(chunked_overlap_filtered, ensure_ascii=False),
        ),
        build_check(
            "incremental_chunk_final_open_cycle_not_upserted",
            chunked_open_cycle["total_metric_rows"] == 4
            and chunked_open_cycle["upsert_batch_sizes"] == []
            and chunked_open_cycle["total_upserted_cycles"] == 0
            and chunked_open_cycle["commit_count"] == 0,
            json.dumps(chunked_open_cycle, ensure_ascii=False),
        ),
        build_check(
            "incremental_chunk_keeps_single_transaction",
            chunked_multi_commit["total_metric_rows"] == 10
            and chunked_multi_commit["total_upserted_cycles"] == 2
            and chunked_multi_commit["commit_count"] == 0,
            json.dumps(chunked_multi_commit, ensure_ascii=False),
        ),
    ]
    metrics: IncrementalMetrics = {
        "scenarios": [boundary_closed, final_open_cycle, overlap_filtered],
        "chunk_upsert_scenarios": [chunked_multi_commit, chunked_overlap_filtered, chunked_open_cycle],
    }
    return checks, metrics


def run_smoke() -> SmokeReport:
    process_chunk_checks, process_chunk_metrics = run_process_chunk_smoke()
    run_range_checks, run_range_metrics = run_range_smoke()
    incremental_checks, incremental_metrics = run_incremental_smoke()
    return {
        "checks": process_chunk_checks + run_range_checks + incremental_checks,
        "process_chunk": process_chunk_metrics,
        "run_range": run_range_metrics,
        "incremental": incremental_metrics,
    }


def main() -> int:
    report = run_smoke()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    failed_checks = [check for check in report["checks"] if not check["ok"]]
    if failed_checks:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
