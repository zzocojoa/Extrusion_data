from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import TypedDict

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.archive_metrics import DbConnectionSettings
import core.cycle_processing as cycle_processing


UTC = timezone.utc


class SmokeCheck(TypedDict):
    name: str
    ok: bool
    detail: str


class CollectMetrics(TypedDict):
    total_metric_rows: int
    collected_cycles: list[dict[str, object]]


class LongCycleMetrics(TypedDict):
    default_total_metric_rows: int
    compact_total_metric_rows: int
    default_cycles: list[dict[str, object]]
    compact_carry_cycles: list[dict[str, object]]


class IncrementalCollectedMetrics(TypedDict):
    total_metric_rows: int
    upsert_candidate_cycles: list[dict[str, object]]


class IncrementalBatchUpsertMetrics(TypedDict):
    total_metric_rows: int
    total_upserted_cycles: int
    upserted_cycles: list[dict[str, object]]
    upsert_batch_sizes: list[int]
    commit_count: int


class IncrementalMetrics(TypedDict):
    collected_reference: IncrementalCollectedMetrics
    batch_upsert_by_chunk: IncrementalBatchUpsertMetrics


class SmokeReport(TypedDict):
    checks: list[SmokeCheck]
    collect_metrics: CollectMetrics
    long_cycle_metrics: LongCycleMetrics
    incremental_metrics: IncrementalMetrics


CompactCarryState = tuple[pd.Timestamp, float]


class FakeCursor:
    def __init__(self, last_processed: datetime):
        self.last_processed = last_processed

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

    def fetchone(self) -> tuple[datetime]:
        return (self.last_processed,)


class FakeConnection:
    def __init__(self, last_processed: datetime):
        self.last_processed = last_processed
        self.commit_count = 0
        self.rollback_count = 0
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self.last_processed)

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def close(self) -> None:
        self.closed = True


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


def build_metrics_frame(rows: list[tuple[str, float, int]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime([row[0] for row in rows], utc=True),
            "main_pressure": [row[1] for row in rows],
            "production_counter": [row[2] for row in rows],
        }
    )


def build_work_log_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "work_log_id": [101],
            "machine_id": ["MACHINE-A"],
            "start_time": pd.to_datetime(["2026-04-21T09:00:00+00:00"], utc=True),
            "end_time": pd.to_datetime(["2026-04-21T12:00:00+00:00"], utc=True),
            "die_id": [501],
        }
    )


def summarize_cycles(cycles: list[tuple[object, ...]]) -> list[dict[str, object]]:
    return [
        {
            "start_time": cycle[1].isoformat(),
            "end_time": cycle[2].isoformat(),
            "production_counter": cycle[3],
            "work_log_id": cycle[4],
            "duration_sec": cycle[5],
            "max_pressure": cycle[6],
            "is_valid": cycle[7],
        }
        for cycle in cycles
    ]


def summarize_cycle_identity(cycles: list[tuple[object, ...]]) -> list[dict[str, object]]:
    return [
        {
            "start_time": cycle[1].isoformat(),
            "end_time": cycle[2].isoformat(),
            "production_counter": cycle[3],
            "max_pressure": cycle[6],
        }
        for cycle in cycles
    ]


def flatten_cycle_batches(cycle_batches: list[list[tuple[object, ...]]]) -> list[tuple[object, ...]]:
    return [cycle for cycle_batch in cycle_batches for cycle in cycle_batch]


def run_collect_incremental_boundary_smoke() -> tuple[list[SmokeCheck], CollectMetrics]:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    metrics_chunks = [
        build_metrics_frame(
            [
                ("2026-04-21T10:00:30+00:00", 120.0, 0),
                ("2026-04-21T10:01:00+00:00", 120.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:02:00+00:00", 120.0, 0),
                ("2026-04-21T10:03:00+00:00", 0.0, 11),
                ("2026-04-21T10:04:00+00:00", 40.0, 0),
                ("2026-04-21T10:05:00+00:00", 150.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:06:00+00:00", 150.0, 0),
            ]
        ),
    ]
    cycles, total_metric_rows = processor._collect_incremental_cycles(
        metrics_chunk_iter=metrics_chunks,
        work_log_frame=build_work_log_frame(),
    )
    collected_cycles = summarize_cycles(cycles)
    checks = [
        build_check(
            "collect_incremental_total_metric_rows",
            total_metric_rows == 7,
            json.dumps({"total_metric_rows": total_metric_rows}, ensure_ascii=False),
        ),
        build_check(
            "collect_incremental_recovers_initial_active_cycle",
            len(cycles) == 1
            and cycles[0][1].isoformat() == "2026-04-21T10:00:30+00:00"
            and cycles[0][2].isoformat() == "2026-04-21T10:03:00+00:00",
            json.dumps(collected_cycles, ensure_ascii=False),
        ),
        build_check(
            "collect_incremental_skips_last_open_cycle",
            len(cycles) == 1 and cycles[0][3] == 11,
            json.dumps(collected_cycles, ensure_ascii=False),
        ),
    ]
    metrics: CollectMetrics = {
        "total_metric_rows": total_metric_rows,
        "collected_cycles": collected_cycles,
    }
    return checks, metrics


def build_long_active_metrics_chunks() -> list[pd.DataFrame]:
    return [
        build_metrics_frame(
            [
                ("2026-04-21T10:00:00+00:00", 0.0, 0),
                ("2026-04-21T10:00:30+00:00", 120.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:01:00+00:00", 135.0, 0),
                ("2026-04-21T10:01:30+00:00", 132.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:02:00+00:00", 155.0, 0),
                ("2026-04-21T10:02:30+00:00", 149.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:03:00+00:00", 140.0, 0),
                ("2026-04-21T10:03:30+00:00", 0.0, 21),
            ]
        ),
    ]


def build_cycle_tuple(
    processor: cycle_processing.CycleProcessor,
    start_time: pd.Timestamp,
    end_time: pd.Timestamp,
    production_counter: object,
    work_log_id: int | None,
    max_pressure: float,
) -> tuple[object, ...]:
    duration = (end_time - start_time).total_seconds()
    return (
        processor.machine_id,
        start_time,
        end_time,
        int(production_counter) if pd.notnull(production_counter) else None,
        work_log_id,
        float(duration),
        float(max_pressure),
        bool(
            duration >= cycle_processing.MIN_DURATION
            and max_pressure >= cycle_processing.MIN_MAX_PRESSURE
        ),
        False,
        processor.source_mode,
        processor.algorithm_version,
    )


def collect_cycles_with_compact_carry(
    processor: cycle_processing.CycleProcessor,
    metrics_chunks: list[pd.DataFrame],
    work_log_frame: pd.DataFrame,
) -> tuple[list[tuple[object, ...]], int]:
    work_log_lookup = processor._build_work_log_lookup(work_log_frame)
    work_log_pointer = 0
    total_metric_rows = 0
    open_cycle_state: CompactCarryState | None = None
    collected_cycles: list[tuple[object, ...]] = []

    for metrics_chunk in metrics_chunks:
        if metrics_chunk.empty:
            continue

        total_metric_rows += len(metrics_chunk)
        prepared_frame = processor._prepare_metrics_frame(
            metrics_chunk.loc[:, cycle_processing.METRIC_COLUMNS].copy(),
        )

        for row in prepared_frame.itertuples(index=False):
            timestamp = row.timestamp
            main_pressure = float(row.main_pressure)
            production_counter = row.production_counter
            is_active = bool(row.is_active)

            if open_cycle_state is None:
                if is_active:
                    open_cycle_state = (timestamp, main_pressure)
                continue

            if is_active:
                open_cycle_state = (
                    open_cycle_state[0],
                    max(open_cycle_state[1], main_pressure),
                )
                continue

            work_log_id, work_log_pointer = processor._match_work_log_id(
                start_time=open_cycle_state[0],
                work_log_lookup=work_log_lookup,
                work_log_pointer=work_log_pointer,
            )
            collected_cycles.append(
                build_cycle_tuple(
                    processor=processor,
                    start_time=open_cycle_state[0],
                    end_time=timestamp,
                    production_counter=production_counter,
                    work_log_id=work_log_id,
                    max_pressure=open_cycle_state[1],
                )
            )
            open_cycle_state = None

    return collected_cycles, total_metric_rows


def run_long_cycle_compact_carry_smoke() -> tuple[list[SmokeCheck], LongCycleMetrics]:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    metrics_chunks = build_long_active_metrics_chunks()
    work_log_frame = build_work_log_frame()

    default_cycles, default_total_metric_rows = processor._collect_incremental_cycles(
        metrics_chunk_iter=metrics_chunks,
        work_log_frame=work_log_frame,
    )
    compact_cycles, compact_total_metric_rows = collect_cycles_with_compact_carry(
        processor=processor,
        metrics_chunks=build_long_active_metrics_chunks(),
        work_log_frame=work_log_frame,
    )

    default_cycle_identity = summarize_cycle_identity(default_cycles)
    compact_cycle_identity = summarize_cycle_identity(compact_cycles)
    checks = [
        build_check(
            "collect_incremental_long_cycle_spans_four_chunks",
            default_total_metric_rows == 8
            and len(default_cycles) == 1
            and default_cycle_identity
            == [
                {
                    "start_time": "2026-04-21T10:00:30+00:00",
                    "end_time": "2026-04-21T10:03:30+00:00",
                    "production_counter": 21,
                    "max_pressure": 155.0,
                }
            ],
            json.dumps(
                {
                    "default_total_metric_rows": default_total_metric_rows,
                    "default_cycle_identity": default_cycle_identity,
                },
                ensure_ascii=False,
            ),
        ),
        build_check(
            "collect_incremental_compact_carry_matches_default_identity",
            compact_total_metric_rows == default_total_metric_rows
            and compact_cycle_identity == default_cycle_identity,
            json.dumps(
                {
                    "default_cycle_identity": default_cycle_identity,
                    "compact_cycle_identity": compact_cycle_identity,
                    "default_total_metric_rows": default_total_metric_rows,
                    "compact_total_metric_rows": compact_total_metric_rows,
                },
                ensure_ascii=False,
            ),
        ),
    ]
    metrics: LongCycleMetrics = {
        "default_total_metric_rows": default_total_metric_rows,
        "compact_total_metric_rows": compact_total_metric_rows,
        "default_cycles": summarize_cycles(default_cycles),
        "compact_carry_cycles": summarize_cycles(compact_cycles),
    }
    return checks, metrics


def collect_incremental_reference_metrics(last_processed: datetime) -> IncrementalCollectedMetrics:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    collected_cycles, total_metric_rows = processor._collect_incremental_cycles(
        metrics_chunk_iter=iter(build_long_active_metrics_chunks()),
        work_log_frame=build_work_log_frame(),
    )
    upsert_candidate_cycles = [cycle for cycle in collected_cycles if cycle[2] > last_processed]
    return {
        "total_metric_rows": total_metric_rows,
        "upsert_candidate_cycles": summarize_cycles(upsert_candidate_cycles),
    }


def collect_batch_upsert_metrics(last_processed: datetime) -> IncrementalBatchUpsertMetrics:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    fake_connection = FakeConnection(last_processed)
    upsert_batches: list[list[tuple[object, ...]]] = []
    original_upsert_cycles = processor._upsert_cycles

    def fake_upsert_cycles(cursor: object, cycles: list[tuple[object, ...]]) -> None:
        _ = cursor
        upsert_batches.append(list(cycles))

    processor._upsert_cycles = fake_upsert_cycles

    try:
        with fake_connection.cursor() as cursor:
            total_metric_rows, total_upserted_cycles = processor._upsert_incremental_cycles_by_chunk(
                cursor=cursor,
                metrics_chunk_iter=iter(build_long_active_metrics_chunks()),
                work_log_frame=build_work_log_frame(),
                last_processed=last_processed,
            )
    finally:
        processor._upsert_cycles = original_upsert_cycles
        fake_connection.close()

    upserted_cycles = flatten_cycle_batches(upsert_batches)
    return {
        "total_metric_rows": total_metric_rows,
        "total_upserted_cycles": total_upserted_cycles,
        "upserted_cycles": summarize_cycles(upserted_cycles),
        "upsert_batch_sizes": [len(cycle_batch) for cycle_batch in upsert_batches],
        "commit_count": fake_connection.commit_count,
    }


def run_incremental_boundary_smoke() -> tuple[list[SmokeCheck], IncrementalMetrics]:
    last_processed = datetime(2026, 4, 21, 10, 0, 0, tzinfo=UTC)
    collected_reference_metrics = collect_incremental_reference_metrics(last_processed)
    batch_upsert_metrics = collect_batch_upsert_metrics(last_processed)
    checks = [
        build_check(
            "collect_incremental_reference_builds_same_long_cycle_candidate",
            collected_reference_metrics["total_metric_rows"] == 8
            and collected_reference_metrics["upsert_candidate_cycles"]
            == [
                {
                    "start_time": "2026-04-21T10:00:30+00:00",
                    "end_time": "2026-04-21T10:03:30+00:00",
                    "production_counter": 21,
                    "work_log_id": 101,
                    "duration_sec": 180.0,
                    "max_pressure": 155.0,
                    "is_valid": True,
                }
            ],
            json.dumps(collected_reference_metrics, ensure_ascii=False),
        ),
        build_check(
            "batch_upsert_by_chunk_matches_collected_reference",
            batch_upsert_metrics["upserted_cycles"] == collected_reference_metrics["upsert_candidate_cycles"],
            json.dumps(
                {
                    "collected_reference": collected_reference_metrics,
                    "batch_upsert_by_chunk": batch_upsert_metrics,
                },
                ensure_ascii=False,
            ),
        ),
        build_check(
            "batch_upsert_by_chunk_observations_are_reasonable",
            batch_upsert_metrics["total_metric_rows"] == 8
            and batch_upsert_metrics["total_upserted_cycles"] == 1
            and batch_upsert_metrics["upsert_batch_sizes"] == [1]
            and batch_upsert_metrics["commit_count"] == 0,
            json.dumps(batch_upsert_metrics, ensure_ascii=False),
        ),
    ]
    metrics: IncrementalMetrics = {
        "collected_reference": collected_reference_metrics,
        "batch_upsert_by_chunk": batch_upsert_metrics,
    }
    return checks, metrics


def run_smoke() -> SmokeReport:
    collect_checks, collect_metrics = run_collect_incremental_boundary_smoke()
    long_cycle_checks, long_cycle_metrics = run_long_cycle_compact_carry_smoke()
    incremental_checks, incremental_metrics = run_incremental_boundary_smoke()
    return {
        "checks": collect_checks + long_cycle_checks + incremental_checks,
        "collect_metrics": collect_metrics,
        "long_cycle_metrics": long_cycle_metrics,
        "incremental_metrics": incremental_metrics,
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
