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


class CycleSummary(TypedDict):
    start_time: str
    end_time: str
    production_counter: int | None
    work_log_id: int | None
    duration_sec: float
    max_pressure: float
    is_valid: bool


class ChunkObservation(TypedDict):
    chunk_index: int
    row_count: int
    completed_cycle_count: int
    completed_cycles: list[CycleSummary]
    pending_open_cycle_start: str | None
    pending_open_cycle_max_pressure: float | None


class UpsertObservation(TypedDict):
    chunk_index: int
    cycle_count: int
    cycles: list[CycleSummary]


class BatchUpsertMetrics(TypedDict):
    total_metric_rows: int
    total_upserted_cycles: int
    commit_count: int
    rollback_count: int
    chunk_observations: list[ChunkObservation]
    upsert_observations: list[UpsertObservation]
    upserted_cycles: list[CycleSummary]


class ReferenceMetrics(TypedDict):
    total_metric_rows: int
    collected_cycles: list[CycleSummary]


class SmokeReport(TypedDict):
    checks: list[SmokeCheck]
    batch_upsert: BatchUpsertMetrics
    reference_collect: ReferenceMetrics


class CycleIdentity(TypedDict):
    start_time: str
    end_time: str
    production_counter: int | None
    max_pressure: float
    is_valid: bool


class FakeCursor:
    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        _ = exc_type
        _ = exc
        _ = traceback


class FakeConnection:
    def __init__(self) -> None:
        self.commit_count: int = 0
        self.rollback_count: int = 0

    def commit(self) -> None:
        self.commit_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1


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
        algorithm_version="smoke-batch-upsert-v1",
    )


def build_metrics_frame(rows: list[tuple[str, float, int]]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime([row[0] for row in rows], utc=True),
            "main_pressure": [row[1] for row in rows],
            "production_counter": [row[2] for row in rows],
        }
    )


def build_metrics_chunks() -> list[pd.DataFrame]:
    return [
        build_metrics_frame(
            [
                ("2026-04-21T10:00:00+00:00", 0.0, 0),
                ("2026-04-21T10:00:30+00:00", 40.0, 0),
                ("2026-04-21T10:01:00+00:00", 120.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:01:30+00:00", 0.0, 11),
                ("2026-04-21T10:02:00+00:00", 50.0, 0),
                ("2026-04-21T10:02:30+00:00", 150.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:03:00+00:00", 0.0, 12),
                ("2026-04-21T10:03:30+00:00", 60.0, 0),
                ("2026-04-21T10:04:00+00:00", 130.0, 0),
            ]
        ),
        build_metrics_frame(
            [
                ("2026-04-21T10:04:30+00:00", 160.0, 0),
            ]
        ),
    ]


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


def clone_metrics_chunks(metrics_chunks: list[pd.DataFrame]) -> list[pd.DataFrame]:
    return [chunk.copy(deep=True) for chunk in metrics_chunks]


def summarize_cycle(cycle: tuple[object, ...]) -> CycleSummary:
    return {
        "start_time": cycle[1].isoformat(),
        "end_time": cycle[2].isoformat(),
        "production_counter": cycle[3],
        "work_log_id": cycle[4],
        "duration_sec": float(cycle[5]),
        "max_pressure": float(cycle[6]),
        "is_valid": bool(cycle[7]),
    }


def summarize_cycles(cycles: list[tuple[object, ...]]) -> list[CycleSummary]:
    return [summarize_cycle(cycle) for cycle in cycles]


def extract_cycle_identity(cycles: list[CycleSummary]) -> list[CycleIdentity]:
    return [
        {
            "start_time": cycle["start_time"],
            "end_time": cycle["end_time"],
            "production_counter": cycle["production_counter"],
            "max_pressure": cycle["max_pressure"],
            "is_valid": cycle["is_valid"],
        }
        for cycle in cycles
    ]


def run_batch_upsert_smoke() -> tuple[list[SmokeCheck], BatchUpsertMetrics, ReferenceMetrics]:
    log_messages: list[str] = []
    progress_points: list[float] = []
    processor = build_processor(log_messages, progress_points)
    reference_processor = build_processor([], [])
    metrics_chunks = build_metrics_chunks()
    work_log_frame = build_work_log_frame()
    last_processed = datetime(2026, 4, 21, 9, 59, 0, tzinfo=UTC)
    fake_cursor = FakeCursor()
    fake_connection = FakeConnection()
    chunk_observations: list[ChunkObservation] = []
    upsert_observations: list[UpsertObservation] = []
    current_chunk_index: int = 0

    original_process_incremental_chunk = processor._process_incremental_chunk
    original_upsert_cycles = processor._upsert_cycles

    def wrapped_process_incremental_chunk(
        metrics_chunk: pd.DataFrame,
        work_log_lookup: cycle_processing.WorkLogLookup,
        work_log_pointer: int,
        previous_is_active: bool | None,
        open_cycle_state: cycle_processing.OpenCycleState | None,
    ) -> tuple[list[tuple[object, ...]], int, bool | None, cycle_processing.OpenCycleState | None]:
        nonlocal current_chunk_index
        next_chunk_index = len(chunk_observations) + 1
        result = original_process_incremental_chunk(
            metrics_chunk=metrics_chunk,
            work_log_lookup=work_log_lookup,
            work_log_pointer=work_log_pointer,
            previous_is_active=previous_is_active,
            open_cycle_state=open_cycle_state,
        )
        chunk_cycles, _, _, next_open_cycle_state = result
        current_chunk_index = next_chunk_index
        pending_open_cycle_start: str | None = None
        pending_open_cycle_max_pressure: float | None = None
        if next_open_cycle_state is not None:
            pending_open_cycle_start = next_open_cycle_state[0].isoformat()
            pending_open_cycle_max_pressure = float(next_open_cycle_state[1])
        chunk_observations.append(
            {
                "chunk_index": next_chunk_index,
                "row_count": len(metrics_chunk),
                "completed_cycle_count": len(chunk_cycles),
                "completed_cycles": summarize_cycles(chunk_cycles),
                "pending_open_cycle_start": pending_open_cycle_start,
                "pending_open_cycle_max_pressure": pending_open_cycle_max_pressure,
            }
        )
        return result

    def recording_upsert_cycles(cursor: object, cycles: list[tuple[object, ...]]) -> None:
        _ = cursor
        upsert_observations.append(
            {
                "chunk_index": current_chunk_index,
                "cycle_count": len(cycles),
                "cycles": summarize_cycles(cycles),
            }
        )

    processor._process_incremental_chunk = wrapped_process_incremental_chunk
    processor._upsert_cycles = recording_upsert_cycles

    try:
        total_metric_rows, total_upserted_cycles = processor._upsert_incremental_cycles_by_chunk(
            cursor=fake_cursor,
            metrics_chunk_iter=iter(clone_metrics_chunks(metrics_chunks)),
            work_log_frame=work_log_frame.copy(deep=True),
            last_processed=last_processed,
        )
    finally:
        processor._process_incremental_chunk = original_process_incremental_chunk
        processor._upsert_cycles = original_upsert_cycles

    reference_cycles, reference_total_metric_rows = reference_processor._collect_incremental_cycles(
        metrics_chunk_iter=iter(clone_metrics_chunks(metrics_chunks)),
        work_log_frame=work_log_frame.copy(deep=True),
    )

    upserted_cycles: list[CycleSummary] = [
        cycle_summary
        for observation in upsert_observations
        for cycle_summary in observation["cycles"]
    ]
    batch_upsert_metrics: BatchUpsertMetrics = {
        "total_metric_rows": total_metric_rows,
        "total_upserted_cycles": total_upserted_cycles,
        "commit_count": fake_connection.commit_count,
        "rollback_count": fake_connection.rollback_count,
        "chunk_observations": chunk_observations,
        "upsert_observations": upsert_observations,
        "upserted_cycles": upserted_cycles,
    }
    reference_metrics: ReferenceMetrics = {
        "total_metric_rows": reference_total_metric_rows,
        "collected_cycles": summarize_cycles(reference_cycles),
    }

    chunk_completion_counts: list[int] = [observation["completed_cycle_count"] for observation in chunk_observations]
    upsert_chunk_indexes: list[int] = [observation["chunk_index"] for observation in upsert_observations]
    upsert_cycle_counts: list[int] = [observation["cycle_count"] for observation in upsert_observations]
    reference_cycle_identity = extract_cycle_identity(reference_metrics["collected_cycles"])
    upsert_cycle_identity = extract_cycle_identity(upserted_cycles)
    final_chunk_observation = chunk_observations[-1]

    checks = [
        build_check(
            "batch_upsert_total_metric_rows",
            total_metric_rows == 10 and reference_total_metric_rows == 10,
            json.dumps(
                {
                    "batch_upsert_total_metric_rows": total_metric_rows,
                    "reference_total_metric_rows": reference_total_metric_rows,
                },
                ensure_ascii=False,
            ),
        ),
        build_check(
            "batch_upsert_emits_completed_cycles_at_chunk_end",
            upsert_chunk_indexes == [2, 3] and upsert_cycle_counts == [1, 1],
            json.dumps(upsert_observations, ensure_ascii=False),
        ),
        build_check(
            "batch_upsert_holds_final_open_cycle",
            final_chunk_observation["completed_cycle_count"] == 0
            and final_chunk_observation["pending_open_cycle_start"] == "2026-04-21T10:03:30+00:00"
            and total_upserted_cycles == 2,
            json.dumps(final_chunk_observation, ensure_ascii=False),
        ),
        build_check(
            "batch_upsert_does_not_accumulate_prior_cycle_lists",
            chunk_completion_counts == [0, 1, 1, 0] and upsert_cycle_counts == [1, 1],
            json.dumps(
                {
                    "chunk_completion_counts": chunk_completion_counts,
                    "upsert_cycle_counts": upsert_cycle_counts,
                },
                ensure_ascii=False,
            ),
        ),
        build_check(
            "batch_upsert_matches_reference_collect_correctness",
            upsert_cycle_identity == reference_cycle_identity,
            json.dumps(
                {
                    "upserted_cycles": upsert_cycle_identity,
                    "reference_cycles": reference_cycle_identity,
                },
                ensure_ascii=False,
            ),
        ),
        build_check(
            "batch_upsert_keeps_single_transaction_open",
            fake_connection.commit_count == 0 and fake_connection.rollback_count == 0,
            json.dumps(
                {
                    "commit_count": fake_connection.commit_count,
                    "rollback_count": fake_connection.rollback_count,
                },
                ensure_ascii=False,
            ),
        ),
    ]
    return checks, batch_upsert_metrics, reference_metrics


def run_smoke() -> SmokeReport:
    checks, batch_upsert_metrics, reference_metrics = run_batch_upsert_smoke()
    return {
        "checks": checks,
        "batch_upsert": batch_upsert_metrics,
        "reference_collect": reference_metrics,
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
