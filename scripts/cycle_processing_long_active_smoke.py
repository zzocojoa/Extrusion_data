from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import TypedDict

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.archive_metrics import DbConnectionSettings
import core.cycle_processing as cycle_processing


class SmokeCheck(TypedDict):
    name: str
    ok: bool
    detail: str


class SmokeMetrics(TypedDict):
    total_metric_rows: int
    cycle_summaries: list[dict[str, object]]


class SmokeReport(TypedDict):
    checks: list[SmokeCheck]
    metrics: SmokeMetrics


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


def build_processor() -> cycle_processing.CycleProcessor:
    return cycle_processing.CycleProcessor(
        db_settings=build_db_settings(),
        machine_id="MACHINE-A",
        log_callback=None,
        progress_callback=None,
        source_mode="legacy",
        algorithm_version="smoke-v1",
    )


def build_metrics_chunk(rows: list[tuple[str, float, int]]) -> pd.DataFrame:
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
            "start_time": pd.to_datetime(["2026-04-21T08:00:00+00:00"], utc=True),
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


def run_smoke() -> SmokeReport:
    processor = build_processor()
    metrics_chunks = [
        build_metrics_chunk(
            [
                ("2026-04-21T10:00:00+00:00", 0.0, 0),
                ("2026-04-21T10:00:10+00:00", 40.0, 0),
                ("2026-04-21T10:00:20+00:00", 60.0, 0),
            ]
        ),
        build_metrics_chunk(
            [
                ("2026-04-21T10:00:30+00:00", 180.0, 0),
                ("2026-04-21T10:00:40+00:00", 150.0, 0),
            ]
        ),
        build_metrics_chunk(
            [
                ("2026-04-21T10:00:50+00:00", 140.0, 0),
                ("2026-04-21T10:01:00+00:00", 130.0, 0),
            ]
        ),
        build_metrics_chunk(
            [
                ("2026-04-21T10:01:10+00:00", 0.0, 77),
            ]
        ),
    ]
    cycles, total_metric_rows = processor._collect_incremental_cycles(
        metrics_chunk_iter=metrics_chunks,
        work_log_frame=build_work_log_frame(),
    )
    cycle_summaries = summarize_cycles(cycles)
    checks = [
        build_check(
            "long_active_single_cycle_count",
            len(cycles) == 1,
            json.dumps(cycle_summaries, ensure_ascii=False),
        ),
        build_check(
            "long_active_cycle_bounds",
            len(cycles) == 1
            and cycles[0][1].isoformat() == "2026-04-21T10:00:10+00:00"
            and cycles[0][2].isoformat() == "2026-04-21T10:01:10+00:00",
            json.dumps(cycle_summaries, ensure_ascii=False),
        ),
        build_check(
            "long_active_cycle_aggregates",
            len(cycles) == 1
            and cycles[0][3] == 77
            and float(cycles[0][6]) == 180.0
            and bool(cycles[0][7]),
            json.dumps(cycle_summaries, ensure_ascii=False),
        ),
        build_check(
            "long_active_total_metric_rows",
            total_metric_rows == 8,
            json.dumps({"total_metric_rows": total_metric_rows}, ensure_ascii=False),
        ),
    ]
    metrics: SmokeMetrics = {
        "total_metric_rows": total_metric_rows,
        "cycle_summaries": cycle_summaries,
    }
    return {
        "checks": checks,
        "metrics": metrics,
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
