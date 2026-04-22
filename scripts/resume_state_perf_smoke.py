import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import TypedDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core import state as core_state


REPEAT_COUNT = 1000
FIXED_OFFSET = 12345


class SmokeCheck(TypedDict):
    name: str
    ok: bool
    detail: str


class SmokeMetrics(TypedDict):
    repeat_count: int
    elapsed_ms_total: float
    elapsed_ms_per_write: float
    db_size_bytes: int


class SmokeReport(TypedDict):
    checks: list[SmokeCheck]
    metrics: SmokeMetrics


def build_check(name: str, ok: bool, detail: str) -> SmokeCheck:
    return {
        "name": name,
        "ok": ok,
        "detail": detail,
    }


def build_temp_file(workspace: Path) -> Path:
    target_path = workspace / "plc-folder" / "sample.csv"
    target_path.parent.mkdir(parents=True, exist_ok=True)
    target_path.write_text("timestamp,value\n2026-04-21T00:00:00Z,1\n", encoding="utf-8")
    return target_path


def run_smoke() -> SmokeReport:
    temp_root = Path(tempfile.mkdtemp(prefix="resume-state-smoke-"))
    original_appdata = os.environ.get("APPDATA")

    try:
        os.environ["APPDATA"] = str(temp_root)
        sample_path = build_temp_file(temp_root)
        resume_key = core_state.build_file_state_key("folder", "sample.csv", str(sample_path))

        core_state.set_resume_offset(resume_key, FIXED_OFFSET, None)

        db_path = Path(core_state.get_db_path(None))
        db_size_before = db_path.stat().st_size

        started_at = time.perf_counter()
        for _ in range(REPEAT_COUNT):
            core_state.set_resume_offset(resume_key, FIXED_OFFSET, None)
        elapsed_ms_total = (time.perf_counter() - started_at) * 1000.0

        db_size_after = db_path.stat().st_size

        resume_map_before_clear = core_state.load_resume(None)
        core_state.mark_file_completed("folder", "sample.csv", str(sample_path), None, None)
        processed_keys = core_state.load_processed(None)
        resume_map_after_clear = core_state.load_resume(None)
        failed_retry_set_after_clear = core_state.load_failed_retry_set(None)

        checks = [
            build_check(
                "processed_marker_stable",
                processed_keys == {resume_key, "folder/sample.csv"},
                json.dumps(sorted(processed_keys), ensure_ascii=False),
            ),
            build_check(
                "db_size_nonshrinking",
                db_size_after >= db_size_before,
                f"before={db_size_before}, after={db_size_after}",
            ),
            build_check(
                "resume_value_persisted",
                resume_map_before_clear.get(resume_key) == FIXED_OFFSET,
                json.dumps(resume_map_before_clear, ensure_ascii=False, sort_keys=True),
            ),
            build_check(
                "resume_value_cleared",
                resume_map_after_clear == {},
                json.dumps(resume_map_after_clear, ensure_ascii=False, sort_keys=True),
            ),
            build_check(
                "failed_retry_set_empty_after_clear",
                failed_retry_set_after_clear == (),
                json.dumps(failed_retry_set_after_clear, ensure_ascii=False),
            ),
        ]

        metrics: SmokeMetrics = {
            "repeat_count": REPEAT_COUNT,
            "elapsed_ms_total": round(elapsed_ms_total, 3),
            "elapsed_ms_per_write": round(elapsed_ms_total / float(REPEAT_COUNT), 6),
            "db_size_bytes": db_size_after,
        }
        return {
            "checks": checks,
            "metrics": metrics,
        }
    finally:
        if original_appdata is None:
            os.environ.pop("APPDATA", None)
        else:
            os.environ["APPDATA"] = original_appdata
        shutil.rmtree(temp_root, ignore_errors=True)


def main() -> int:
    report = run_smoke()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    failed_checks = [check for check in report["checks"] if not check["ok"]]
    if failed_checks:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
