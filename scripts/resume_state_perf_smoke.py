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
    manifest_size_bytes: int
    log_size_bytes: int
    resume_size_bytes: int


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


def read_text_lines(path: Path) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8").splitlines()


def run_smoke() -> SmokeReport:
    temp_root = Path(tempfile.mkdtemp(prefix="resume-state-smoke-"))
    original_appdata = os.environ.get("APPDATA")

    try:
        os.environ["APPDATA"] = str(temp_root)
        sample_path = build_temp_file(temp_root)
        resume_key = core_state.build_file_state_key("folder", "sample.csv", str(sample_path))

        core_state.log_processed("folder", "sample.csv", str(sample_path), None)
        core_state.set_resume_offset(resume_key, FIXED_OFFSET, None)

        manifest_path = Path(core_state.get_manifest_path(None))
        log_path = Path(core_state.get_log_path(None))
        resume_path = Path(core_state.get_resume_path(None))

        manifest_size_before = manifest_path.stat().st_size
        log_size_before = log_path.stat().st_size
        resume_size_before = resume_path.stat().st_size

        started_at = time.perf_counter()
        for _ in range(REPEAT_COUNT):
            core_state.set_resume_offset(resume_key, FIXED_OFFSET, None)
        elapsed_ms_total = (time.perf_counter() - started_at) * 1000.0

        manifest_size_after = manifest_path.stat().st_size
        log_size_after = log_path.stat().st_size
        resume_size_after = resume_path.stat().st_size

        processed_keys = core_state.load_processed(None)
        resume_map_before_clear = core_state.load_resume(None)
        core_state.set_resume_offset(resume_key, 0, None)
        resume_map_after_clear = core_state.load_resume(None)
        log_lines = read_text_lines(log_path)

        checks = [
            build_check(
                "processed_marker_stable",
                resume_key in processed_keys and "folder/sample.csv" in processed_keys and "sample.csv" in processed_keys,
                json.dumps(sorted(processed_keys), ensure_ascii=False),
            ),
            build_check(
                "manifest_size_stable",
                manifest_size_before == manifest_size_after,
                f"before={manifest_size_before}, after={manifest_size_after}",
            ),
            build_check(
                "legacy_log_size_stable",
                log_size_before == log_size_after,
                f"before={log_size_before}, after={log_size_after}",
            ),
            build_check(
                "resume_size_stable",
                resume_size_before == resume_size_after,
                f"before={resume_size_before}, after={resume_size_after}",
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
                "legacy_log_line_count",
                len(log_lines) == 1,
                json.dumps(log_lines, ensure_ascii=False),
            ),
        ]

        metrics: SmokeMetrics = {
            "repeat_count": REPEAT_COUNT,
            "elapsed_ms_total": round(elapsed_ms_total, 3),
            "elapsed_ms_per_write": round(elapsed_ms_total / float(REPEAT_COUNT), 6),
            "manifest_size_bytes": manifest_size_after,
            "log_size_bytes": log_size_after,
            "resume_size_bytes": resume_size_after,
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
