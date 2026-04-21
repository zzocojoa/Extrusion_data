from __future__ import annotations

import json
import shutil
import sys
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Mapping, TypedDict


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core import state as core_state
import uploader_gui_tk as gui


KST = timezone(timedelta(hours=9))
FIXED_NOW = datetime(2026, 4, 21, 9, 0, tzinfo=KST)


class SmokeCheck(TypedDict):
    name: str
    ok: bool
    detail: str


class SmokeMetrics(TypedDict):
    preview_has_data_calls: list[str]
    sample_rows: list[int]
    included_files: list[str]
    excluded_reasons: dict[str, str]


class SmokeReport(TypedDict):
    checks: list[SmokeCheck]
    metrics: SmokeMetrics


def build_check(name: str, ok: bool, detail: str) -> SmokeCheck:
    return {
        "name": name,
        "ok": ok,
        "detail": detail,
    }


def write_csv_file(target_path: Path) -> None:
    target_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")


def build_translate_fn() -> Callable[[str, Mapping[str, object]], str]:
    def translate(key: str, params: Mapping[str, object]) -> str:
        _ = params
        return key

    return translate


def run_smoke() -> SmokeReport:
    temp_root = Path(tempfile.mkdtemp(prefix="preview-lightweight-smoke-"))
    plc_dir = temp_root / "plc"
    plc_dir.mkdir(parents=True, exist_ok=True)

    filenames = [
        "260421_keep.csv",
        "260421_empty.csv",
        "260421_processed.csv",
        "260421_unstable.csv",
        "260421_locked.csv",
        "260420_old.csv",
    ]
    for filename in filenames:
        write_csv_file(plc_dir / filename)

    processed_path = plc_dir / "260421_processed.csv"
    processed_keys = set(
        core_state.build_file_state_lookup_keys(
            str(plc_dir),
            processed_path.name,
            str(processed_path),
        )
    )
    preview_has_data_calls: list[str] = []
    sample_rows: list[int] = []
    preview_globals = gui.preview_diagnostics.__globals__
    original_attributes: dict[str, object] = {
        "load_processed": preview_globals["load_processed"],
        "stable_enough": preview_globals["stable_enough"],
        "is_locked": preview_globals["is_locked"],
        "kst_now": preview_globals["kst_now"],
        "preview_has_data": gui.core_files.preview_has_data,
    }

    def fake_stable_enough(path: str, lag_minutes: int) -> bool:
        _ = lag_minutes
        return not path.endswith("260421_unstable.csv")

    def fake_is_locked(path: str) -> bool:
        return path.endswith("260421_locked.csv")

    def fake_kst_now() -> datetime:
        return FIXED_NOW

    def fake_preview_has_data(kind: str, path: str, max_rows: int) -> bool:
        _ = kind
        filename = Path(path).name
        preview_has_data_calls.append(filename)
        sample_rows.append(max_rows)
        return filename == "260421_keep.csv"

    preview_globals["load_processed"] = lambda: processed_keys
    preview_globals["stable_enough"] = fake_stable_enough
    preview_globals["is_locked"] = fake_is_locked
    preview_globals["kst_now"] = fake_kst_now
    gui.core_files.preview_has_data = fake_preview_has_data

    try:
        included, excluded = gui.preview_diagnostics(
            str(plc_dir),
            "",
            date(2026, 4, 21),
            date(2026, 4, 21),
            15,
            True,
            True,
            build_translate_fn(),
        )
    finally:
        preview_globals["load_processed"] = original_attributes["load_processed"]
        preview_globals["stable_enough"] = original_attributes["stable_enough"]
        preview_globals["is_locked"] = original_attributes["is_locked"]
        preview_globals["kst_now"] = original_attributes["kst_now"]
        gui.core_files.preview_has_data = original_attributes["preview_has_data"]
        shutil.rmtree(temp_root, ignore_errors=True)

    included_files = sorted(filename for _, filename, _, _ in included)
    excluded_reasons = {
        filename: reason
        for _, filename, reason in excluded
    }
    expected_excluded_reasons = {
        "260420_old.csv": "dashboard.preview.excluded.out_of_range",
        "260421_empty.csv": "dashboard.preview.excluded.no_data",
        "260421_locked.csv": "dashboard.preview.excluded.locked",
        "260421_processed.csv": "dashboard.preview.excluded.already_processed",
        "260421_unstable.csv": "dashboard.preview.excluded.unstable_today",
    }
    checks = [
        build_check(
            "preview_included_files",
            included_files == ["260421_keep.csv"],
            json.dumps(included_files, ensure_ascii=False),
        ),
        build_check(
            "preview_excluded_reasons",
            excluded_reasons == expected_excluded_reasons,
            json.dumps(excluded_reasons, ensure_ascii=False, sort_keys=True),
        ),
        build_check(
            "preview_has_data_called_only_for_candidates",
            sorted(preview_has_data_calls) == ["260421_empty.csv", "260421_keep.csv"],
            json.dumps(sorted(preview_has_data_calls), ensure_ascii=False),
        ),
        build_check(
            "preview_sample_rows_stays_small",
            sample_rows == [gui.PREVIEW_VALIDATION_SAMPLE_ROWS, gui.PREVIEW_VALIDATION_SAMPLE_ROWS],
            json.dumps(sample_rows, ensure_ascii=False),
        ),
        build_check(
            "preview_total_file_accounting",
            len(included) + len(excluded) == len(filenames),
            f"included={len(included)}, excluded={len(excluded)}, total={len(filenames)}",
        ),
    ]
    metrics: SmokeMetrics = {
        "preview_has_data_calls": sorted(preview_has_data_calls),
        "sample_rows": sample_rows,
        "included_files": included_files,
        "excluded_reasons": excluded_reasons,
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
