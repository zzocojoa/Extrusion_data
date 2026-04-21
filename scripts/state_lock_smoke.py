import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core import state as core_state


PROCESS_COUNT = 4


def run_worker(index: int, appdata_root: str) -> int:
    os.environ["APPDATA"] = appdata_root
    try:
        target_file = str(Path(appdata_root) / f"dummy-{index}.csv")
        core_state.log_processed("folder", f"dummy-{index}.csv", target_file, None)
        print(json.dumps({"index": index, "ok": True, "detail": ""}, ensure_ascii=False))
        return 0
    except Exception as error:
        print(
            json.dumps(
                {
                    "index": index,
                    "ok": False,
                    "detail": f"{error.__class__.__name__}: {error}",
                },
                ensure_ascii=False,
            )
        )
        return 1


def parse_worker_args(args: list[str]) -> tuple[int, str] | None:
    if len(args) != 4:
        return None
    if args[1] != "--worker":
        return None
    if args[3] == "":
        return None
    return int(args[2]), args[3]


def run_parent() -> int:
    appdata_root = tempfile.mkdtemp(prefix="state-lock-smoke-")
    os.environ["APPDATA"] = appdata_root
    commands: list[subprocess.Popen[str]] = []
    results: list[dict[str, Any]] = []

    try:
        for index in range(PROCESS_COUNT):
            process = subprocess.Popen(
                [sys.executable, __file__, "--worker", str(index), appdata_root],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
            )
            commands.append(process)

        for process in commands:
            stdout_text, stderr_text = process.communicate(timeout=30)
            if stdout_text.strip() == "":
                results.append(
                    {
                        "index": -1,
                        "ok": False,
                        "detail": stderr_text.strip() or f"exit={process.returncode}",
                    }
                )
                continue
            try:
                result = json.loads(stdout_text.strip())
            except json.JSONDecodeError as error:
                results.append(
                    {
                        "index": -1,
                        "ok": False,
                        "detail": f"JSONDecodeError: {error}: {stdout_text.strip()}",
                    }
                )
                continue
            if stderr_text.strip() != "":
                result["ok"] = False
                result["detail"] = stderr_text.strip()
            if process.returncode != 0:
                result["ok"] = False
                if str(result.get("detail", "")).strip() == "":
                    result["detail"] = f"exit={process.returncode}"
            results.append(result)
    finally:
        for process in commands:
            if process.poll() is None:
                process.kill()
        shutil.rmtree(appdata_root, ignore_errors=True)

    results.sort(key=lambda result: int(result["index"]))
    print(json.dumps(results, ensure_ascii=False, indent=2))
    failed = [result for result in results if not bool(result["ok"])]
    if len(results) != PROCESS_COUNT:
        return 1
    if failed:
        return 1
    return 0


def main(args: list[str]) -> int:
    worker_args = parse_worker_args(args)
    if worker_args is not None:
        worker_index, appdata_root = worker_args
        return run_worker(worker_index, appdata_root)
    return run_parent()


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
