import json
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import TypedDict

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core import upload as core_upload


class SmokeCheck(TypedDict):
    name: str
    ok: bool
    detail: str


class UploadViaEdgeMetrics(TypedDict):
    batch_sizes: list[int]
    resume_offsets: list[int]
    progress_points: list[int]


class UploadItemMetrics(TypedDict):
    batch_sizes: list[int]
    resume_offsets: list[int]
    progress_points: list[str]
    processed_items: list[str]


class SmokeReport(TypedDict):
    checks: list[SmokeCheck]
    upload_via_edge: UploadViaEdgeMetrics
    upload_item: UploadItemMetrics


@dataclass(frozen=True)
class FakeResponse:
    status_code: int
    inserted: int

    @property
    def text(self) -> str:
        return ""

    def json(self) -> dict[str, int]:
        return {"inserted": self.inserted}


class RecordingHttpClient:
    def __init__(self) -> None:
        self.batch_sizes: list[int] = []

    def __enter__(self) -> "RecordingHttpClient":
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        return None

    def post(
        self,
        edge_url: str,
        json: list[dict[str, object]],
        headers: dict[str, str],
        timeout: float,
    ) -> FakeResponse:
        _ = edge_url
        _ = headers
        _ = timeout
        self.batch_sizes.append(len(json))
        return FakeResponse(status_code=200, inserted=len(json))


def build_check(name: str, ok: bool, detail: str) -> SmokeCheck:
    return {
        "name": name,
        "ok": ok,
        "detail": detail,
    }


def build_dataframe(row_count: int) -> pd.DataFrame:
    timestamps = [f"2026-04-21T00:{index // 60:02d}:{index % 60:02d}Z" for index in range(row_count)]
    values = list(range(row_count))
    return pd.DataFrame({"timestamp": timestamps, "value": values})


def run_upload_via_edge_smoke() -> tuple[list[SmokeCheck], UploadViaEdgeMetrics]:
    df = build_dataframe(1050)
    client = RecordingHttpClient()
    resume_offsets: list[int] = []
    progress_points: list[int] = []
    original_set_resume_offset = core_upload.set_resume_offset

    def record_resume_offset(key: str, offset: int) -> None:
        _ = key
        resume_offsets.append(offset)

    try:
        core_upload.set_resume_offset = record_resume_offset
        ok = core_upload.upload_via_edge(
            "http://localhost/upload",
            "anon-key",
            df,
            client,
            log=lambda message: None,
            resume_key="resume-key",
            start_index=0,
            batch_size=200,
            progress_cb=lambda done, total: progress_points.append(done if total == len(df) else -1),
            pause_event=None,
            silent=True,
        )
    finally:
        core_upload.set_resume_offset = original_set_resume_offset

    checks = [
        build_check("upload_via_edge_ok", ok, f"batch_sizes={client.batch_sizes}"),
        build_check(
            "upload_via_edge_batch_sizes",
            client.batch_sizes == [200, 200, 200, 200, 200, 50],
            json.dumps(client.batch_sizes, ensure_ascii=False),
        ),
        build_check(
            "upload_via_edge_resume_offsets",
            resume_offsets == [200, 400, 600, 800, 1000, 1050],
            json.dumps(resume_offsets, ensure_ascii=False),
        ),
        build_check(
            "upload_via_edge_progress_points",
            progress_points == [200, 400, 600, 800, 1000, 1050],
            json.dumps(progress_points, ensure_ascii=False),
        ),
    ]
    metrics: UploadViaEdgeMetrics = {
        "batch_sizes": client.batch_sizes,
        "resume_offsets": resume_offsets,
        "progress_points": progress_points,
    }
    return checks, metrics


def create_csv_file(workspace: Path, row_count: int) -> Path:
    target_path = workspace / "chunked.csv"
    rows = ["timestamp,value"] + [f"2026-04-21T00:{index // 60:02d}:{index % 60:02d}Z,{index}" for index in range(row_count)]
    target_path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return target_path


def build_chunked_builder(source_df: pd.DataFrame) -> Callable[[str, str, int], list[pd.DataFrame]]:
    def builder(path: str, filename: str, chunksize: int) -> list[pd.DataFrame]:
        _ = path
        _ = filename
        chunks: list[pd.DataFrame] = []
        for start in range(0, len(source_df), chunksize):
            chunks.append(source_df.iloc[start : start + chunksize].copy())
        return chunks

    return builder


def run_upload_item_smoke() -> tuple[list[SmokeCheck], UploadItemMetrics]:
    source_df = build_dataframe(300)
    temp_root = Path(tempfile.mkdtemp(prefix="upload-item-smoke-"))
    csv_path = create_csv_file(temp_root, 300)
    client = RecordingHttpClient()
    resume_offsets: list[int] = []
    progress_points: list[str] = []
    processed_items: list[str] = []
    original_create_upload_http_client = core_upload.create_upload_http_client

    def create_client() -> RecordingHttpClient:
        return client

    def record_resume_offset(key: str, offset: int) -> None:
        _ = key
        resume_offsets.append(offset)

    def record_processed(folder: str, filename: str, path: str) -> None:
        processed_items.append(f"{folder}/{filename}|{path}")

    try:
        core_upload.create_upload_http_client = create_client
        ok = core_upload.upload_item(
            "http://localhost/upload",
            "anon-key",
            "folder",
            "chunked.csv",
            str(csv_path),
            "plc",
            build_plc=build_chunked_builder(source_df),
            build_temp=build_chunked_builder(source_df),
            get_resume_offset=lambda key: 0,
            set_resume_offset_fn=record_resume_offset,
            log_processed_fn=record_processed,
            log=lambda message: None,
            batch_size=50,
            chunk_size=120,
            progress_cb=lambda done, total: progress_points.append(f"{done}/{total}"),
            progress_update_interval_seconds=0.0,
            enable_smart_sync=False,
            pause_event=None,
            latest_timestamp=None,
        )
    finally:
        core_upload.create_upload_http_client = original_create_upload_http_client
        csv_path.unlink(missing_ok=True)
        temp_root.rmdir()

    checks = [
        build_check("upload_item_ok", ok, f"batch_sizes={client.batch_sizes}"),
        build_check(
            "upload_item_batch_sizes",
            client.batch_sizes == [50, 50, 20, 50, 50, 20, 50, 10],
            json.dumps(client.batch_sizes, ensure_ascii=False),
        ),
        build_check(
            "upload_item_resume_offsets",
            resume_offsets == [120, 240, 300, 0],
            json.dumps(resume_offsets, ensure_ascii=False),
        ),
        build_check(
            "upload_item_progress_points",
            len(progress_points) >= 1 and progress_points[-1] == "300/300",
            json.dumps(progress_points, ensure_ascii=False),
        ),
        build_check(
            "upload_item_processed_marker",
            processed_items == [f"folder/chunked.csv|{csv_path}"],
            json.dumps(processed_items, ensure_ascii=False),
        ),
    ]
    metrics: UploadItemMetrics = {
        "batch_sizes": client.batch_sizes,
        "resume_offsets": resume_offsets,
        "progress_points": progress_points,
        "processed_items": processed_items,
    }
    return checks, metrics


def run_smoke() -> SmokeReport:
    upload_via_edge_checks, upload_via_edge_metrics = run_upload_via_edge_smoke()
    upload_item_checks, upload_item_metrics = run_upload_item_smoke()
    return {
        "checks": upload_via_edge_checks + upload_item_checks,
        "upload_via_edge": upload_via_edge_metrics,
        "upload_item": upload_item_metrics,
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
