import shutil
import tempfile
from pathlib import Path
from typing import Any, Callable, Iterator
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from core import upload as core_upload


class NullClient:
    def __enter__(self) -> "NullClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        traceback: Any,
    ) -> None:
        _ = exc_type
        _ = exc
        _ = traceback
        return None


class UploadProgressCoreTests(TestCase):
    def create_workspace(self) -> Path:
        workspace = Path(tempfile.mkdtemp(prefix="upload-progress-core-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        return workspace

    def create_csv_file(self, workspace: Path, filename: str) -> Path:
        target_path = workspace / filename
        target_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")
        return target_path

    def make_dataframe(self, values: list[int]) -> pd.DataFrame:
        row_count = len(values)
        return pd.DataFrame(
            {
                "timestamp": [f"2026-04-21T00:00:0{i}+09:00" for i in range(row_count)],
                "device_id": ["extruder_plc" for _ in values],
                "value": values,
            }
        )

    def test_upload_item_reports_known_total_for_chunk_sequence(self) -> None:
        workspace = self.create_workspace()
        file_path = self.create_csv_file(workspace, "known_total.csv")
        progress_events: list[tuple[int, int]] = []
        chunks = [self.make_dataframe([1, 2, 3])]

        def build_plc(path: str, filename: str, chunksize: int) -> list[pd.DataFrame]:
            _ = path
            _ = filename
            _ = chunksize
            return chunks

        def record_progress(done: int, total: int) -> None:
            progress_events.append((done, total))

        def fake_upload_via_edge(
            edge_url: str,
            anon_key: str,
            df: pd.DataFrame,
            client: Any,
            *,
            log: Callable[[str], None],
            resume_key: str | None,
            start_index: int,
            batch_size: int,
            progress_cb: Callable[[int, int], None] | None,
            pause_event: Any,
            silent: bool,
        ) -> bool:
            _ = edge_url
            _ = anon_key
            _ = df
            _ = client
            _ = log
            _ = resume_key
            _ = start_index
            _ = batch_size
            _ = progress_cb
            _ = pause_event
            _ = silent
            return True

        with patch.object(core_upload, "create_upload_http_client", new=lambda: NullClient()):
            with patch.object(core_upload, "upload_via_edge", new=fake_upload_via_edge):
                ok = core_upload.upload_item(
                    "http://localhost/upload",
                    "anon-key",
                    "folder",
                    file_path.name,
                    str(file_path),
                    "plc",
                    build_plc=build_plc,
                    build_temp=build_plc,
                    get_resume_offset=lambda key: 1,
                    set_resume_offset_fn=lambda key, offset: None,
                    mark_file_completed_fn=lambda folder, filename, path, run_id: None,
                    record_file_failure_fn=lambda folder, filename, path, resume_offset, error_message, run_id: None,
                    log=lambda message: None,
                    batch_size=100,
                    chunk_size=100,
                    progress_cb=record_progress,
                    progress_update_interval_seconds=0.0,
                    enable_smart_sync=False,
                    resolve_latest_timestamp_fn=None,
                    pause_event=None,
                    run_id=1,
                )

        self.assertTrue(ok)
        self.assertEqual(progress_events, [(1, 3), (3, 3), (3, 3)])

    def test_upload_item_keeps_unknown_total_zero_until_completion_for_generator(self) -> None:
        workspace = self.create_workspace()
        file_path = self.create_csv_file(workspace, "unknown_total.csv")
        progress_events: list[tuple[int, int]] = []

        def build_plc(path: str, filename: str, chunksize: int) -> Iterator[pd.DataFrame]:
            _ = path
            _ = filename
            _ = chunksize

            def generator() -> Iterator[pd.DataFrame]:
                yield self.make_dataframe([1, 2, 3])

            return generator()

        def record_progress(done: int, total: int) -> None:
            progress_events.append((done, total))

        def fake_upload_via_edge(
            edge_url: str,
            anon_key: str,
            df: pd.DataFrame,
            client: Any,
            *,
            log: Callable[[str], None],
            resume_key: str | None,
            start_index: int,
            batch_size: int,
            progress_cb: Callable[[int, int], None] | None,
            pause_event: Any,
            silent: bool,
        ) -> bool:
            _ = edge_url
            _ = anon_key
            _ = df
            _ = client
            _ = log
            _ = resume_key
            _ = start_index
            _ = batch_size
            _ = progress_cb
            _ = pause_event
            _ = silent
            return True

        with patch.object(core_upload, "create_upload_http_client", new=lambda: NullClient()):
            with patch.object(core_upload, "upload_via_edge", new=fake_upload_via_edge):
                ok = core_upload.upload_item(
                    "http://localhost/upload",
                    "anon-key",
                    "folder",
                    file_path.name,
                    str(file_path),
                    "plc",
                    build_plc=build_plc,
                    build_temp=build_plc,
                    get_resume_offset=lambda key: 1,
                    set_resume_offset_fn=lambda key, offset: None,
                    mark_file_completed_fn=lambda folder, filename, path, run_id: None,
                    record_file_failure_fn=lambda folder, filename, path, resume_offset, error_message, run_id: None,
                    log=lambda message: None,
                    batch_size=100,
                    chunk_size=100,
                    progress_cb=record_progress,
                    progress_update_interval_seconds=0.0,
                    enable_smart_sync=False,
                    resolve_latest_timestamp_fn=None,
                    pause_event=None,
                    run_id=2,
                )

        self.assertTrue(ok)
        self.assertEqual(progress_events, [(1, 0), (3, 0), (3, 3)])

    def test_upload_item_records_failure_when_builder_raises(self) -> None:
        workspace = self.create_workspace()
        file_path = self.create_csv_file(workspace, "broken.csv")
        recorded_failures: list[tuple[str, str, str, int, str, int | None]] = []
        completed_items: list[tuple[str, str, str, int | None]] = []

        def build_plc(path: str, filename: str, chunksize: int) -> Iterator[pd.DataFrame]:
            _ = path
            _ = filename
            _ = chunksize
            raise ValueError("CSV 변환 실패")

        def record_failure(
            folder: str,
            filename: str,
            path: str,
            resume_offset: int,
            error_message: str,
            run_id: int | None,
        ) -> None:
            recorded_failures.append((folder, filename, path, resume_offset, error_message, run_id))

        def record_completed(folder: str, filename: str, path: str, run_id: int | None) -> None:
            completed_items.append((folder, filename, path, run_id))

        ok = core_upload.upload_item(
            "http://localhost/upload",
            "anon-key",
            "folder",
            file_path.name,
            str(file_path),
            "plc",
            build_plc=build_plc,
            build_temp=build_plc,
            get_resume_offset=lambda key: 0,
            set_resume_offset_fn=lambda key, offset: None,
            mark_file_completed_fn=record_completed,
            record_file_failure_fn=record_failure,
            log=lambda message: None,
            batch_size=100,
            chunk_size=100,
            progress_cb=None,
            progress_update_interval_seconds=0.0,
            enable_smart_sync=False,
            resolve_latest_timestamp_fn=None,
            pause_event=None,
            run_id=3,
        )

        self.assertFalse(ok)
        self.assertEqual(completed_items, [])
        self.assertEqual(len(recorded_failures), 1)
        self.assertEqual(recorded_failures[0][0:3], ("folder", file_path.name, str(file_path)))
        self.assertEqual(recorded_failures[0][3], 1)
        self.assertIn("CSV 변환 실패", recorded_failures[0][4])
        self.assertEqual(recorded_failures[0][5], 3)
