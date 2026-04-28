import shutil
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from core import files as core_files
from core import state as core_state
from core import transform as core_transform
from core import upload as core_upload


KST = timezone(timedelta(hours=9))
FIXED_NOW = datetime(2026, 4, 21, 9, 0, tzinfo=KST)


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


class SmartSyncUploadRegressionTests(TestCase):
    def create_workspace(self) -> Path:
        workspace = Path(tempfile.mkdtemp(prefix="smart-sync-regression-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        return workspace

    def create_csv_file(self, workspace: Path, filename: str) -> Path:
        target_path = workspace / filename
        target_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")
        return target_path

    def test_upload_item_skips_processed_marker_when_smart_sync_filters_every_row(self) -> None:
        workspace = self.create_workspace()
        csv_path = self.create_csv_file(workspace, "260421_all_old.csv")
        completed_items: list[tuple[str, str, str, int | None]] = []
        recorded_failures: list[tuple[str, str, str, int, str, int | None]] = []
        resume_updates: list[tuple[str, int]] = []
        resolved_device_ids: list[str] = []

        def build_chunked_plc(path: str, filename: str, chunksize: int) -> list[pd.DataFrame]:
            _ = path
            _ = filename
            _ = chunksize
            return [
                pd.DataFrame(
                    {
                        "timestamp": [
                            "2026-04-20T23:59:58+09:00",
                            "2026-04-20T23:59:59+09:00",
                        ],
                        "device_id": ["extruder_plc", "extruder_plc"],
                        "value": [1, 2],
                    }
                )
            ]

        def record_resume_offset(key: str, offset: int) -> None:
            resume_updates.append((key, offset))

        def record_completed(folder: str, filename: str, path: str, run_id: int | None) -> None:
            completed_items.append((folder, filename, path, run_id))

        def record_failure(
            folder: str,
            filename: str,
            path: str,
            resume_offset: int,
            error_message: str,
            run_id: int | None,
        ) -> None:
            recorded_failures.append((folder, filename, path, resume_offset, error_message, run_id))

        with patch.object(core_upload, "create_upload_http_client", new=lambda: NullClient()):
            with patch.object(core_upload, "upload_via_edge") as upload_via_edge_mock:
                ok = core_upload.upload_item(
                    "http://localhost/upload",
                    "anon-key",
                    "folder",
                    csv_path.name,
                    str(csv_path),
                    "plc",
                    build_plc=build_chunked_plc,
                    build_temp=build_chunked_plc,
                    get_resume_offset=lambda key: 4,
                    set_resume_offset_fn=record_resume_offset,
                    mark_file_completed_fn=record_completed,
                    record_file_failure_fn=record_failure,
                    log=lambda message: None,
                    batch_size=100,
                    chunk_size=100,
                    progress_cb=None,
                    progress_update_interval_seconds=0.0,
                    enable_smart_sync=True,
                    resolve_latest_timestamp_fn=lambda device_id: resolved_device_ids.append(device_id) or "2026-04-21T00:00:00+09:00",
                    pause_event=None,
                    run_id=11,
                )

        self.assertTrue(ok)
        self.assertEqual(upload_via_edge_mock.call_count, 0)
        self.assertEqual(completed_items, [])
        self.assertEqual(recorded_failures, [])
        self.assertEqual(resolved_device_ids, ["extruder_plc"])
        self.assertEqual([offset for _, offset in resume_updates], [0])

    def test_upload_item_ignores_resume_offset_after_smart_sync_filtering(self) -> None:
        workspace = self.create_workspace()
        csv_path = self.create_csv_file(workspace, "260421_new_rows.csv")
        captured_frames: list[pd.DataFrame] = []
        resume_updates: list[tuple[str, int]] = []
        completed_items: list[tuple[str, str, str, int | None]] = []
        recorded_failures: list[tuple[str, str, str, int, str, int | None]] = []
        resolved_device_ids: list[str] = []

        def build_chunked_plc(path: str, filename: str, chunksize: int) -> list[pd.DataFrame]:
            _ = path
            _ = filename
            _ = chunksize
            return [
                pd.DataFrame(
                    {
                        "timestamp": [
                            "2026-04-21T00:00:00+09:00",
                            "2026-04-21T00:00:01+09:00",
                            "2026-04-21T00:00:02+09:00",
                            "2026-04-21T00:00:03+09:00",
                        ],
                        "device_id": [
                            "extruder_plc",
                            "extruder_plc",
                            "extruder_plc",
                            "extruder_plc",
                        ],
                        "value": [1, 2, 3, 4],
                    }
                )
            ]

        def record_resume_offset(key: str, offset: int) -> None:
            resume_updates.append((key, offset))

        def record_completed(folder: str, filename: str, path: str, run_id: int | None) -> None:
            completed_items.append((folder, filename, path, run_id))

        def record_failure(
            folder: str,
            filename: str,
            path: str,
            resume_offset: int,
            error_message: str,
            run_id: int | None,
        ) -> None:
            recorded_failures.append((folder, filename, path, resume_offset, error_message, run_id))

        def fake_upload_via_edge(
            edge_url: str,
            anon_key: str,
            df: pd.DataFrame,
            client: Any,
            *,
            log: Any,
            resume_key: str | None,
            start_index: int,
            batch_size: int,
            progress_cb: Any,
            pause_event: Any,
            silent: bool,
        ) -> bool:
            _ = edge_url
            _ = anon_key
            _ = client
            _ = log
            _ = resume_key
            _ = start_index
            _ = batch_size
            _ = progress_cb
            _ = pause_event
            _ = silent
            captured_frames.append(df.copy())
            return True

        with patch.object(core_upload, "create_upload_http_client", new=lambda: NullClient()):
            with patch.object(core_upload, "upload_via_edge", new=fake_upload_via_edge):
                ok = core_upload.upload_item(
                    "http://localhost/upload",
                    "anon-key",
                    "folder",
                    csv_path.name,
                    str(csv_path),
                    "plc",
                    build_plc=build_chunked_plc,
                    build_temp=build_chunked_plc,
                    get_resume_offset=lambda key: 3,
                    set_resume_offset_fn=record_resume_offset,
                    mark_file_completed_fn=record_completed,
                    record_file_failure_fn=record_failure,
                    log=lambda message: None,
                    batch_size=100,
                    chunk_size=100,
                    progress_cb=None,
                    progress_update_interval_seconds=0.0,
                    enable_smart_sync=True,
                    resolve_latest_timestamp_fn=lambda device_id: resolved_device_ids.append(device_id) or "2026-04-21T00:00:01+09:00",
                    pause_event=None,
                    run_id=12,
                )

        self.assertTrue(ok)
        self.assertEqual(len(captured_frames), 1)
        self.assertEqual(captured_frames[0]["value"].tolist(), [3, 4])
        self.assertEqual(captured_frames[0]["device_id"].tolist(), ["extruder_plc", "extruder_plc"])
        self.assertEqual(resolved_device_ids, ["extruder_plc"])
        self.assertEqual([offset for _, offset in resume_updates], [2])
        self.assertEqual(completed_items, [("folder", csv_path.name, str(csv_path), 12)])
        self.assertEqual(recorded_failures, [])

    def test_run_upload_session_preserves_multiple_failures_and_skips_recent_success_profile(self) -> None:
        items = [
            core_upload.UploadSessionItem(folder="folder-a", filename="a.csv", path="C:/tmp/a.csv", kind="plc"),
            core_upload.UploadSessionItem(folder="folder-b", filename="b.csv", path="C:/tmp/b.csv", kind="plc"),
        ]
        config = core_upload.UploadSessionConfig(
            edge_url="http://localhost/upload",
            anon_key="anon-key",
            batch_size=100,
            chunk_size=100,
            progress_update_interval_seconds=0.0,
            enable_smart_sync=False,
            max_workers=1,
        )
        finished_runs: list[tuple[int, int, int, int, tuple[str, ...], dict[str, object] | None]] = []

        def fake_upload_item(*args: Any, **kwargs: Any) -> bool:
            _ = args
            _ = kwargs
            return False

        with patch.object(core_upload, "upload_item", new=fake_upload_item):
            session_result = core_upload.run_upload_session(
                items,
                config,
                build_plc=lambda path, filename: pd.DataFrame(),
                build_temp=lambda path, filename: pd.DataFrame(),
                get_resume_offset=lambda key: 7,
                set_resume_offset_fn=lambda key, offset: None,
                mark_file_completed_fn=lambda folder, filename, path, run_id: None,
                record_file_failure_fn=lambda folder, filename, path, resume_offset, error_message, run_id: None,
                start_upload_run_fn=lambda total_count, retry_failed_only, runtime_config_values: 41,
                finish_upload_run_fn=lambda run_id, total_count, success_count, failure_count, warning_messages, recent_successful_upload_profile: finished_runs.append(
                    (run_id, total_count, success_count, failure_count, warning_messages, recent_successful_upload_profile)
                ),
                retry_failed_only=True,
                recent_successful_upload_profile={
                    "profile_name": "recent",
                    "applied_at": 10.0,
                    "values": {"PLC_DIR": "C:/plc"},
                },
                runtime_config_values={"PLC_DIR": "C:/plc"},
                log=lambda message: None,
                pause_event=None,
                progress_cb=None,
                file_complete_cb=None,
            )

        self.assertEqual(session_result.run_id, 41)
        self.assertEqual(session_result.failure_count, 2)
        self.assertEqual(len(session_result.failed_items), 2)
        self.assertEqual(
            [failed_item.resume_offset for failed_item in session_result.failed_items],
            [7, 7],
        )
        self.assertEqual(
            finished_runs,
            [(41, 2, 0, 2, (), None)],
        )


class ProcessedFileRegressionTests(TestCase):
    def create_workspace(self) -> Path:
        workspace = Path(tempfile.mkdtemp(prefix="processed-file-regression-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        return workspace

    def create_plc_file(self, plc_dir: Path, filename: str) -> Path:
        target_path = plc_dir / filename
        target_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")
        return target_path

    def test_list_candidates_excludes_processed_files_for_current_and_legacy_keys(self) -> None:
        workspace = self.create_workspace()
        plc_dir = workspace / "plc"
        plc_dir.mkdir(parents=True, exist_ok=True)
        keep_path = self.create_plc_file(plc_dir, "260421_keep.csv")
        skip_path = self.create_plc_file(plc_dir, "260421_skip.csv")
        skip_filename = skip_path.name
        current_key = core_state.build_file_state_key(str(plc_dir), skip_filename, str(skip_path))
        legacy_key = core_state.build_legacy_file_key(str(plc_dir), skip_filename)

        for processed_key in [current_key, legacy_key]:
            with self.subTest(processed_key=processed_key):
                with patch.object(core_files, "load_processed", return_value={processed_key}):
                    with patch.object(core_files, "kst_now", return_value=FIXED_NOW):
                        with patch.object(core_files, "stable_enough", return_value=True):
                            with patch.object(core_files, "is_locked", return_value=False):
                                items = core_files.list_candidates(
                                    str(plc_dir),
                                    None,
                                    date(2026, 4, 21),
                                    date(2026, 4, 21),
                                    15,
                                    True,
                                    True,
                                    False,
                                )

                self.assertEqual(items, [(str(plc_dir), keep_path.name, str(keep_path), "plc")])


class BuilderDeviceIdRegressionTests(TestCase):
    def create_workspace(self) -> Path:
        workspace = Path(tempfile.mkdtemp(prefix="builder-device-id-regression-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        return workspace

    def test_build_records_plc_sets_legacy_device_id(self) -> None:
        workspace = self.create_workspace()
        file_path = workspace / "260421_plc.csv"
        file_path.write_text("Time\n00:00:01\n00:00:02\n", encoding="utf-8")

        dataframe = core_transform.build_records_plc(str(file_path), file_path.name, None)

        self.assertFalse(dataframe.empty)
        self.assertEqual(dataframe["device_id"].tolist(), [core_transform.PLC_DEVICE_ID, core_transform.PLC_DEVICE_ID])

    def test_build_records_plc_sets_integrated_device_id(self) -> None:
        workspace = self.create_workspace()
        file_path = workspace / "Factory_Integrated_Log_20260421_010101.csv"
        file_path.write_text(
            "Date,Time,Mold1,Temperature\n2026-04-21,00:00:01,11,301.5\n2026-04-21,00:00:02,12,302.5\n",
            encoding="utf-8",
        )

        dataframe = core_transform.build_records_plc(str(file_path), file_path.name, None)

        self.assertFalse(dataframe.empty)
        self.assertEqual(
            dataframe["device_id"].tolist(),
            [core_transform.INTEGRATED_PLC_DEVICE_ID, core_transform.INTEGRATED_PLC_DEVICE_ID],
        )
        self.assertEqual(dataframe["mold_1"].tolist(), [11, 12])

    def test_build_records_plc_raises_for_missing_file(self) -> None:
        workspace = self.create_workspace()
        file_path = workspace / "260421_missing.csv"

        with self.assertRaisesRegex(ValueError, "PLC CSV 변환 실패"):
            core_transform.build_records_plc(str(file_path), file_path.name, None)

    def test_build_records_plc_wraps_chunk_iteration_errors(self) -> None:
        class BrokenReader:
            def __iter__(self) -> Iterator[pd.DataFrame]:
                raise RuntimeError("reader exploded")

        chunks = None
        with patch.object(core_transform.pd, "read_csv", return_value=BrokenReader()):
            chunks = core_transform.build_records_plc("C:/broken.csv", "broken.csv", 10)

        with self.assertRaisesRegex(ValueError, "PLC CSV 변환 실패"):
            list(chunks)

    def test_build_records_temp_sets_temperature_device_id(self) -> None:
        workspace = self.create_workspace()
        file_path = workspace / "temperature.csv"
        file_path.write_text(
            "Date,Time,Temperature\n2026-04-21,00:00:01,101.5\n2026-04-21,00:00:02,102.5\n",
            encoding="utf-8",
        )

        dataframe = core_transform.build_records_temp(str(file_path), file_path.name, None)

        self.assertFalse(dataframe.empty)
        self.assertEqual(
            dataframe["device_id"].tolist(),
            ["spot_temperature_sensor", "spot_temperature_sensor"],
        )
