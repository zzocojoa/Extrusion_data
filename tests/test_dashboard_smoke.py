import shutil
import tempfile
import time
import tkinter as tk
from contextlib import ExitStack
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest import SkipTest, TestCase
from unittest.mock import Mock, patch

from core import state as core_state
import uploader_gui_tk


def pump(app: uploader_gui_tk.App) -> None:
    app.update_idletasks()
    app.update()


class DashboardSmokeTests(TestCase):
    def create_app(self, appdata_root: Path) -> uploader_gui_tk.App:
        data_dir = appdata_root / "ExtrusionUploader"
        data_dir.mkdir(parents=True, exist_ok=True)
        env_patcher = patch.dict("os.environ", {"APPDATA": str(appdata_root)}, clear=False)
        data_dir_patcher = patch.object(uploader_gui_tk, "DATA_DIR", str(data_dir))
        state_db_path_patcher = patch.object(uploader_gui_tk, "STATE_DB_PATH", str(data_dir / "uploader_state.db"))
        tcp_patcher = patch.object(uploader_gui_tk, "can_connect_tcp", return_value=False)
        wsl_patcher = patch.object(uploader_gui_tk.App, "request_wsl_storage_refresh", new=lambda self: None)
        cards_patcher = patch.object(
            uploader_gui_tk.App,
            "refresh_upload_operational_cards",
            new=lambda self: None,
        )
        for patcher in (
            env_patcher,
            data_dir_patcher,
            state_db_path_patcher,
            tcp_patcher,
            wsl_patcher,
            cards_patcher,
        ):
            patcher.start()
            self.addCleanup(patcher.stop)
        try:
            app = uploader_gui_tk.App()
        except tk.TclError as error:
            raise SkipTest(f"Tk runtime unavailable: {error}") from error
        self.addCleanup(app.close_application)
        pump(app)
        return app

    def refresh_dashboard_layout_with_widths(
        self,
        app: uploader_gui_tk.App,
        body_width: int,
        footer_width: int,
    ) -> None:
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(app.dashboard_body_frame, "winfo_width", return_value=body_width)
            )
            stack.enter_context(
                patch.object(app.action_frame, "winfo_width", return_value=footer_width)
            )
            app.dashboard_layout_signature = None
            app.refresh_dashboard_layout()

    def test_dashboard_renders_upload_operation_cards(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-appdata-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.show_dashboard()
        pump(app)
        app.geometry("1160x760")
        pump(app)
        app.refresh_dashboard_layout()
        pump(app)

        self.assertTrue(app.upload_precheck_frame.winfo_exists())
        self.assertTrue(app.upload_resume_card.winfo_exists())
        self.assertTrue(app.recent_success_card.winfo_exists())
        self.assertTrue(app.btn_retry_failed.winfo_exists())
        self.assertTrue(app.btn_rerun_recent_success.winfo_exists())
        self.assertTrue(app.lbl_state_store_status.winfo_exists())
        self.assertTrue(app.lbl_hero_status_detail.winfo_exists())
        self.assertTrue(app.dashboard_scroll_frame.winfo_exists())

        self.assertEqual(app.btn_retry_failed.winfo_manager(), "grid")
        self.assertEqual(app.btn_preview.winfo_manager(), "grid")
        self.assertEqual(app.btn_pause.winfo_manager(), "grid")
        self.assertEqual(app.btn_start.winfo_manager(), "grid")

        self.assertEqual(int(app.btn_start.grid_info()["column"]), 0)
        self.assertEqual(int(app.btn_pause.grid_info()["column"]), 1)
        self.assertEqual(int(app.btn_preview.grid_info()["column"]), 2)
        self.assertEqual(int(app.btn_retry_failed.grid_info()["column"]), 3)
        self.assertEqual(int(app.tasks_frame.grid_info()["row"]), 0)
        self.assertEqual(int(app.tasks_frame.grid_info()["column"]), 0)
        self.assertEqual(int(app.wsl_storage_frame.grid_info()["row"]), 1)
        self.assertEqual(int(app.supabase_action_row.grid_info()["row"]), 0)
        self.assertEqual(int(app.supabase_action_row.grid_info()["column"]), 0)
        self.assertEqual(int(app.upload_action_row.grid_info()["row"]), 1)
        self.assertEqual(int(app.upload_action_row.grid_info()["column"]), 0)

        app.set_local_supabase_progress_visible(True)
        pump(app)
        self.assertEqual(int(app.local_supabase_progress.grid_info()["row"]), 6)
        self.assertEqual(int(app.lbl_state_store_status.grid_info()["row"]), 7)

    def test_build_upload_operational_cards_state_uses_failed_retry_set_and_recent_profile(self) -> None:
        workspace = Path(tempfile.mkdtemp(prefix="dashboard-state-smoke-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        plc_dir = workspace / "plc"
        plc_dir.mkdir(parents=True, exist_ok=True)
        first_path = plc_dir / "260421_first.csv"
        second_path = plc_dir / "260421_second.csv"
        first_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")
        second_path.write_text("timestamp,value\n2026-04-21T00:00:01+09:00,2\n", encoding="utf-8")

        dashboard_state_snapshot: core_state.UploadDashboardStateSnapshot = {
            "resume": {
                core_state.build_file_state_key(str(plc_dir), first_path.name, str(first_path)): 5,
            },
            "recent_successful_upload_profile": {
                "profile_name": "recent-profile",
                "applied_at": 100.0,
                "values": {"PLC_DIR": str(plc_dir)},
            },
            "failed_retry_set": (
                {
                    "file_key": core_state.build_file_state_key(str(plc_dir), second_path.name, str(second_path)),
                    "folder": str(plc_dir),
                    "filename": second_path.name,
                    "legacy_key": core_state.build_legacy_file_key(str(plc_dir), second_path.name),
                    "resume_offset": 3,
                    "retry_count": 2,
                    "failed_at": 200.0,
                    "error_message": "boom",
                },
            ),
        }
        state_health_snapshot: core_state.StateHealthSnapshot = {
            "state": "attention",
            "read_mode": "sqlite",
            "can_start_upload": True,
            "pending_resume_count": 1,
            "failed_retry_count": 1,
            "recovery_action_required": True,
            "summary_code": "recovery_available",
            "detail_codes": ("failed_retry_present", "resume_present", "can_resume"),
            "backup_dir": str(workspace / "migration_backups"),
        }
        vals = {
            "PLC_DIR": str(plc_dir),
            "RANGE_MODE": "today",
            "CUSTOM_DATE_START": "",
            "CUSTOM_DATE_END": "",
            "CUSTOM_DATE": "",
            "MTIME_LAG_MIN": "15",
            "CHECK_LOCK": "true",
            "EDGE_FUNCTION_URL": "",
            "SUPABASE_URL": "http://localhost:54321",
        }
        preview_scan_result = uploader_gui_tk.build_preview_scan_result(
            vals,
            [
                (str(plc_dir), first_path.name, str(first_path), "plc"),
                (str(plc_dir), second_path.name, str(second_path), "plc"),
            ],
            [],
            time.time(),
            lambda key, kwargs: key if kwargs == {} else f"{key}:{kwargs}",
        )

        with patch.object(uploader_gui_tk, "validate_config", return_value=(True, [])):
            with patch.object(uploader_gui_tk.core_files, "build_upload_preflight_plan", side_effect=AssertionError("dashboard preflight scan must not run")):
                with patch.object(uploader_gui_tk, "is_edge_url_origin_mismatch", return_value=False):
                    with patch.object(uploader_gui_tk, "is_local_supabase_target", return_value=False):
                        operational_state = uploader_gui_tk.build_upload_operational_cards_state(
                            vals,
                            dashboard_state_snapshot,
                            state_health_snapshot,
                            preview_scan_result,
                            "ready",
                            lambda key, kwargs: key if kwargs == {} else f"{key}:{kwargs}",
                        )

        self.assertEqual(len(operational_state.retryable_upload_items), 0)
        self.assertTrue(operational_state.has_retryable_state)
        self.assertEqual(operational_state.failed_retry_set, dashboard_state_snapshot["failed_retry_set"])
        self.assertTrue(operational_state.can_rerun_recent_success)
        self.assertEqual(operational_state.state_health_status_text, "dashboard.state_store.status.recovery_available")
        self.assertEqual(operational_state.preflight_status_text, "dashboard.upload.status.preflight_ready")
        self.assertEqual(
            operational_state.preflight_detail_lines,
            ("dashboard.upload.detail.preflight_summary:{'target_count': 2, 'total_count': 2}",),
        )
        self.assertIn(second_path.name, operational_state.resume_detail_lines)
        self.assertIn("recent-profile", operational_state.recent_success_detail_lines)

    def test_build_upload_operational_cards_state_blocks_upload_when_state_health_is_blocked(self) -> None:
        dashboard_state_snapshot: core_state.UploadDashboardStateSnapshot = {
            "resume": {},
            "recent_successful_upload_profile": None,
            "failed_retry_set": (),
        }
        state_health_snapshot: core_state.StateHealthSnapshot = {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "corruption",
            "detail_codes": ("restore_sqlite", "backup_dir"),
            "error_message": "db-corrupt",
            "backup_dir": "C:/backup",
        }
        vals = {
            "PLC_DIR": "",
            "RANGE_MODE": "today",
            "CUSTOM_DATE_START": "",
            "CUSTOM_DATE_END": "",
            "CUSTOM_DATE": "",
            "MTIME_LAG_MIN": "15",
            "CHECK_LOCK": "true",
            "EDGE_FUNCTION_URL": "",
            "SUPABASE_URL": "http://localhost:54321",
        }
        with patch.object(uploader_gui_tk, "validate_config", return_value=(True, [])):
            with patch.object(uploader_gui_tk, "is_edge_url_origin_mismatch", return_value=False):
                with patch.object(uploader_gui_tk, "is_local_supabase_target", return_value=False):
                    operational_state = uploader_gui_tk.build_upload_operational_cards_state(
                        vals,
                        dashboard_state_snapshot,
                        state_health_snapshot,
                        None,
                        "ready",
                        lambda key, kwargs: key if kwargs == {} else f"{key}:{kwargs}",
                    )

        self.assertTrue(operational_state.state_health_blocks_upload)
        self.assertTrue(operational_state.is_upload_preflight_blocked)
        self.assertIn("db-corrupt", operational_state.state_health_detail_lines)

    def test_build_dashboard_hero_view_state_prioritizes_blocked_state(self) -> None:
        operational_state = uploader_gui_tk.UploadOperationalCardsState(
            recent_successful_upload_profile=None,
            failed_retry_set=(),
            state_health_status_text="state-blocked",
            state_health_status_color="#E06C75",
            state_health_detail_lines=("restore required",),
            state_health_blocks_upload=True,
            is_upload_preflight_blocked=True,
            has_retryable_state=True,
            retryable_upload_items=(),
            preflight_status_text="preflight-blocked",
            preflight_status_color="#E06C75",
            preflight_detail_lines=("preflight issue",),
            resume_status_text="resume-available",
            resume_status_color="#E5C07B",
            resume_detail_lines=("retry item",),
            recent_success_status_text="recent-success",
            recent_success_status_color="#2CC985",
            recent_success_detail_lines=("profile-a",),
            can_rerun_recent_success=True,
        )

        hero_view_state = uploader_gui_tk.build_dashboard_hero_view_state(
            operational_state,
            "waiting",
        )

        self.assertEqual(hero_view_state.status_kind, "blocked")
        self.assertEqual(hero_view_state.status_text, "state-blocked")
        self.assertEqual(hero_view_state.detail_text, "restore required")

    def test_build_dashboard_hero_view_state_keeps_preflight_detail_when_recent_success_exists(self) -> None:
        operational_state = uploader_gui_tk.UploadOperationalCardsState(
            recent_successful_upload_profile={"profile_name": "profile-a"},
            failed_retry_set=(),
            state_health_status_text="healthy",
            state_health_status_color="#2CC985",
            state_health_detail_lines=(),
            state_health_blocks_upload=False,
            is_upload_preflight_blocked=False,
            has_retryable_state=False,
            retryable_upload_items=(),
            preflight_status_text="preflight-ready",
            preflight_status_color="#2CC985",
            preflight_detail_lines=("targets: 2",),
            resume_status_text="resume-empty",
            resume_status_color="gray",
            resume_detail_lines=(),
            recent_success_status_text="profile-a",
            recent_success_status_color="#2CC985",
            recent_success_detail_lines=("profile-a", "2026-04-22 09:00:00"),
            can_rerun_recent_success=True,
        )

        hero_view_state = uploader_gui_tk.build_dashboard_hero_view_state(
            operational_state,
            "waiting",
        )

        self.assertEqual(hero_view_state.status_kind, "ready")
        self.assertEqual(hero_view_state.status_text, "preflight-ready")
        self.assertEqual(hero_view_state.detail_text, "targets: 2")

    def test_dashboard_layout_uses_stacked_body_and_split_footer_at_default_width(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-layout-default-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.show_dashboard()
        pump(app)
        self.refresh_dashboard_layout_with_widths(app, body_width=1032, footer_width=1032)

        self.assertFalse(app.dashboard_layout_signature.is_split_body)
        self.assertTrue(app.dashboard_layout_signature.is_split_footer)
        self.assertEqual(int(app.tasks_frame.grid_info()["row"]), 0)
        self.assertEqual(int(app.tasks_frame.grid_info()["column"]), 0)
        self.assertEqual(int(app.wsl_storage_frame.grid_info()["row"]), 1)
        self.assertEqual(int(app.wsl_storage_frame.grid_info()["column"]), 0)
        self.assertEqual(int(app.supabase_action_row.grid_info()["row"]), 0)
        self.assertEqual(int(app.supabase_action_row.grid_info()["column"]), 0)
        self.assertEqual(int(app.upload_action_row.grid_info()["row"]), 0)
        self.assertEqual(int(app.upload_action_row.grid_info()["column"]), 1)

    def test_dashboard_layout_keeps_tasks_first_when_body_is_stacked(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-layout-stacked-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.show_dashboard()
        pump(app)
        self.refresh_dashboard_layout_with_widths(app, body_width=952, footer_width=930)

        self.assertFalse(app.dashboard_layout_signature.is_split_body)
        self.assertFalse(app.dashboard_layout_signature.is_split_footer)
        self.assertEqual(int(app.tasks_frame.grid_info()["row"]), 0)
        self.assertEqual(int(app.tasks_frame.grid_info()["column"]), 0)
        self.assertEqual(int(app.wsl_storage_frame.grid_info()["row"]), 1)
        self.assertEqual(int(app.wsl_storage_frame.grid_info()["column"]), 0)
        self.assertEqual(int(app.supabase_action_row.grid_info()["row"]), 0)
        self.assertEqual(int(app.upload_action_row.grid_info()["row"]), 1)
        self.assertEqual(int(app.btn_start.grid_info()["row"]), 0)
        self.assertEqual(int(app.btn_start.grid_info()["column"]), 0)
        self.assertEqual(int(app.btn_retry_failed.grid_info()["column"]), 3)

    def test_dashboard_layout_stacks_upload_buttons_with_primary_first(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-layout-stack-buttons-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.show_dashboard()
        pump(app)
        self.refresh_dashboard_layout_with_widths(app, body_width=952, footer_width=680)

        self.assertTrue(app.dashboard_layout_signature.button_stack)
        self.assertEqual(int(app.btn_start.grid_info()["row"]), 0)
        self.assertEqual(int(app.btn_pause.grid_info()["row"]), 1)
        self.assertEqual(int(app.btn_preview.grid_info()["row"]), 2)
        self.assertEqual(int(app.btn_retry_failed.grid_info()["row"]), 3)
        self.assertEqual(int(app.btn_start_supabase.grid_info()["row"]), 0)
        self.assertEqual(int(app.btn_open_studio.grid_info()["row"]), 1)
        self.assertEqual(int(app.btn_stop_supabase.grid_info()["row"]), 2)

    def test_dashboard_action_button_lists_match_render_priority(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-button-contract-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        self.assertEqual(
            app.upload_action_buttons,
            [app.btn_start, app.btn_pause, app.btn_preview, app.btn_retry_failed],
        )
        self.assertEqual(
            app.supabase_action_buttons,
            [app.btn_start_supabase, app.btn_open_studio, app.btn_stop_supabase],
        )

    def test_update_dashboard_loop_uses_hero_state_when_idle(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-hero-state-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.show_dashboard()
        pump(app)
        app.is_uploading = False
        app.upload_dashboard_status_text = app.tr("common.status.waiting")
        app.upload_dashboard_status_color = "gray"
        app.dashboard_hero_view_state = uploader_gui_tk.DashboardHeroViewState(
            status_kind="attention",
            status_text="resume-available",
            status_color="#E5C07B",
            detail_text="retry item",
            detail_color="#E5C07B",
        )

        app.update_dashboard_loop(app.dashboard_view_generation)
        pump(app)

        self.assertEqual(app.lbl_big_status.cget("text"), "resume-available")
        self.assertEqual(app.lbl_hero_status_detail.cget("text"), "retry item")

    def test_refresh_active_task_labels_toggles_empty_state_and_renders_progress(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-active-tasks-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.show_dashboard()
        pump(app)
        app.refresh_active_task_labels()
        pump(app)

        self.assertTrue(app.lbl_active_tasks_empty.winfo_ismapped())
        self.assertEqual(app.task_labels, {})

        with app.progress_lock:
            app.active_progress = {"task-a": (2, 4)}

        app.refresh_active_task_labels()
        pump(app)

        self.assertFalse(app.lbl_active_tasks_empty.winfo_ismapped())
        self.assertIn("task-a", app.task_labels)
        self.assertEqual(app.task_labels["task-a"].cget("text"), app.tr("dashboard.active_task.progress", task_key="task-a", percent=50))

    def test_build_wsl_storage_ui_snapshot_keeps_guest_metrics_as_primary_basis(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-wsl-guest-basis-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.cfg["SUPABASE_URL"] = "http://127.0.0.1:54321"
        app.wsl_storage_raw_snapshot = uploader_gui_tk.core_wsl_storage.WslStorageSnapshot(
            state="warning",
            guest_metrics=uploader_gui_tk.core_wsl_storage.WslGuestStorageMetrics(
                distro_name="Ubuntu",
                mount_path="/",
                total_bytes=100,
                used_bytes=85,
                available_bytes=15,
                usage_ratio=0.85,
            ),
            host_metrics=uploader_gui_tk.core_wsl_storage.WslHostStorageMetrics(
                vhdx_path=Path("C:/WSL/ext4.vhdx"),
                file_size_bytes=300,
                drive_free_bytes=700,
                drive_total_bytes=1000,
                source="config_override",
            ),
            issues=(),
            is_partial=False,
            collected_at=datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc),
        )
        app.wsl_storage_error_detail = ""
        app.is_wsl_storage_refreshing = False

        snapshot = app.build_wsl_storage_ui_snapshot()

        self.assertEqual(snapshot.metric_basis, "guest")
        self.assertEqual(snapshot.used_label_text, app.tr("dashboard.wsl_storage.label.used"))
        self.assertEqual(snapshot.available_label_text, app.tr("dashboard.wsl_storage.label.available"))
        self.assertEqual(snapshot.total_label_text, app.tr("dashboard.wsl_storage.label.total"))
        self.assertEqual(snapshot.usage_label_text, app.tr("dashboard.wsl_storage.label.usage"))
        self.assertEqual(snapshot.used_text, "85 B")
        self.assertEqual(snapshot.available_text, "15 B")
        self.assertEqual(snapshot.total_text, "100 B")
        self.assertEqual(snapshot.usage_text, "85%")
        self.assertEqual(snapshot.progress_value, 0.85)
        self.assertEqual(snapshot.metric_basis_text, app.tr("dashboard.wsl_storage.metric_basis.guest"))
        self.assertEqual(snapshot.vhdx_text, "300 B")
        self.assertEqual(snapshot.host_free_text, "700 B")
        self.assertEqual(snapshot.source_text, app.tr("dashboard.wsl_storage.source.config_override"))
        self.assertEqual(snapshot.path_text, str(Path("C:/WSL/ext4.vhdx")))

    def test_build_wsl_storage_ui_snapshot_marks_refresh_failures_as_stale(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-wsl-stale-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.cfg["SUPABASE_URL"] = "http://127.0.0.1:54321"
        app.wsl_storage_raw_snapshot = uploader_gui_tk.core_wsl_storage.WslStorageSnapshot(
            state="safe",
            guest_metrics=uploader_gui_tk.core_wsl_storage.WslGuestStorageMetrics(
                distro_name="Ubuntu",
                mount_path="/",
                total_bytes=100,
                used_bytes=40,
                available_bytes=60,
                usage_ratio=0.4,
            ),
            host_metrics=None,
            issues=(),
            is_partial=False,
            collected_at=datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc),
        )
        app.wsl_storage_error_detail = "refresh_failed"
        app.is_wsl_storage_refreshing = False

        snapshot = app.build_wsl_storage_ui_snapshot()

        self.assertTrue(snapshot.is_stale)
        self.assertFalse(snapshot.is_partial)
        self.assertEqual(snapshot.state, "partial")
        self.assertIn(app.tr("dashboard.wsl_storage.issue.refresh_failed"), snapshot.detail_text)
        self.assertIn(app.tr("dashboard.wsl_storage.meta.cached"), snapshot.last_updated_text)

    def test_build_wsl_storage_ui_snapshot_preserves_partial_state_without_error_message(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-wsl-partial-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.cfg["SUPABASE_URL"] = "http://127.0.0.1:54321"
        app.wsl_storage_raw_snapshot = SimpleNamespace(
            state="partial",
            guest_metrics=SimpleNamespace(
                distro_name="Ubuntu",
                used_bytes=30,
                available_bytes=70,
                total_bytes=100,
                usage_ratio=0.3,
            ),
            host_metrics=None,
            issues=(),
            is_partial=True,
            collected_at=datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc),
        )
        app.wsl_storage_error_detail = ""
        app.is_wsl_storage_refreshing = False

        snapshot = app.build_wsl_storage_ui_snapshot()

        self.assertEqual(snapshot.state, "partial")
        self.assertNotIn(app.tr("dashboard.wsl_storage.message.error"), snapshot.detail_text)

    def test_render_wsl_storage_card_uses_neutral_progress_when_snapshot_is_stale(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-wsl-render-stale-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.show_dashboard()
        pump(app)
        app.cfg["SUPABASE_URL"] = "http://127.0.0.1:54321"
        app.wsl_storage_raw_snapshot = uploader_gui_tk.core_wsl_storage.WslStorageSnapshot(
            state="warning",
            guest_metrics=uploader_gui_tk.core_wsl_storage.WslGuestStorageMetrics(
                distro_name="Ubuntu",
                mount_path="/",
                total_bytes=100,
                used_bytes=85,
                available_bytes=15,
                usage_ratio=0.85,
            ),
            host_metrics=None,
            issues=(),
            is_partial=False,
            collected_at=datetime(2026, 4, 22, 0, 0, tzinfo=timezone.utc),
        )
        app.wsl_storage_error_detail = "refresh_failed"
        app.is_wsl_storage_refreshing = False

        app.render_wsl_storage_card()

        self.assertEqual(app.lbl_wsl_storage_badge.cget("text"), app.tr("dashboard.wsl_storage.badge.partial"))
        self.assertEqual(app.wsl_storage_progress.cget("progress_color"), "gray")
        self.assertEqual(
            app.lbl_wsl_storage_metric_basis_value.cget("text"),
            app.tr("dashboard.wsl_storage.metric_basis.guest"),
        )
        self.assertEqual(app.lbl_wsl_storage_source_value.cget("text"), app.tr("dashboard.wsl_storage.source.unavailable"))
        self.assertEqual(app.lbl_wsl_storage_path_value.cget("text"), "—")

    def test_start_upload_with_values_blocks_when_state_health_is_blocked(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-blocked-start-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        blocked_health_snapshot: core_state.StateHealthSnapshot = {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "corruption",
            "detail_codes": ("restore_sqlite",),
            "error_message": "db-corrupt",
            "backup_dir": str(appdata_root / "ExtrusionUploader" / "migration_backups"),
        }
        vals = {
            "SUPABASE_URL": "http://127.0.0.1:54321",
            "EDGE_FUNCTION_URL": "",
        }

        with patch.object(uploader_gui_tk, "load_state_health_snapshot", return_value=blocked_health_snapshot):
            with patch.object(uploader_gui_tk.messagebox, "showwarning") as showwarning:
                with patch.object(app, "ensure_local_supabase_ready", side_effect=AssertionError("must not reach supabase check")):
                    app.start_upload_with_values(vals, False)

        self.assertFalse(app.is_uploading)
        self.assertTrue(app.state_health_blocks_upload)
        showwarning.assert_called_once()

    def test_auto_start_upload_does_not_call_on_start_when_state_health_is_blocked(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-blocked-auto-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        blocked_health_snapshot: core_state.StateHealthSnapshot = {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "corruption",
            "detail_codes": ("restore_sqlite",),
            "error_message": "db-corrupt",
            "backup_dir": str(appdata_root / "ExtrusionUploader" / "migration_backups"),
        }
        on_start_mock = Mock()
        app.on_start = on_start_mock

        with patch.object(uploader_gui_tk, "load_state_health_snapshot", return_value=blocked_health_snapshot):
            with patch.object(uploader_gui_tk.messagebox, "showwarning"):
                app.auto_start_upload()

        on_start_mock.assert_not_called()

    def test_retry_and_rerun_actions_do_not_start_upload_when_state_health_is_blocked(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-blocked-retry-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        blocked_health_snapshot: core_state.StateHealthSnapshot = {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "corruption",
            "detail_codes": ("restore_sqlite",),
            "error_message": "db-corrupt",
            "backup_dir": str(appdata_root / "ExtrusionUploader" / "migration_backups"),
        }
        app.start_upload_with_values = Mock()

        with patch.object(uploader_gui_tk, "load_state_health_snapshot", return_value=blocked_health_snapshot):
            with patch.object(uploader_gui_tk.messagebox, "showwarning"):
                app.on_retry_failed()
                app.on_rerun_recent_success()

        app.start_upload_with_values.assert_not_called()

    def test_run_preview_logic_logs_completion(self) -> None:
        log_entries: list[tuple[str, str]] = []
        scheduled_callbacks: list[tuple[int, object]] = []
        refresh_calls: list[str] = []
        fake_app = SimpleNamespace()
        fake_app.cfg = {}
        fake_app.config_source = ""
        fake_app.config_metadata = {}
        fake_app.tr_map = lambda key, params: key if params == {} else f"{key}:{params}"
        fake_app.tr = lambda key, **params: key if params == {} else f"{key}:{params}"
        fake_app.log = lambda msg, level="INFO": log_entries.append((level, msg))
        fake_app.refresh_upload_operational_cards = lambda: refresh_calls.append("refresh")
        fake_app.schedule_gui_callback = lambda delay_ms, callback, *args: (
            scheduled_callbacks.append((delay_ms, callback)),
            callback(*args),
        )[-1]

        with patch.object(
            uploader_gui_tk,
            "load_config_with_sources",
            return_value=(
                {
                    "PLC_DIR": "C:/plc",
                    "RANGE_MODE": "today",
                    "CUSTOM_DATE_START": "",
                    "CUSTOM_DATE_END": "",
                    "CUSTOM_DATE": "",
                    "MTIME_LAG_MIN": "15",
                    "CHECK_LOCK": "true",
                },
                "config.ini",
                {},
            ),
        ):
            with patch.object(
                uploader_gui_tk,
                "preview_diagnostics",
                return_value=([("C:/plc", "sample.csv", "C:/plc/sample.csv", "plc")], []),
            ):
                with patch.object(uploader_gui_tk, "load_processed", return_value=set()):
                    with patch.object(
                        uploader_gui_tk,
                        "build_preview_scan_result",
                        return_value=SimpleNamespace(),
                    ):
                        uploader_gui_tk.App._run_preview_logic(fake_app)

        self.assertIn(("INFO", "dashboard.preview.log.target_count:{'count': 1}"), log_entries)
        self.assertIn(("INFO", " - sample.csv"), log_entries)
        self.assertIn(("INFO", "logs.preview.completed"), log_entries)
        self.assertEqual(len(scheduled_callbacks), 1)
        self.assertEqual(scheduled_callbacks[0][0], 0)
        self.assertEqual(refresh_calls, ["refresh"])

    def test_run_preview_logic_logs_failure_instead_of_silently_stopping(self) -> None:
        log_entries: list[tuple[str, str]] = []
        fake_app = SimpleNamespace()
        fake_app.cfg = {}
        fake_app.config_source = ""
        fake_app.config_metadata = {}
        fake_app.tr_map = lambda key, params: key if params == {} else f"{key}:{params}"
        fake_app.tr = lambda key, **params: key if params == {} else f"{key}:{params}"
        fake_app.log = lambda msg, level="INFO": log_entries.append((level, msg))

        with patch.object(
            uploader_gui_tk,
            "load_config_with_sources",
            return_value=(
                {
                    "PLC_DIR": "",
                    "RANGE_MODE": "today",
                },
                "config.ini",
                {},
            ),
        ):
            uploader_gui_tk.App._run_preview_logic(fake_app)

        self.assertEqual(log_entries[-1][0], "ERROR")
        self.assertIn("logs.preview.failed", log_entries[-1][1])

    def test_resolve_upload_candidate_items_reuses_recent_preview_result(self) -> None:
        workspace = Path(tempfile.mkdtemp(prefix="dashboard-preview-reuse-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        plc_dir = workspace / "plc"
        plc_dir.mkdir(parents=True, exist_ok=True)
        first_path = plc_dir / "260421_first.csv"
        first_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")
        vals = {
            "PLC_DIR": str(plc_dir),
            "RANGE_MODE": "custom",
            "CUSTOM_DATE_START": "2026-04-21",
            "CUSTOM_DATE_END": "2026-04-21",
            "CUSTOM_DATE": "",
            "MTIME_LAG_MIN": "15",
            "CHECK_LOCK": "true",
        }
        preview_scan_result = uploader_gui_tk.build_preview_scan_result(
            vals,
            [(str(plc_dir), first_path.name, str(first_path), "plc")],
            [],
            time.time(),
            lambda key, kwargs: key if kwargs == {} else f"{key}:{kwargs}",
        )
        window_start, window_end = uploader_gui_tk.compute_date_window("custom", "2026-04-21", "2026-04-21")
        fake_app = SimpleNamespace(last_preview_scan_result=preview_scan_result)

        with patch.object(uploader_gui_tk, "load_processed", return_value=set()):
            with patch.object(uploader_gui_tk, "list_candidates", side_effect=AssertionError("must not rescan")):
                items, reused_preview = uploader_gui_tk.App.resolve_upload_candidate_items(
                    fake_app,
                    vals,
                    window_start,
                    window_end,
                    15,
                    False,
                    True,
                    False,
                )

        self.assertTrue(reused_preview)
        self.assertEqual(items, [(str(plc_dir), first_path.name, str(first_path), "plc")])

    def test_resolve_upload_candidate_items_rescans_when_preview_is_stale(self) -> None:
        workspace = Path(tempfile.mkdtemp(prefix="dashboard-preview-stale-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        plc_dir = workspace / "plc"
        plc_dir.mkdir(parents=True, exist_ok=True)
        first_path = plc_dir / "260421_first.csv"
        first_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")
        vals = {
            "PLC_DIR": str(plc_dir),
            "RANGE_MODE": "custom",
            "CUSTOM_DATE_START": "2026-04-21",
            "CUSTOM_DATE_END": "2026-04-21",
            "CUSTOM_DATE": "",
            "MTIME_LAG_MIN": "15",
            "CHECK_LOCK": "true",
        }
        preview_scan_result = uploader_gui_tk.build_preview_scan_result(
            vals,
            [(str(plc_dir), first_path.name, str(first_path), "plc")],
            [],
            time.time() - uploader_gui_tk.PREVIEW_REUSE_TTL_SECONDS - 1.0,
            lambda key, kwargs: key if kwargs == {} else f"{key}:{kwargs}",
        )
        window_start, window_end = uploader_gui_tk.compute_date_window("custom", "2026-04-21", "2026-04-21")
        fake_app = SimpleNamespace(last_preview_scan_result=preview_scan_result)
        expected_items = [(str(plc_dir), first_path.name, str(first_path), "plc")]

        with patch.object(uploader_gui_tk, "list_candidates", return_value=expected_items) as list_candidates_mock:
            items, reused_preview = uploader_gui_tk.App.resolve_upload_candidate_items(
                fake_app,
                vals,
                window_start,
                window_end,
                15,
                False,
                True,
                False,
            )

        self.assertFalse(reused_preview)
        self.assertEqual(items, expected_items)
        list_candidates_mock.assert_called_once()

    def test_refresh_upload_action_buttons_enables_retry_from_state_without_candidates(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-retry-state-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)

        app.is_uploading = False
        app.state_health_blocks_upload = False
        app.is_upload_preflight_blocked = False
        app.has_retryable_state = True
        app.retryable_upload_items = []

        app.refresh_upload_action_buttons()

        self.assertEqual(app.btn_retry_failed.cget("state"), "normal")

    def test_on_preview_blocks_when_state_health_is_blocked(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="dashboard-blocked-preview-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        blocked_health_snapshot: core_state.StateHealthSnapshot = {
            "state": "blocked",
            "read_mode": "sqlite",
            "can_start_upload": False,
            "pending_resume_count": 0,
            "failed_retry_count": 0,
            "recovery_action_required": True,
            "summary_code": "corruption",
            "detail_codes": ("restore_sqlite",),
            "error_message": "db-corrupt",
            "backup_dir": str(appdata_root / "ExtrusionUploader" / "migration_backups"),
        }

        with patch.object(uploader_gui_tk, "load_state_health_snapshot", return_value=blocked_health_snapshot):
            with patch.object(uploader_gui_tk.messagebox, "showwarning") as showwarning:
                with patch.object(uploader_gui_tk.threading, "Thread", side_effect=AssertionError("preview thread must not start")):
                    app.on_preview()

        self.assertTrue(app.state_health_blocks_upload)
        showwarning.assert_called_once()
