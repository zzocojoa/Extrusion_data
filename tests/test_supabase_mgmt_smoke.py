import shutil
import tempfile
import tkinter as tk
from pathlib import Path
from types import SimpleNamespace
from unittest import SkipTest, TestCase
from unittest.mock import MagicMock, patch

import uploader_gui_tk
from core.supabase_mgmt import SupabaseMetricDateRow


def pump(app: uploader_gui_tk.App) -> None:
    app.update_idletasks()
    app.update()


class ImmediateThread:
    def __init__(self, target: object, daemon: bool) -> None:
        self.target = target
        self.daemon = daemon

    def start(self) -> None:
        if callable(self.target):
            self.target()


class SupabaseMgmtSmokeTests(TestCase):
    def create_app(self, appdata_root: Path) -> uploader_gui_tk.App:
        data_dir = appdata_root / "ExtrusionUploader"
        data_dir.mkdir(parents=True, exist_ok=True)
        env_patcher = patch.dict("os.environ", {"APPDATA": str(appdata_root)}, clear=False)
        data_dir_patcher = patch.object(uploader_gui_tk, "DATA_DIR", str(data_dir))
        state_db_path_patcher = patch.object(
            uploader_gui_tk,
            "STATE_DB_PATH",
            str(data_dir / "uploader_state.db"),
        )
        tcp_patcher = patch.object(uploader_gui_tk, "can_connect_tcp", return_value=False)
        wsl_patcher = patch.object(
            uploader_gui_tk.App,
            "request_wsl_storage_refresh",
            new=lambda self: None,
        )
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

    def build_sample_rows(self) -> tuple[SupabaseMetricDateRow, ...]:
        return (
            SupabaseMetricDateRow(
                kst_date=uploader_gui_tk.date(2026, 4, 21),
                row_count=3,
                min_timestamp="2026-04-20T15:00:00+00:00",
                max_timestamp="2026-04-20T15:30:00+00:00",
            ),
            SupabaseMetricDateRow(
                kst_date=uploader_gui_tk.date(2026, 4, 22),
                row_count=5,
                min_timestamp="2026-04-21T15:00:00+00:00",
                max_timestamp="2026-04-21T16:00:00+00:00",
            ),
        )

    def show_supabase_mgmt_with_rows(
        self,
        app: uploader_gui_tk.App,
        sample_rows: tuple[SupabaseMetricDateRow, ...],
    ) -> None:
        with patch.object(uploader_gui_tk.App, "ensure_local_supabase_ready", return_value=True):
            with patch.object(uploader_gui_tk, "load_supabase_mgmt_rows", return_value=sample_rows):
                with patch.object(uploader_gui_tk.threading, "Thread", ImmediateThread):
                    app.show_supabase_mgmt()
                    pump(app)

    def test_show_supabase_mgmt_renders_rows_and_updates_selection_summary(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-appdata-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()
        self.show_supabase_mgmt_with_rows(app, sample_rows)

        self.assertEqual(app.current_view, "supabase_mgmt")
        self.assertEqual(len(app.supabase_mgmt_row_widgets), 2)
        self.assertEqual(
            app.var_supabase_mgmt_summary.get(),
            app.tr(
                "supabase_mgmt.summary.total",
                date_count=2,
                row_count="8",
                total_date_count=2,
                total_row_count="8",
            ),
        )
        self.assertEqual(app.btn_supabase_delete_all.cget("state"), "normal")

        app.supabase_mgmt_row_widgets[0].selected_var.set(True)
        app.refresh_supabase_mgmt_selection_summary()

        self.assertEqual(
            app.var_supabase_mgmt_selection_summary.get(),
            app.tr(
                "supabase_mgmt.selection.detail",
                date_count=1,
                row_count="3",
                first_date="2026-04-21",
                last_date="2026-04-21",
            ),
        )
        self.assertEqual(app.btn_supabase_delete_selected.cget("state"), "normal")

    def test_supabase_mgmt_nested_scroll_uses_nearest_canvas_only(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-scroll-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()
        self.show_supabase_mgmt_with_rows(app, sample_rows)

        outer_canvas = getattr(app.supabase_mgmt_overview_shell.master, "_parent_canvas", None)
        inner_canvas = getattr(app.supabase_mgmt_rows_frame, "_parent_canvas", None)
        self.assertIsNotNone(outer_canvas)
        self.assertIsNotNone(inner_canvas)
        self.assertIsNot(inner_canvas, outer_canvas)

        target_widget = app.supabase_mgmt_row_widgets[0].checkbox
        self.assertTrue(app.supabase_mgmt_rows_frame.check_if_master_is_canvas(target_widget))
        self.assertFalse(app.supabase_mgmt_overview_shell.master.check_if_master_is_canvas(target_widget))

    def test_supabase_mgmt_date_lookup_filters_rows_immediately(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-filter-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()
        self.show_supabase_mgmt_with_rows(app, sample_rows)

        app.var_supabase_mgmt_filter_query.set("2026-04-22")
        pump(app)

        self.assertEqual(len(app.supabase_mgmt_rows), 1)
        self.assertEqual(app.supabase_mgmt_rows[0].kst_date.isoformat(), "2026-04-22")
        self.assertEqual(
            app.var_supabase_mgmt_summary.get(),
            app.tr(
                "supabase_mgmt.summary.total",
                date_count=1,
                row_count="5",
                total_date_count=2,
                total_row_count="8",
            ),
        )
        self.assertEqual(
            app.var_supabase_mgmt_detail.get(),
            app.tr("supabase_mgmt.filter.active", query="2026-04-22"),
        )
        self.assertEqual(app.btn_supabase_delete_all.cget("state"), "normal")

    def test_supabase_mgmt_buttons_follow_data_task_busy_state(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-busy-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()[:1]
        self.show_supabase_mgmt_with_rows(app, sample_rows)

        app.supabase_mgmt_row_widgets[0].selected_var.set(True)
        app.refresh_supabase_mgmt_selection_summary()
        app.disable_data_mgmt_buttons()

        self.assertEqual(app.btn_supabase_refresh.cget("state"), "disabled")
        self.assertEqual(app.btn_supabase_delete_selected.cget("state"), "disabled")
        self.assertEqual(app.btn_supabase_delete_all.cget("state"), "disabled")
        self.assertEqual(app.supabase_mgmt_row_widgets[0].checkbox.cget("state"), "disabled")
        self.assertEqual(app.supabase_mgmt_row_widgets[0].delete_button.cget("state"), "disabled")

        app.enable_data_mgmt_buttons()

        self.assertEqual(app.btn_supabase_refresh.cget("state"), "normal")
        self.assertEqual(app.btn_supabase_delete_selected.cget("state"), "normal")
        self.assertEqual(app.btn_supabase_delete_all.cget("state"), "normal")
        self.assertEqual(app.supabase_mgmt_row_widgets[0].checkbox.cget("state"), "normal")
        self.assertEqual(app.supabase_mgmt_row_widgets[0].delete_button.cget("state"), "normal")

    def test_supabase_mgmt_delete_selected_wires_preview_and_resets_selection_after_success(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-delete-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()
        remaining_rows = (sample_rows[1],)
        captured_preview: list[object] = []

        def execute_delete(_project_root: Path, preview: object) -> SimpleNamespace:
            captured_preview.append(preview)
            return SimpleNamespace(date_rows=(sample_rows[0],))

        with patch.object(uploader_gui_tk.App, "ensure_local_supabase_ready", return_value=True):
            with patch.object(
                uploader_gui_tk,
                "load_supabase_mgmt_rows",
                side_effect=[sample_rows, remaining_rows],
            ):
                with patch.object(uploader_gui_tk, "execute_supabase_delete", side_effect=execute_delete):
                    with patch.object(uploader_gui_tk.messagebox, "askyesno", return_value=True):
                        with patch.object(uploader_gui_tk.threading, "Thread", ImmediateThread):
                            app.show_supabase_mgmt()
                            pump(app)
                            app.supabase_mgmt_row_widgets[0].selected_var.set(True)
                            app.refresh_supabase_mgmt_selection_summary()
                            app.on_supabase_mgmt_delete_selected()
                            pump(app)

        self.assertEqual(len(captured_preview), 1)
        delete_preview = captured_preview[0]
        self.assertEqual(
            delete_preview.request.selection_mode,
            uploader_gui_tk.SUPABASE_DELETE_MODE_SELECTED,
        )
        self.assertEqual(
            tuple(date_value.isoformat() for date_value in delete_preview.request.dates),
            ("2026-04-21",),
        )
        self.assertEqual(len(app.supabase_mgmt_rows), 1)
        self.assertEqual(app.supabase_mgmt_rows[0].kst_date.isoformat(), "2026-04-22")
        self.assertEqual(app.supabase_mgmt_rows[0].row_count, 5)
        self.assertEqual(len(app.supabase_mgmt_row_widgets), 1)
        self.assertEqual(app.var_supabase_mgmt_selection_summary.get(), app.tr("supabase_mgmt.selection.none"))
        self.assertEqual(
            app.var_supabase_mgmt_summary.get(),
            app.tr(
                "supabase_mgmt.summary.total",
                date_count=1,
                row_count="5",
                total_date_count=1,
                total_row_count="5",
            ),
        )
        self.assertEqual(app.btn_supabase_delete_selected.cget("state"), "disabled")
        self.assertEqual(app.btn_supabase_delete_all.cget("state"), "normal")
        self.assertEqual(app.btn_supabase_clear_selection.cget("state"), "disabled")

    def test_supabase_mgmt_refresh_failure_clears_stale_cache_and_reentry_does_not_render_old_rows(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-failure-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()
        self.show_supabase_mgmt_with_rows(app, sample_rows)

        with patch.object(uploader_gui_tk.App, "ensure_local_supabase_ready", return_value=True):
            with patch.object(
                uploader_gui_tk,
                "load_supabase_mgmt_rows",
                side_effect=RuntimeError("refresh boom"),
            ):
                with patch.object(uploader_gui_tk.messagebox, "showerror", return_value=None):
                    with patch.object(uploader_gui_tk.threading, "Thread", ImmediateThread):
                        app.on_supabase_mgmt_refresh()
                        pump(app)

        self.assertEqual(app.supabase_mgmt_state, "error")
        self.assertEqual(app.supabase_mgmt_rows, ())
        self.assertEqual(app.supabase_mgmt_row_widgets, ())
        self.assertFalse(app.supabase_mgmt_has_loaded_once)
        self.assertEqual(app.supabase_mgmt_last_loaded_at, "")
        self.assertEqual(app.var_supabase_mgmt_summary.get(), app.tr("supabase_mgmt.summary.empty"))
        self.assertEqual(app.var_supabase_mgmt_selection_summary.get(), app.tr("supabase_mgmt.selection.none"))
        self.assertEqual(app.var_supabase_mgmt_detail.get(), "refresh boom")
        self.assertEqual(app.btn_supabase_refresh.cget("state"), "normal")
        self.assertEqual(app.btn_supabase_select_all.cget("state"), "disabled")
        self.assertEqual(app.btn_supabase_delete_all.cget("state"), "disabled")
        self.assertEqual(
            app.supabase_mgmt_last_refresh_label.cget("text"),
            app.tr("supabase_mgmt.summary.last_refresh.empty"),
        )

        app.current_view = "dashboard"
        app.clear_main()
        refresh_mock = MagicMock()
        with patch.object(uploader_gui_tk.App, "on_supabase_mgmt_refresh", new=refresh_mock):
            app.show_supabase_mgmt()
            pump(app)

        refresh_mock.assert_called_once_with()
        self.assertEqual(app.supabase_mgmt_row_widgets, ())
        self.assertIsNotNone(app.supabase_mgmt_state_label)
        self.assertEqual(app.supabase_mgmt_state_label.cget("text"), app.tr("supabase_mgmt.loading.body"))

    def test_supabase_mgmt_delete_failure_classifies_retryable_concurrency_error(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-delete-failure-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()
        self.show_supabase_mgmt_with_rows(app, sample_rows)

        with patch.object(uploader_gui_tk.App, "ensure_local_supabase_ready", return_value=True):
            with patch.object(
                uploader_gui_tk,
                "execute_supabase_delete",
                side_effect=ValueError(
                    "Supabase delete preview does not match the current transaction snapshot. "
                    "mode=selected_dates"
                ),
            ):
                with patch.object(uploader_gui_tk.messagebox, "askyesno", return_value=True):
                    with patch.object(uploader_gui_tk.messagebox, "showerror", return_value=None) as showerror_mock:
                        with patch.object(uploader_gui_tk.threading, "Thread", ImmediateThread):
                            app.supabase_mgmt_row_widgets[0].selected_var.set(True)
                            app.refresh_supabase_mgmt_selection_summary()
                            app.on_supabase_mgmt_delete_selected()
                            pump(app)

        expected_error = app.tr(
            "supabase_mgmt.error.retryable_delete",
            error=(
                "Supabase delete preview does not match the current transaction snapshot. "
                "mode=selected_dates"
            ),
        )
        self.assertEqual(app.supabase_mgmt_state, "error")
        self.assertEqual(app.var_supabase_mgmt_detail.get(), expected_error)
        self.assertEqual(app.btn_supabase_delete_all.cget("state"), "disabled")
        showerror_mock.assert_called_once_with(app.tr("dialog.error.title"), expected_error)

    def test_supabase_mgmt_clear_upload_hold_button_unblocks_maintenance_state(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-clear-hold-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()[:1]
        uploader_gui_tk.set_upload_maintenance_block(
            "supabase_mgmt",
            app.tr("supabase_mgmt.maintenance.reason"),
        )
        self.addCleanup(uploader_gui_tk.clear_upload_maintenance_block)
        self.show_supabase_mgmt_with_rows(app, sample_rows)

        self.assertTrue(app.supabase_mgmt_hold_active)
        self.assertEqual(
            app.supabase_mgmt_hold_status_label.cget("text"),
            app.tr("supabase_mgmt.hold.status.active"),
        )
        self.assertEqual(
            app.supabase_mgmt_hold_detail_label.cget("text"),
            app.tr("supabase_mgmt.maintenance.reason"),
        )
        self.assertEqual(app.btn_supabase_clear_upload_hold.cget("state"), "normal")

        with patch.object(uploader_gui_tk.messagebox, "askyesno", return_value=True):
            app.on_supabase_mgmt_clear_upload_hold()
            pump(app)

        state_health_snapshot = uploader_gui_tk.load_state_health_snapshot(False)
        self.assertTrue(state_health_snapshot["can_start_upload"])
        self.assertFalse(app.supabase_mgmt_hold_active)
        self.assertEqual(
            app.supabase_mgmt_hold_status_label.cget("text"),
            app.tr("supabase_mgmt.hold.status.inactive"),
        )
        self.assertEqual(
            app.supabase_mgmt_hold_detail_label.cget("text"),
            app.tr("supabase_mgmt.hold.detail.inactive"),
        )
        self.assertEqual(app.btn_supabase_clear_upload_hold.cget("state"), "disabled")

    def test_supabase_mgmt_allow_reupload_clears_local_state_and_restores_preview_candidate(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-reupload-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()[:1]
        plc_dir = appdata_root / "plc"
        plc_dir.mkdir(parents=True, exist_ok=True)
        source_file = plc_dir / "Factory_Integrated_Log_20260421_183823.csv"
        source_file.write_text("Date,Time,Mold1\n2026-04-21,00:00:00,1\n", encoding="utf-8")
        uploader_gui_tk.mark_file_completed(str(plc_dir), source_file.name, str(source_file), None)
        uploader_gui_tk.save_pending_supabase_reupload_dates(("2026-04-21",))
        self.addCleanup(uploader_gui_tk.clear_pending_supabase_reupload_dates)

        self.show_supabase_mgmt_with_rows(app, sample_rows)

        self.assertEqual(
            app.supabase_mgmt_reupload_status_label.cget("text"),
            app.tr("supabase_mgmt.reupload.status.pending"),
        )
        self.assertEqual(app.btn_supabase_allow_reupload_dates.cget("state"), "normal")

        with patch.object(uploader_gui_tk.messagebox, "askyesno", return_value=True):
            app.on_supabase_mgmt_allow_reupload_dates()
            pump(app)

        self.assertEqual(uploader_gui_tk.load_processed(), set())
        self.assertEqual(
            app.supabase_mgmt_reupload_status_label.cget("text"),
            app.tr("supabase_mgmt.reupload.status.inactive"),
        )
        included, excluded = uploader_gui_tk.preview_diagnostics(
            str(plc_dir),
            None,
            uploader_gui_tk.date(2026, 4, 21),
            uploader_gui_tk.date(2026, 4, 21),
            0,
            False,
            False,
            uploader_gui_tk.load_processed(),
            app.tr_map,
        )
        self.assertEqual(len(included), 1)
        self.assertEqual(included[0][1], source_file.name)
        self.assertEqual(excluded, [])

    def test_supabase_mgmt_manual_allow_reupload_clears_old_deleted_date_state(self) -> None:
        appdata_root = Path(tempfile.mkdtemp(prefix="supabase-mgmt-manual-reupload-"))
        self.addCleanup(shutil.rmtree, appdata_root, True)
        app = self.create_app(appdata_root)
        sample_rows = self.build_sample_rows()[:1]
        plc_dir = appdata_root / "plc"
        plc_dir.mkdir(parents=True, exist_ok=True)
        source_file = plc_dir / "Factory_Integrated_Log_20260421_183823.csv"
        source_file.write_text("Date,Time,Mold1\n2026-04-21,00:00:00,1\n", encoding="utf-8")
        uploader_gui_tk.mark_file_completed(str(plc_dir), source_file.name, str(source_file), None)
        uploader_gui_tk.save_pending_supabase_reupload_dates(("2026-04-21",))
        self.addCleanup(uploader_gui_tk.clear_pending_supabase_reupload_dates)

        self.show_supabase_mgmt_with_rows(app, sample_rows)

        with patch.object(uploader_gui_tk.simpledialog, "askstring", return_value="2026-04-21"):
            with patch.object(uploader_gui_tk.messagebox, "askyesno", return_value=True):
                app.on_supabase_mgmt_allow_reupload_dates_manual()
                pump(app)

        self.assertEqual(uploader_gui_tk.load_processed(), set())
        self.assertIsNone(uploader_gui_tk.load_pending_supabase_reupload_dates())
        included, excluded = uploader_gui_tk.preview_diagnostics(
            str(plc_dir),
            None,
            uploader_gui_tk.date(2026, 4, 21),
            uploader_gui_tk.date(2026, 4, 21),
            0,
            False,
            False,
            uploader_gui_tk.load_processed(),
            app.tr_map,
        )
        self.assertEqual(len(included), 1)
        self.assertEqual(included[0][1], source_file.name)
        self.assertEqual(excluded, [])
