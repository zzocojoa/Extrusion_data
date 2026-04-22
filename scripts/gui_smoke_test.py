import json
import os
import sys
import traceback
from typing import Any
from unittest.mock import patch

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import uploader_gui_tk
from uploader_gui_tk import App


def pump(app: App) -> None:
    app.update_idletasks()
    app.update()


def build_result(name: str, ok: bool, detail: str) -> dict[str, Any]:
    return {
        "name": name,
        "ok": ok,
        "detail": detail,
    }


def read_grid_detail(widget: Any) -> str:
    return str(widget.grid_info())


def build_grid_result(
    name: str,
    widget: Any,
    expected_row: int,
    expected_column: int,
    expected_columnspan: int,
) -> dict[str, Any]:
    grid_info = widget.grid_info()
    return build_result(
        name,
        grid_info.get("row") == expected_row
        and grid_info.get("column") == expected_column
        and grid_info.get("columnspan", 1) == expected_columnspan,
        read_grid_detail(widget),
    )


def build_manager_result(
    name: str,
    widgets: list[Any],
    expected_manager: str,
) -> dict[str, Any]:
    managers = [str(widget.winfo_manager()) for widget in widgets]
    return build_result(
        name,
        all(manager == expected_manager for manager in managers),
        str(managers),
    )


def run_gui_smoke() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    app: App | None = None
    try:
        with patch.object(uploader_gui_tk, "can_connect_tcp", return_value=False):
            with patch.object(uploader_gui_tk.App, "request_wsl_storage_refresh", new=lambda self: None):
                app = App()
                pump(app)
                results.append(build_result("launch", True, app.title()))
                for language in ["ko", "en"]:
                    app.cfg["UI_LANGUAGE"] = language
                    app.reload_translations()
                    pump(app)
                    app.geometry("1160x760")
                    pump(app)

                    app.show_dashboard()
                    pump(app)
                    app.refresh_dashboard_layout()
                    pump(app)
                    dashboard_tasks = app.tasks_frame.grid_info()
                    results.append(
                        build_result(
                            f"{language}:dashboard",
                            dashboard_tasks.get("row") == 1 and dashboard_tasks.get("column") == 0,
                            str(dashboard_tasks),
                        )
                    )

                    app.show_settings()
                    pump(app)
                    app.var_range.set(app.get_range_mode_label("custom"))
                    app.refresh_settings_form_state()
                    pump(app)
                    settings_start = app.custom_start_entry.grid_info()
                    settings_end = app.custom_end_entry.grid_info()
                    results.append(
                        build_result(
                            f"{language}:settings",
                            settings_start.get("row") == 2 and settings_end.get("row") == 4,
                            f"start={settings_start}, end={settings_end}",
                        )
                    )

                    app.show_cycle_ops()
                    pump(app)
                    app.var_legacy_cycle_range.set(app.get_legacy_cycle_mode_label("custom"))
                    app.on_legacy_cycle_range_change(app.var_legacy_cycle_range.get())
                    pump(app)
                    cycle_menu = app.legacy_cycle_range_menu.grid_info()
                    results.append(
                        build_result(
                            f"{language}:cycle_ops",
                            cycle_menu.get("row") == 1,
                            str(cycle_menu),
                        )
                    )
                    results.append(
                        build_manager_result(
                            f"{language}:cycle_ops_button_managers",
                            [
                                app.btn_run_canonical_refresh,
                                app.btn_run_cycle_snapshot,
                                app.btn_run_cycle_health,
                            ],
                            "grid",
                        )
                    )
                    app.geometry("820x760")
                    pump(app)
                    results.append(
                        build_grid_result(
                            f"{language}:cycle_ops_buttons_medium",
                            app.btn_run_cycle_health,
                            1,
                            0,
                            2,
                        )
                    )

                    app.show_data_mgmt()
                    pump(app)
                    app.refresh_responsive_layout()
                    pump(app)
                    training_menu = app.training_mode_menu.grid_info()
                    archive_entry = app.archive_date_entry.grid_info()
                    results.append(
                        build_result(
                            f"{language}:data_mgmt_training",
                            training_menu.get("row") == 1,
                            str(training_menu),
                        )
                    )
                    results.append(
                        build_result(
                            f"{language}:data_mgmt_archive",
                            archive_entry.get("row") == 1,
                            str(archive_entry),
                        )
                    )
                    results.append(
                        build_manager_result(
                            f"{language}:data_mgmt_archive_button_managers",
                            [
                                app.btn_archive_preview,
                                app.btn_archive_export,
                            ],
                            "grid",
                        )
                    )
                    app.geometry("1160x760")
                    pump(app)
                    app.refresh_responsive_layout()
                    pump(app)
                    results.append(
                        build_grid_result(
                            f"{language}:data_mgmt_archive_buttons_wide",
                            app.btn_archive_export,
                            0,
                            1,
                            1,
                        )
                    )

                    app.show_dashboard()
                    pump(app)
                    results.append(
                        build_manager_result(
                            f"{language}:dashboard_button_managers",
                            [
                                app.btn_retry_failed,
                                app.btn_preview,
                                app.btn_pause,
                                app.btn_start,
                            ],
                            "grid",
                        )
                    )
                    app.geometry("1160x760")
                    pump(app)
                    app.refresh_dashboard_layout()
                    pump(app)
                    results.append(
                        build_grid_result(
                            f"{language}:dashboard_buttons_wide",
                            app.btn_start,
                            0,
                            3,
                            1,
                        )
                    )
                    app.show_data_mgmt()
                    pump(app)
                    app.show_dashboard()
                    pump(app)
                    with app.progress_lock:
                        app.active_progress["smoke-task"] = (42, 0)
                    app.update_dashboard_loop(app.dashboard_view_generation)
                    pump(app)
                    has_smoke_task = "smoke-task" in app.task_labels and app.task_labels["smoke-task"].winfo_exists()
                    results.append(
                        build_result(
                            f"{language}:sidebar_dashboard_loop",
                            bool(app.current_view == "dashboard" and has_smoke_task),
                            f"view={app.current_view}, tasks={list(app.task_labels.keys())}",
                        )
                    )
                    with app.progress_lock:
                        app.active_progress.clear()
    except Exception as error:
        results.append(
            build_result(
                "exception",
                False,
                f"{error.__class__.__name__}: {error}\n{traceback.format_exc()}",
            )
        )
    finally:
        if app is not None:
            try:
                app.close_application()
            except Exception:
                pass
    return results


def main() -> int:
    results = run_gui_smoke()
    print(json.dumps(results, ensure_ascii=False, indent=2))
    failed = [result for result in results if not bool(result["ok"])]
    if failed:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
