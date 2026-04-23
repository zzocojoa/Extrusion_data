import json
import os
import shutil
import sqlite3
import tempfile
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from core import files as core_files
from core import state as core_state
from core import state_db


class SQLiteStatePhase1Tests(TestCase):
    def create_workspace(self) -> Path:
        workspace = Path(tempfile.mkdtemp(prefix="state-sqlite-phase1-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        return workspace

    def create_appdata(self, workspace: Path) -> Path:
        appdata_root = workspace / "appdata"
        appdata_root.mkdir(parents=True, exist_ok=True)
        return appdata_root

    def write_json(self, path: Path, value: object) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def write_manifest(
        self,
        manifest_path: Path,
        processed: list[str],
        resume: dict[str, int],
        recent_profile: dict[str, object] | None,
        failed_state: dict[str, object] | None,
    ) -> None:
        manifest: dict[str, object] = {
            "version": core_state.MANIFEST_VERSION,
            "processed": processed,
            "resume": resume,
        }
        if recent_profile is not None:
            manifest["recent_successful_upload_profile"] = recent_profile
        if failed_state is not None:
            manifest["last_failed_retry_state"] = failed_state
        self.write_json(manifest_path, manifest)

    def create_state_paths(self, appdata_root: Path) -> tuple[Path, Path, Path, Path, Path]:
        data_dir = appdata_root / "ExtrusionUploader"
        data_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = data_dir / core_state.MANIFEST_FILENAME
        log_path = data_dir / core_state.LOG_FILENAME
        resume_path = data_dir / core_state.RESUME_FILENAME
        db_path = data_dir / state_db.DB_FILENAME
        backup_root = data_dir / state_db.MIGRATION_BACKUP_DIRNAME
        return manifest_path, log_path, resume_path, db_path, backup_root

    def create_csv_file(self, workspace: Path, relative_path: str) -> Path:
        target_path = workspace / relative_path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text("timestamp,value\n2026-04-21T00:00:00+09:00,1\n", encoding="utf-8")
        return target_path

    def test_bootstrap_sets_pragmas_and_creates_backup_snapshot(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        processed_key = "folder/sample.csv|size=10|mtime_ns=20"
        self.write_manifest(
            manifest_path,
            [processed_key],
            {},
            {"profile_name": "last-good", "applied_at": 1.0, "values": {"RANGE_MODE": "today"}},
            None,
        )
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        result = state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        self.assertTrue(result["imported"])
        self.assertTrue(db_path.exists())
        self.assertIsNotNone(result["backup_dir"])
        self.assertTrue((Path(result["backup_dir"]) / "source_manifest.json").exists())
        self.assertEqual(state_db.get_user_version(str(db_path)), state_db.DB_SCHEMA_VERSION)

        connection = state_db.connect_state_db(str(db_path))
        try:
            journal_mode = str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower()
            synchronous = int(connection.execute("PRAGMA synchronous").fetchone()[0])
            busy_timeout = int(connection.execute("PRAGMA busy_timeout").fetchone()[0])
        finally:
            connection.close()

        self.assertEqual(journal_mode, "wal")
        self.assertEqual(synchronous, 2)
        self.assertEqual(busy_timeout, 10000)

    def test_importer_is_idempotent_for_same_source_snapshot(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        processed_key = "folder/sample.csv|size=11|mtime_ns=21"
        self.write_manifest(manifest_path, [processed_key], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        first_result = state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )
        second_result = state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        self.assertTrue(first_result["imported"])
        self.assertFalse(second_result["imported"])
        self.assertEqual(first_result["source_fingerprint"], second_result["source_fingerprint"])

    def test_importer_accepts_manifest_when_legacy_artifacts_are_empty(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        processed_key = "folder/from-manifest.csv|size=12|mtime_ns=22"
        self.write_manifest(manifest_path, [processed_key], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )
        snapshot = state_db.load_sqlite_snapshot(str(db_path))

        self.assertEqual(snapshot["processed_keys"], [processed_key])
        self.assertEqual(snapshot["resume"], {})

    def test_importer_rejects_manifest_and_legacy_conflict(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        manifest_key = "folder/from-manifest.csv|size=15|mtime_ns=25"
        self.write_manifest(manifest_path, [manifest_key], {}, None, None)
        log_path.write_text("folder/from-log.csv|size=16|mtime_ns=26\n", encoding="utf-8")
        self.write_json(resume_path, {})

        with self.assertRaises(state_db.StateDbImportError):
            state_db.ensure_sqlite_snapshot_from_legacy(
                str(manifest_path),
                str(log_path),
                str(resume_path),
                str(db_path),
                str(backup_root),
            )

    def test_parity_check_is_strict_for_resume_mismatch(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        resume_key = "folder/sample.csv|size=13|mtime_ns=23"
        self.write_manifest(manifest_path, [], {resume_key: 7}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )
        expected_bundle = state_db.build_legacy_snapshot(str(manifest_path), str(log_path), str(resume_path))

        connection = sqlite3.connect(str(db_path))
        try:
            connection.execute("UPDATE file_state SET resume_offset = 8 WHERE file_key = ?", (resume_key,))
            connection.commit()
        finally:
            connection.close()

        with self.assertRaises(state_db.StateDbParityError):
            state_db.parity_check(expected_bundle["parity_snapshot"], state_db.load_sqlite_snapshot(str(db_path)))

    def test_parity_check_is_strict_for_source_fingerprint_mismatch(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        resume_key = "folder/sample.csv|size=17|mtime_ns=27"
        self.write_manifest(manifest_path, [], {resume_key: 9}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )
        expected_bundle = state_db.build_legacy_snapshot(str(manifest_path), str(log_path), str(resume_path))
        actual_snapshot = state_db.load_sqlite_snapshot(str(db_path))
        actual_snapshot["source_fingerprint"] = "tampered"

        with self.assertRaises(state_db.StateDbParityError):
            state_db.parity_check(expected_bundle["parity_snapshot"], actual_snapshot)

    def test_importer_rejects_ambiguous_basename_alias(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        log_path.write_text("dir-a/sample.csv\ndir-b/sample.csv\n", encoding="utf-8")
        self.write_json(resume_path, {"sample.csv": 5})

        with self.assertRaises(state_db.StateDbImportError):
            state_db.ensure_sqlite_snapshot_from_legacy(
                str(manifest_path),
                str(log_path),
                str(resume_path),
                str(db_path),
                str(backup_root),
            )

    def test_importer_rejects_processed_and_failed_conflict(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        conflicting_key = "folder/sample.csv|size=20|mtime_ns=30"
        self.write_manifest(
            manifest_path,
            [conflicting_key],
            {},
            None,
            {
                "file_key": conflicting_key,
                "offset": 4,
                "retry_count": 1,
                "failed_at": 7.0,
                "error_message": "boom",
            },
        )
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        with self.assertRaises(state_db.StateDbImportError):
            state_db.ensure_sqlite_snapshot_from_legacy(
                str(manifest_path),
                str(log_path),
                str(resume_path),
                str(db_path),
                str(backup_root),
            )

    def test_importer_rejects_missing_legacy_source_when_db_exists(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        processed_key = "folder/sample.csv|size=18|mtime_ns=28"
        self.write_manifest(manifest_path, [processed_key], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        manifest_path.unlink()
        log_path.unlink()
        resume_path.unlink()

        with self.assertRaises(state_db.StateDbImportError):
            state_db.ensure_sqlite_snapshot_from_legacy(
                str(manifest_path),
                str(log_path),
                str(resume_path),
                str(db_path),
                str(backup_root),
            )

    def test_facade_sqlite_read_mode_fails_closed_on_corrupt_db(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, _ = self.create_state_paths(appdata_root)
        self.write_manifest(manifest_path, ["folder/sample.csv"], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})
        db_path.write_text("not-a-sqlite-db", encoding="utf-8")

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "sqlite",
            },
            clear=False,
        ):
            with self.assertRaises(core_state.StateDbCorruptionError):
                core_state.load_processed(None)

    def test_load_sqlite_snapshot_fails_closed_on_invalid_metadata_json(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        self.write_manifest(manifest_path, ["folder/sample.csv|size=21|mtime_ns=31"], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        connection = state_db.connect_state_db(str(db_path))
        try:
            connection.execute(
                "UPDATE state_meta SET value_json = ? WHERE key = 'source_fingerprint'",
                ("not-json",),
            )
            connection.commit()
        finally:
            connection.close()

        with self.assertRaises(state_db.StateDbCorruptionError):
            state_db.load_sqlite_snapshot(str(db_path))

    def test_facade_sqlite_mode_blocks_when_db_is_missing_and_legacy_artifacts_exist(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, _ = self.create_state_paths(appdata_root)
        processed_key = "folder/sample.csv|size=19|mtime_ns=29"
        self.write_manifest(manifest_path, [processed_key], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "sqlite",
            },
            clear=False,
        ):
            health_snapshot = core_state.load_state_health(None, verify_integrity=False)
            with self.assertRaises(core_state.StateDbImportError):
                core_state.load_processed(None)

        self.assertEqual(health_snapshot["state"], "blocked")
        self.assertFalse(health_snapshot["can_start_upload"])
        self.assertEqual(health_snapshot["summary_code"], "unavailable")
        self.assertIn("migrate_legacy_state()", health_snapshot["error_message"])
        self.assertFalse(db_path.exists())

    def test_migrate_legacy_state_imports_legacy_when_requested_explicitly(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, _ = self.create_state_paths(appdata_root)
        processed_key = "folder/sample.csv|size=19|mtime_ns=29"
        self.write_manifest(manifest_path, [processed_key], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "sqlite",
            },
            clear=False,
        ):
            core_state.migrate_legacy_state(None)
            processed = core_state.load_processed(None)

        self.assertEqual(processed, {processed_key, "folder/sample.csv"})
        self.assertTrue(db_path.exists())

    def test_facade_sqlite_read_mode_returns_snapshot_when_enabled(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        processed_key = "folder/sample.csv|size=14|mtime_ns=24"
        failed_state = {
            "file_key": processed_key,
            "offset": 3,
            "retry_count": 2,
            "failed_at": 5.0,
            "error_message": "boom",
        }
        self.write_manifest(
            manifest_path,
            [],
            {processed_key: 3},
            {"profile_name": "last-good", "applied_at": 2.0, "values": {"PLC_DIR": "PLC_data"}},
            failed_state,
        )
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})
        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "sqlite",
            },
            clear=False,
        ):
            processed = core_state.load_processed(None)
            resume = core_state.load_resume(None)
            dashboard_snapshot = core_state.load_upload_dashboard_state(None)

        self.assertEqual(processed, set())
        self.assertEqual(resume, {processed_key: 3})
        self.assertEqual(dashboard_snapshot["resume"], {processed_key: 3})
        self.assertEqual(len(dashboard_snapshot["failed_retry_set"]), 1)
        self.assertEqual(dashboard_snapshot["failed_retry_set"][0]["file_key"], processed_key)

    def test_mark_file_completed_clears_resume_and_failed_retry_set(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        source_file = self.create_csv_file(workspace, "plc/260421_sample.csv")
        folder = str(source_file.parent)
        filename = source_file.name
        self.write_manifest(manifest_path, [], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})
        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        state_db.set_resume_offset(str(db_path), core_state.build_file_state_key(folder, filename, str(source_file)), 6)
        state_db.record_file_failure(str(db_path), folder, filename, str(source_file), 6, "boom", 7)
        state_db.mark_file_completed(str(db_path), folder, filename, str(source_file), 7)

        snapshot = state_db.load_sqlite_snapshot(str(db_path))
        completed_key = core_state.build_file_state_key(folder, filename, str(source_file))
        self.assertEqual(snapshot["processed_keys"], [completed_key])
        self.assertEqual(snapshot["resume"], {})
        self.assertEqual(snapshot["failed_retry_set"], ())

    def test_clear_local_upload_state_by_legacy_keys_removes_processed_resume_and_failed_rows(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        _, _, _, db_path, _ = self.create_state_paths(appdata_root)
        source_file = self.create_csv_file(workspace, "plc/Factory_Integrated_Log_20260421_183823.csv")
        state_db.ensure_bootstrap_database(str(db_path))

        folder = str(source_file.parent)
        filename = source_file.name
        file_key = core_state.build_file_state_key(folder, filename, str(source_file))
        legacy_key = core_state.build_legacy_file_key(folder, filename)

        state_db.mark_file_completed(str(db_path), folder, filename, str(source_file), None)
        state_db.set_resume_offset(str(db_path), file_key, 7)
        state_db.record_file_failure(str(db_path), folder, filename, str(source_file), 7, "boom", None)

        cleared_count = core_state.clear_local_upload_state_by_legacy_keys((legacy_key,), str(db_path))

        self.assertEqual(cleared_count, 1)
        snapshot = state_db.load_sqlite_snapshot(str(db_path))
        self.assertEqual(snapshot["processed_keys"], [])
        self.assertEqual(snapshot["processed_lookup_keys"], [])
        self.assertEqual(snapshot["resume"], {})
        self.assertEqual(snapshot["resume_lookup"], {})
        self.assertEqual(snapshot["failed_retry_set"], ())

    def test_clear_local_upload_state_by_legacy_keys_removes_all_versions_for_same_legacy_key(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        _, _, _, db_path, _ = self.create_state_paths(appdata_root)
        source_file = self.create_csv_file(workspace, "plc/Factory_Integrated_Log_20260421_183823.csv")
        state_db.ensure_bootstrap_database(str(db_path))

        folder = str(source_file.parent)
        filename = source_file.name
        legacy_key = core_state.build_legacy_file_key(folder, filename)
        first_file_key = core_state.build_file_state_key(folder, filename, str(source_file))
        state_db.mark_file_completed(str(db_path), folder, filename, str(source_file), None)

        source_file.write_text(
            "timestamp,value\n2026-04-21T00:00:00+09:00,1\n2026-04-21T00:01:00+09:00,2\n",
            encoding="utf-8",
        )
        second_file_key = core_state.build_file_state_key(folder, filename, str(source_file))
        state_db.mark_file_completed(str(db_path), folder, filename, str(source_file), None)

        self.assertNotEqual(first_file_key, second_file_key)
        cleared_count = core_state.clear_local_upload_state_by_legacy_keys((legacy_key,), str(db_path))

        self.assertEqual(cleared_count, 2)
        remaining_rows = state_db.load_file_state_rows(str(db_path))
        self.assertEqual(remaining_rows, ())

    def test_completed_file_does_not_block_same_filename_in_other_folder(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        _, _, _, db_path, _ = self.create_state_paths(appdata_root)
        first_file = self.create_csv_file(workspace, "plc-a/sample.csv")
        second_file = self.create_csv_file(workspace, "plc-b/sample.csv")
        state_db.ensure_bootstrap_database(str(db_path))
        state_db.mark_file_completed(
            str(db_path),
            str(first_file.parent),
            first_file.name,
            str(first_file),
            None,
        )

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "sqlite",
            },
            clear=False,
        ):
            processed = core_state.load_processed(None)

        self.assertEqual(
            processed,
            {
                core_state.build_file_state_key(str(first_file.parent), first_file.name, str(first_file)),
                core_state.build_legacy_file_key(str(first_file.parent), first_file.name),
            },
        )
        self.assertFalse(
            core_files._is_processed_file(
                processed,
                str(second_file.parent),
                second_file.name,
                str(second_file),
            )
        )

    def test_record_file_failure_preserves_multiple_failed_retry_entries(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        first_file = self.create_csv_file(workspace, "plc-a/sample.csv")
        second_file = self.create_csv_file(workspace, "plc-b/sample.csv")
        self.write_manifest(manifest_path, [], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})
        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        state_db.record_file_failure(str(db_path), str(first_file.parent), first_file.name, str(first_file), 3, "first", 10)
        state_db.record_file_failure(str(db_path), str(second_file.parent), second_file.name, str(second_file), 5, "second", 10)

        failed_retry_set = state_db.load_failed_retry_set(str(db_path))
        self.assertEqual(len(failed_retry_set), 2)
        self.assertEqual({entry["filename"] for entry in failed_retry_set}, {first_file.name, second_file.name})
        self.assertEqual(
            {entry["legacy_key"] for entry in failed_retry_set},
            {
                core_state.build_legacy_file_key(str(first_file.parent), first_file.name),
                core_state.build_legacy_file_key(str(second_file.parent), second_file.name),
            },
        )

    def test_finish_upload_run_persists_recent_profile_only_on_success(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        self.write_manifest(manifest_path, [], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})
        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )

        failed_run_id = state_db.start_upload_run(str(db_path), 2, False, {"PLC_DIR": "A"})
        state_db.finish_upload_run(
            str(db_path),
            failed_run_id,
            2,
            1,
            1,
            (),
            {"profile_name": "should-not-save", "applied_at": 1.0, "values": {"PLC_DIR": "A"}},
        )
        self.assertIsNone(state_db.load_recent_successful_upload_profile(str(db_path)))

        success_run_id = state_db.start_upload_run(str(db_path), 1, False, {"PLC_DIR": "B"})
        state_db.finish_upload_run(
            str(db_path),
            success_run_id,
            1,
            1,
            0,
            (),
            {"profile_name": "saved", "applied_at": 2.0, "values": {"PLC_DIR": "B"}},
        )
        profile = state_db.load_recent_successful_upload_profile(str(db_path))
        self.assertIsNotNone(profile)
        self.assertEqual(profile["profile_name"], "saved")

    def test_load_state_health_reports_recovery_available(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, db_path, backup_root = self.create_state_paths(appdata_root)
        source_file = self.create_csv_file(workspace, "plc/260421_sample.csv")
        self.write_manifest(manifest_path, [], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})
        state_db.ensure_sqlite_snapshot_from_legacy(
            str(manifest_path),
            str(log_path),
            str(resume_path),
            str(db_path),
            str(backup_root),
        )
        state_db.record_file_failure(
            str(db_path),
            str(source_file.parent),
            source_file.name,
            str(source_file),
            9,
            "boom",
            3,
        )

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "sqlite",
            },
            clear=False,
        ):
            health_snapshot = core_state.load_state_health(None, verify_integrity=True)

        self.assertEqual(health_snapshot["state"], "attention")
        self.assertTrue(health_snapshot["can_start_upload"])
        self.assertEqual(health_snapshot["failed_retry_count"], 1)
        self.assertEqual(health_snapshot["summary_code"], "recovery_available")

    def test_load_state_health_blocks_explicit_legacy_mode(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "legacy",
            },
            clear=False,
        ):
            health_snapshot = core_state.load_state_health(None, verify_integrity=True)

        self.assertEqual(health_snapshot["state"], "blocked")
        self.assertFalse(health_snapshot["can_start_upload"])
        self.assertEqual(health_snapshot["summary_code"], "legacy_mode")

    def test_load_state_health_blocks_corrupted_database(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        _, _, _, db_path, _ = self.create_state_paths(appdata_root)
        db_path.write_text("not-a-db", encoding="utf-8")

        with patch.dict(
            os.environ,
            {
                "APPDATA": str(appdata_root),
                core_state.SQLITE_READ_MODE_ENV_NAME: "sqlite",
            },
            clear=False,
        ):
            health_snapshot = core_state.load_state_health(None, verify_integrity=True)

        self.assertEqual(health_snapshot["state"], "blocked")
        self.assertFalse(health_snapshot["can_start_upload"])
        self.assertEqual(health_snapshot["summary_code"], "corruption")
        self.assertIn("Corrupted SQLite state database", health_snapshot["error_message"])

    def test_load_state_health_blocks_when_upload_maintenance_hold_is_active(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        _, _, _, db_path, _ = self.create_state_paths(appdata_root)
        state_db.ensure_bootstrap_database(str(db_path))
        state_db.set_upload_maintenance_block(
            str(db_path),
            "supabase_mgmt",
            "maintenance-hold",
        )

        with patch.dict(
            os.environ,
            {"APPDATA": str(appdata_root)},
            clear=False,
        ):
            health_snapshot = core_state.load_state_health(None, verify_integrity=False)

        self.assertEqual(health_snapshot["state"], "blocked")
        self.assertFalse(health_snapshot["can_start_upload"])
        self.assertEqual(health_snapshot["summary_code"], "maintenance_block")
        self.assertEqual(health_snapshot["detail_codes"], ("maintenance_block",))
        self.assertEqual(health_snapshot["error_message"], "maintenance-hold")
        self.assertEqual(health_snapshot["maintenance_source"], "supabase_mgmt")

    def test_migrate_legacy_state_rejects_conflicting_sources(self) -> None:
        workspace = self.create_workspace()
        appdata_root = self.create_appdata(workspace)
        manifest_path, log_path, resume_path, _, _ = self.create_state_paths(appdata_root)
        script_dir = workspace / "legacy-script"
        script_dir.mkdir(parents=True, exist_ok=True)
        script_manifest_path = script_dir / core_state.MANIFEST_FILENAME
        script_log_path = script_dir / core_state.LOG_FILENAME
        script_resume_path = script_dir / core_state.RESUME_FILENAME

        self.write_manifest(manifest_path, ["app/sample.csv|size=1|mtime_ns=1"], {}, None, None)
        log_path.write_text("", encoding="utf-8")
        self.write_json(resume_path, {})
        self.write_manifest(script_manifest_path, ["script/sample.csv|size=2|mtime_ns=2"], {}, None, None)
        script_log_path.write_text("", encoding="utf-8")
        self.write_json(script_resume_path, {})

        with patch.dict(
            os.environ,
            {"APPDATA": str(appdata_root)},
            clear=False,
        ):
            with self.assertRaises(core_state.StateDbImportError):
                core_state.migrate_legacy_state(str(script_dir))
