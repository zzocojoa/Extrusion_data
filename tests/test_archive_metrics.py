from datetime import datetime, timezone
from pathlib import Path
import shutil
import tempfile
from unittest import TestCase
from unittest.mock import patch

from core import archive_metrics


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
        self.connection.executed.append((query, params))

    def fetchone(self) -> tuple[object, ...] | None:
        if len(self.connection.results) == 0:
            raise AssertionError("fetchone result is missing")
        return self.connection.results.pop(0)


class FakeConnection:
    def __init__(self, results: list[tuple[object, ...] | None]) -> None:
        self.results = list(results)
        self.executed: list[tuple[str, tuple[object, ...]]] = []
        self.commit_called = False
        self.rollback_called = False
        self.closed = False
        self.session_calls: list[dict[str, object]] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def commit(self) -> None:
        self.commit_called = True

    def rollback(self) -> None:
        self.rollback_called = True

    def close(self) -> None:
        self.closed = True

    def set_session(self, **kwargs: object) -> None:
        self.session_calls.append(kwargs)


def _normalize_query_text(query: str) -> str:
    return " ".join(query.split())


class ArchiveMetricsDeleteTests(TestCase):
    def build_stats_row(
        self,
        row_count: int,
        min_timestamp: str,
        max_timestamp: str,
    ) -> tuple[object, ...]:
        return (
            row_count,
            min_timestamp,
            max_timestamp,
        )

    def assert_query_contains(self, query: str, expected_fragment: str) -> None:
        self.assertIn(
            _normalize_query_text(expected_fragment),
            _normalize_query_text(query),
        )

    def test_query_all_metrics_archive_stats_with_delete_lock_executes_lock_first(self) -> None:
        before_datetime = datetime(2026, 4, 23, tzinfo=timezone.utc)
        fake_connection = FakeConnection(
            [
                self.build_stats_row(
                    row_count=2,
                    min_timestamp="2026-04-22T00:00:00+00:00",
                    max_timestamp="2026-04-22T01:00:00+00:00",
                )
            ]
        )

        result = archive_metrics.query_all_metrics_archive_stats_with_delete_lock(
            fake_connection,
            before_datetime,
        )

        self.assertEqual(result.row_count, 2)
        self.assertEqual(result.min_timestamp, "2026-04-22T00:00:00+00:00")
        self.assertEqual(result.max_timestamp, "2026-04-22T01:00:00+00:00")
        self.assertEqual(len(fake_connection.executed), 2)
        self.assertEqual(
            _normalize_query_text(fake_connection.executed[0][0]),
            "LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE",
        )
        self.assertQueryContains = self.assert_query_contains  # type: ignore[attr-defined]
        self.assert_query_contains(
            fake_connection.executed[1][0],
            'FROM public.all_metrics WHERE "timestamp" < %s',
        )
        self.assertEqual(fake_connection.executed[1][1], (before_datetime,))

    def test_delete_archived_all_metrics_commits_with_lock_and_empty_recheck(self) -> None:
        db_settings = archive_metrics.DbConnectionSettings(
            host="127.0.0.1",
            port=25432,
            user="postgres",
            password="postgres",
            dbname="postgres",
        )
        before_datetime = datetime(2026, 4, 23, tzinfo=timezone.utc)
        expected_stats = archive_metrics.ArchiveStats(
            row_count=2,
            min_timestamp="2026-04-22T00:00:00+00:00",
            max_timestamp="2026-04-22T01:00:00+00:00",
        )
        fake_connection = FakeConnection(
            [
                self.build_stats_row(
                    row_count=2,
                    min_timestamp="2026-04-22T00:00:00+00:00",
                    max_timestamp="2026-04-22T01:00:00+00:00",
                ),
                self.build_stats_row(
                    row_count=2,
                    min_timestamp="2026-04-22T00:00:00+00:00",
                    max_timestamp="2026-04-22T01:00:00+00:00",
                ),
                self.build_stats_row(
                    row_count=0,
                    min_timestamp="",
                    max_timestamp="",
                ),
            ]
        )

        original_create_connection = archive_metrics.create_connection
        archive_metrics.create_connection = lambda _db_settings: fake_connection
        try:
            result = archive_metrics.delete_archived_all_metrics(
                db_settings,
                before_datetime,
                expected_stats,
            )
        finally:
            archive_metrics.create_connection = original_create_connection

        self.assertEqual(result, expected_stats)
        self.assertTrue(fake_connection.commit_called)
        self.assertFalse(fake_connection.rollback_called)
        self.assertTrue(fake_connection.closed)
        self.assertEqual(
            fake_connection.session_calls,
            [{"isolation_level": "SERIALIZABLE", "autocommit": False}],
        )
        self.assertEqual(
            _normalize_query_text(fake_connection.executed[0][0]),
            "LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE",
        )
        self.assert_query_contains(
            fake_connection.executed[2][0],
            'DELETE FROM public.all_metrics WHERE "timestamp" < %s',
        )
        self.assertEqual(fake_connection.executed[2][1], (before_datetime,))
        self.assertEqual(fake_connection.executed[3][1], (before_datetime,))

    def test_delete_archived_all_metrics_rolls_back_when_rows_remain(self) -> None:
        db_settings = archive_metrics.DbConnectionSettings(
            host="127.0.0.1",
            port=25432,
            user="postgres",
            password="postgres",
            dbname="postgres",
        )
        before_datetime = datetime(2026, 4, 23, tzinfo=timezone.utc)
        expected_stats = archive_metrics.ArchiveStats(
            row_count=2,
            min_timestamp="2026-04-22T00:00:00+00:00",
            max_timestamp="2026-04-22T01:00:00+00:00",
        )
        fake_connection = FakeConnection(
            [
                self.build_stats_row(
                    row_count=2,
                    min_timestamp="2026-04-22T00:00:00+00:00",
                    max_timestamp="2026-04-22T01:00:00+00:00",
                ),
                self.build_stats_row(
                    row_count=2,
                    min_timestamp="2026-04-22T00:00:00+00:00",
                    max_timestamp="2026-04-22T01:00:00+00:00",
                ),
                self.build_stats_row(
                    row_count=1,
                    min_timestamp="2026-04-22T02:00:00+00:00",
                    max_timestamp="2026-04-22T02:00:00+00:00",
                ),
            ]
        )

        original_create_connection = archive_metrics.create_connection
        archive_metrics.create_connection = lambda _db_settings: fake_connection
        try:
            with self.assertRaisesRegex(ValueError, "cutoff 이전 row가 남아 있습니다"):
                archive_metrics.delete_archived_all_metrics(
                    db_settings,
                    before_datetime,
                    expected_stats,
                )
        finally:
            archive_metrics.create_connection = original_create_connection

        self.assertFalse(fake_connection.commit_called)
        self.assertTrue(fake_connection.rollback_called)
        self.assertTrue(fake_connection.closed)

    def test_read_local_db_port_uses_executable_parent_when_frozen(self) -> None:
        workspace = Path(tempfile.mkdtemp(prefix="archive-metrics-frozen-"))
        self.addCleanup(shutil.rmtree, workspace, True)
        repo_root = workspace / "repo"
        dist_dir = repo_root / "dist"
        supabase_dir = repo_root / "supabase"
        meipass_dir = workspace / "_MEI123456"
        dist_dir.mkdir(parents=True, exist_ok=True)
        supabase_dir.mkdir(parents=True, exist_ok=True)
        meipass_dir.mkdir(parents=True, exist_ok=True)
        executable_path = dist_dir / "ExtrusionUploader.exe"
        executable_path.write_text("", encoding="utf-8")
        (supabase_dir / "config.toml").write_text("[db]\nport = 25432\n", encoding="utf-8")

        with patch.object(archive_metrics.sys, "frozen", True, create=True):
            with patch.object(archive_metrics.sys, "executable", str(executable_path)):
                port = archive_metrics.read_local_db_port(meipass_dir)

        self.assertEqual(port, 25432)
