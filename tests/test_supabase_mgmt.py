from datetime import date
from pathlib import Path
from unittest import TestCase
from unittest.mock import patch

from core import supabase_mgmt


class FakeCursor:
    def __init__(self, connection: "FakeConnection") -> None:
        self.connection = connection

    def __enter__(self) -> "FakeCursor":
        return self

    def __exit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False

    def execute(self, query: str, params: tuple[object, ...] = ()) -> None:
        self.connection.executed.append((query, params))

    def fetchall(self) -> list[tuple[object, ...]]:
        if len(self.connection.results) == 0:
            raise AssertionError("fetchall result is missing")
        return self.connection.results.pop(0)

    def fetchone(self) -> tuple[object, ...] | None:
        rows = self.fetchall()
        if len(rows) == 0:
            return None
        if len(rows) != 1:
            raise AssertionError("fetchone result must contain exactly one row")
        return rows[0]


class FakeConnection:
    def __init__(
        self,
        results: list[list[tuple[object, ...]]],
    ) -> None:
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


class SupabaseMgmtCoreTests(TestCase):
    def build_metric_date_row(
        self,
        kst_date: date,
        row_count: int,
        min_timestamp: str,
        max_timestamp: str,
    ) -> supabase_mgmt.SupabaseMetricDateRow:
        return supabase_mgmt.SupabaseMetricDateRow(
            kst_date=kst_date,
            row_count=row_count,
            min_timestamp=min_timestamp,
            max_timestamp=max_timestamp,
        )

    def build_grouped_query_row(
        self,
        kst_date: date,
        row_count: int,
        min_timestamp: str,
        max_timestamp: str,
    ) -> tuple[object, object, object, object]:
        return (
            kst_date,
            row_count,
            min_timestamp,
            max_timestamp,
        )

    def build_delete_preview(
        self,
        selection_mode: supabase_mgmt.SupabaseDeleteMode,
        dates: tuple[date, ...],
        date_rows: tuple[supabase_mgmt.SupabaseMetricDateRow, ...],
    ) -> supabase_mgmt.SupabaseDeletePreview:
        request = supabase_mgmt.SupabaseDeleteRequest(
            selection_mode=selection_mode,
            dates=dates,
        )
        return supabase_mgmt.SupabaseDeletePreview(
            request=request,
            date_rows=date_rows,
            summary=supabase_mgmt._build_supabase_delete_summary(date_rows),
        )

    def assert_query_contains(self, query: str, expected_fragment: str) -> None:
        self.assertIn(
            _normalize_query_text(expected_fragment),
            _normalize_query_text(query),
        )

    def test_build_supabase_delete_preview_summarizes_selected_dates(self) -> None:
        date_rows = (
            self.build_metric_date_row(
                kst_date=date(2026, 4, 21),
                row_count=3,
                min_timestamp="2026-04-20T15:00:00+00:00",
                max_timestamp="2026-04-20T15:30:00+00:00",
            ),
            self.build_metric_date_row(
                kst_date=date(2026, 4, 22),
                row_count=5,
                min_timestamp="2026-04-21T15:00:00+00:00",
                max_timestamp="2026-04-21T16:00:00+00:00",
            ),
        )
        request = supabase_mgmt.SupabaseDeleteRequest(
            selection_mode=supabase_mgmt.SUPABASE_DELETE_MODE_SELECTED,
            dates=(date(2026, 4, 22),),
        )

        preview = supabase_mgmt.build_supabase_delete_preview(date_rows, request)

        self.assertEqual(preview.summary.date_count, 1)
        self.assertEqual(preview.summary.row_count, 5)
        self.assertEqual(preview.summary.min_timestamp, "2026-04-21T15:00:00+00:00")
        self.assertEqual(preview.summary.max_timestamp, "2026-04-21T16:00:00+00:00")
        self.assertEqual(preview.date_rows[0].kst_date, date(2026, 4, 22))

    def test_query_supabase_metric_date_rows_with_delete_lock_executes_lock_query(self) -> None:
        fake_connection = FakeConnection(
            [
                [
                    self.build_grouped_query_row(
                        kst_date=date(2026, 4, 21),
                        row_count=1,
                        min_timestamp="2026-04-20T14:59:59+00:00",
                        max_timestamp="2026-04-20T14:59:59+00:00",
                    ),
                    self.build_grouped_query_row(
                        kst_date=date(2026, 4, 22),
                        row_count=1,
                        min_timestamp="2026-04-20T15:00:00+00:00",
                        max_timestamp="2026-04-20T15:00:00+00:00",
                    ),
                ]
            ]
        )

        result = supabase_mgmt._query_supabase_metric_date_rows_with_delete_lock(fake_connection)

        self.assertEqual(
            result,
            (
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 21),
                    row_count=1,
                    min_timestamp="2026-04-20T14:59:59+00:00",
                    max_timestamp="2026-04-20T14:59:59+00:00",
                ),
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 22),
                    row_count=1,
                    min_timestamp="2026-04-20T15:00:00+00:00",
                    max_timestamp="2026-04-20T15:00:00+00:00",
                ),
            ),
        )
        self.assertEqual(len(fake_connection.executed), 1)
        self.assert_query_contains(
            fake_connection.executed[0][0],
            "LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE",
        )
        self.assert_query_contains(
            fake_connection.executed[0][0],
            "FROM public.all_metrics GROUP BY 1 ORDER BY 1 ASC",
        )
        self.assertEqual(fake_connection.executed[0][1], ())

    def test_execute_supabase_delete_commits_when_preview_matches(self) -> None:
        selected_dates = (date(2026, 4, 21),)
        preview_rows = (
            self.build_metric_date_row(
                kst_date=date(2026, 4, 21),
                row_count=3,
                min_timestamp="2026-04-20T15:00:00+00:00",
                max_timestamp="2026-04-20T15:30:00+00:00",
            ),
        )
        preview = self.build_delete_preview(
            selection_mode=supabase_mgmt.SUPABASE_DELETE_MODE_SELECTED,
            dates=selected_dates,
            date_rows=preview_rows,
        )
        fake_connection = FakeConnection(
            [
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        3,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:30:00+00:00",
                    )
                ],
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        3,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:30:00+00:00",
                    )
                ],
                [(0,)],
            ]
        )

        with patch.object(supabase_mgmt, "_open_supabase_connection", return_value=fake_connection):
            result = supabase_mgmt.execute_supabase_delete(Path("C:/repo"), preview)

        self.assertTrue(fake_connection.commit_called)
        self.assertFalse(fake_connection.rollback_called)
        self.assertTrue(fake_connection.closed)
        self.assertEqual(result.summary.row_count, 3)
        self.assertEqual(len(fake_connection.executed), 3)
        self.assert_query_contains(
            fake_connection.executed[0][0],
            "LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE",
        )
        self.assert_query_contains(
            fake_connection.executed[1][0],
            'WHERE ("timestamp" AT TIME ZONE \'Asia/Seoul\')::date = ANY(%s)',
        )
        self.assert_query_contains(
            fake_connection.executed[2][0],
            'WHERE ("timestamp" AT TIME ZONE \'Asia/Seoul\')::date = ANY(%s)',
        )
        self.assertEqual(fake_connection.executed[1][1], ([date(2026, 4, 21)],))
        self.assertEqual(fake_connection.executed[2][1], ([date(2026, 4, 21)],))
        self.assertEqual(
            fake_connection.session_calls,
            [{"isolation_level": "SERIALIZABLE", "autocommit": False}],
        )

    def test_execute_supabase_delete_rolls_back_when_preview_mismatches(self) -> None:
        preview = self.build_delete_preview(
            selection_mode=supabase_mgmt.SUPABASE_DELETE_MODE_SELECTED,
            dates=(date(2026, 4, 21),),
            date_rows=(
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 21),
                    row_count=3,
                    min_timestamp="2026-04-20T15:00:00+00:00",
                    max_timestamp="2026-04-20T15:30:00+00:00",
                ),
            ),
        )
        fake_connection = FakeConnection(
            [
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        4,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:40:00+00:00",
                    )
                ]
            ]
        )

        with patch.object(supabase_mgmt, "_open_supabase_connection", return_value=fake_connection):
            with self.assertRaisesRegex(ValueError, "preview does not match"):
                supabase_mgmt.execute_supabase_delete(Path("C:/repo"), preview)

        self.assertFalse(fake_connection.commit_called)
        self.assertTrue(fake_connection.rollback_called)
        self.assertTrue(fake_connection.closed)
        self.assertEqual(len(fake_connection.executed), 1)
        self.assert_query_contains(
            fake_connection.executed[0][0],
            "LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE",
        )

    def test_execute_supabase_delete_commits_for_all_delete(self) -> None:
        preview = self.build_delete_preview(
            selection_mode=supabase_mgmt.SUPABASE_DELETE_MODE_ALL,
            dates=(),
            date_rows=(
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 21),
                    row_count=2,
                    min_timestamp="2026-04-20T15:00:00+00:00",
                    max_timestamp="2026-04-20T15:30:00+00:00",
                ),
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 22),
                    row_count=1,
                    min_timestamp="2026-04-21T15:05:00+00:00",
                    max_timestamp="2026-04-21T15:05:00+00:00",
                ),
            ),
        )
        fake_connection = FakeConnection(
            [
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        2,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:30:00+00:00",
                    ),
                    self.build_grouped_query_row(
                        date(2026, 4, 22),
                        1,
                        "2026-04-21T15:05:00+00:00",
                        "2026-04-21T15:05:00+00:00",
                    ),
                ],
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        2,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:30:00+00:00",
                    ),
                    self.build_grouped_query_row(
                        date(2026, 4, 22),
                        1,
                        "2026-04-21T15:05:00+00:00",
                        "2026-04-21T15:05:00+00:00",
                    ),
                ],
                [(0,)],
            ]
        )

        with patch.object(supabase_mgmt, "_open_supabase_connection", return_value=fake_connection):
            result = supabase_mgmt.execute_supabase_delete(Path("C:/repo"), preview)

        self.assertTrue(fake_connection.commit_called)
        self.assertFalse(fake_connection.rollback_called)
        self.assertEqual(result.summary.row_count, 3)
        self.assert_query_contains(
            fake_connection.executed[0][0],
            "LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE",
        )
        self.assert_query_contains(
            fake_connection.executed[1][0],
            "DELETE FROM public.all_metrics RETURNING",
        )
        self.assert_query_contains(
            fake_connection.executed[2][0],
            "SELECT COUNT(*) FROM public.all_metrics",
        )
        self.assertNotIn("ANY(%s)", _normalize_query_text(fake_connection.executed[1][0]))
        self.assertNotIn("ANY(%s)", _normalize_query_text(fake_connection.executed[2][0]))
        self.assertEqual(fake_connection.executed[1][1], ())
        self.assertEqual(fake_connection.executed[2][1], ())

    def test_execute_supabase_delete_rolls_back_when_all_delete_snapshot_is_empty(self) -> None:
        preview = self.build_delete_preview(
            selection_mode=supabase_mgmt.SUPABASE_DELETE_MODE_ALL,
            dates=(),
            date_rows=(
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 21),
                    row_count=2,
                    min_timestamp="2026-04-20T15:00:00+00:00",
                    max_timestamp="2026-04-20T15:30:00+00:00",
                ),
            ),
        )
        fake_connection = FakeConnection([[]])

        with patch.object(supabase_mgmt, "_open_supabase_connection", return_value=fake_connection):
            with self.assertRaisesRegex(ValueError, "found no current rows"):
                supabase_mgmt.execute_supabase_delete(Path("C:/repo"), preview)

        self.assertFalse(fake_connection.commit_called)
        self.assertTrue(fake_connection.rollback_called)
        self.assertTrue(fake_connection.closed)
        self.assertEqual(len(fake_connection.executed), 1)
        self.assert_query_contains(
            fake_connection.executed[0][0],
            "LOCK TABLE public.all_metrics IN SHARE ROW EXCLUSIVE MODE",
        )

    def test_execute_supabase_delete_rolls_back_when_rows_remain_in_selected_range(self) -> None:
        preview = self.build_delete_preview(
            selection_mode=supabase_mgmt.SUPABASE_DELETE_MODE_SELECTED,
            dates=(date(2026, 4, 21),),
            date_rows=(
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 21),
                    row_count=3,
                    min_timestamp="2026-04-20T15:00:00+00:00",
                    max_timestamp="2026-04-20T15:30:00+00:00",
                ),
            ),
        )
        fake_connection = FakeConnection(
            [
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        3,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:30:00+00:00",
                    )
                ],
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        3,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:30:00+00:00",
                    )
                ],
                [(1,)],
            ]
        )

        with patch.object(supabase_mgmt, "_open_supabase_connection", return_value=fake_connection):
            with self.assertRaisesRegex(ValueError, "left rows in the target range"):
                supabase_mgmt.execute_supabase_delete(Path("C:/repo"), preview)

        self.assertFalse(fake_connection.commit_called)
        self.assertTrue(fake_connection.rollback_called)
        self.assert_query_contains(
            fake_connection.executed[2][0],
            'WHERE ("timestamp" AT TIME ZONE \'Asia/Seoul\')::date = ANY(%s)',
        )
        self.assertEqual(fake_connection.executed[2][1], ([date(2026, 4, 21)],))

    def test_query_supabase_metric_date_rows_preserves_kst_grouping_boundary(self) -> None:
        fake_connection = FakeConnection(
            [
                [
                    self.build_grouped_query_row(
                        date(2026, 4, 21),
                        1,
                        "2026-04-20T14:59:59+00:00",
                        "2026-04-20T14:59:59+00:00",
                    ),
                    self.build_grouped_query_row(
                        date(2026, 4, 22),
                        1,
                        "2026-04-20T15:00:00+00:00",
                        "2026-04-20T15:00:00+00:00",
                    ),
                ]
            ]
        )

        result = supabase_mgmt._query_supabase_metric_date_rows(fake_connection)

        self.assertEqual(
            result,
            (
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 21),
                    row_count=1,
                    min_timestamp="2026-04-20T14:59:59+00:00",
                    max_timestamp="2026-04-20T14:59:59+00:00",
                ),
                self.build_metric_date_row(
                    kst_date=date(2026, 4, 22),
                    row_count=1,
                    min_timestamp="2026-04-20T15:00:00+00:00",
                    max_timestamp="2026-04-20T15:00:00+00:00",
                ),
            ),
        )
