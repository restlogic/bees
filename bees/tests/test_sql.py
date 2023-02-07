from __future__ import absolute_import

import contextlib
from unittest import TestCase, mock

from bees import sql


class TestSql(TestCase):

    @mock.patch("bees_profiler.sql._before_cursor_execute")
    @mock.patch("bees_profiler.sql._after_cursor_execute")
    def test_disabled(self, mock_after_exc, mock_before_exc):
        sqlalchemy = mock.MagicMock()
        engine = mock.MagicMock()

        sql.disable()
        sql.add_tracing(sqlalchemy, engine)
        self.assertFalse(mock_after_exc.called)
        self.assertFalse(mock_before_exc.called)

        sql.enable()
        sql.add_tracing(sqlalchemy, engine)
        self.assertTrue(mock_after_exc.called)
        self.assertTrue(mock_before_exc.called)

    def test_before_execute(self):
        handler = sql._before_cursor_execute()
        conn = mock.MagicMock()
        context = mock.MagicMock()
        context.compiled.statement.__visit_name__ = "test"
        handler(conn, "cursor", "statement", "params", context, "executemany")
        self.assertTrue(context._span)

    def test_after_execute(self):
        handler = sql._after_cursor_execute()
        cursor = mock.MagicMock()
        context = mock.MagicMock()
        context._span = mock.MagicMock()
        handler("conn", cursor, "statement", "params", context, "executemany")
        context._span.finish.assert_called_once()

    def test_error_handle(self):
        original_exception = Exception("error")
        chained_exception = Exception("error and the reason")

        sqlalchemy_exception_ctx = mock.MagicMock()
        sqlalchemy_exception_ctx._span = mock.MagicMock()
        sqlalchemy_exception_ctx.original_exception = original_exception
        sqlalchemy_exception_ctx.chained_exception = chained_exception

        sql._handle_error(sqlalchemy_exception_ctx)
        sqlalchemy_exception_ctx._span.finish.assert_called_once()

    @mock.patch("bees_profiler.sql._before_cursor_execute")
    @mock.patch("bees_profiler.sql._after_cursor_execute")
    @mock.patch("bees_profiler.sql._handle_error")
    def test_add_tracing(self, mock_handle_error, mock_after_execute, mock_before_execute):
        sqlalchemy = mock.MagicMock()
        engine = mock.MagicMock()

        mock_before_execute.return_value = "before"
        mock_after_execute.return_value = "after"

        sql.add_tracing(sqlalchemy, engine)
        mock_before_execute.assert_called_once()
        mock_after_execute.assert_called_once()

        expected_calls = [
            mock.call(engine, "before_cursor_execute", "before"),
            mock.call(engine, "after_cursor_execute", "after"),
            mock.call(engine, "handle_error", mock_handle_error),
        ]

        self.assertEqual(sqlalchemy.event.listen.call_args_list, expected_calls)

    @mock.patch("bees_profiler.sql._before_cursor_execute")
    @mock.patch("bees_profiler.sql._after_cursor_execute")
    @mock.patch("bees_profiler.sql._handle_error")
    def test_wrap_session(self, mock_handle_error, mock_after_execute, mock_before_execute):
        sqlalchemy = mock.MagicMock()

        @contextlib.contextmanager
        def _session():
            session = mock.MagicMock()
            # current engine object stored within the session
            session.bind = mock.MagicMock()
            session._traced = None
            yield session

        mock_before_execute.return_value = "before"
        mock_after_execute.return_value = "after"

        sess = sql.wrap_session(sqlalchemy, _session())

        with sess as s:
            pass

        mock_before_execute.assert_called_once()
        mock_after_execute.assert_called_once()

        expected_calls = [
            mock.call(s.bind, "before_cursor_execute", "before"),
            mock.call(s.bind, "after_cursor_execute", "after"),
            mock.call(s.bind, "handle_error", mock_handle_error),
        ]

        self.assertEqual(sqlalchemy.event.listen.call_args_list, expected_calls)
