from __future__ import absolute_import

import contextlib

from opentracing import global_tracer

_DISABLED = False


def disable():
    """Disable tracing of all DB queries. Reduce a lot size of profiles."""

    global _DISABLED
    _DISABLED = True


def enable():
    """add_tracing adds event listeners for sqlalchemy."""

    global _DISABLED
    _DISABLED = False


def _before_cursor_execute():
    """Add listener that will send trace info before query is executed."""

    def handler(conn, cursor, statement, params, context, executemany):

        if context.compiled is not None:
            stmt_obj = context.compiled.statement
            name = stmt_obj.__visit_name__
        else:
            name = 'other'

        parent_span = getattr(conn, '_parent_span', None)
        tracer = global_tracer()
        span = tracer.start_span(operation_name=name, child_of=parent_span)

        span.set_tag('db.statement', statement)

        span.set_tag('sqlalchemy.dialect', context.dialect.name)
        span.set_tag('component', 'sqlalchemy')
        span.set_tag('db.type', 'sql')
        span.set_tag('db.params', params)

        context._span = span

    return handler


def _after_cursor_execute():
    """Add listener that will send trace info after query is executed."""

    def handler(conn, cursor, statement, params, context, executemany):
        span = getattr(context, '_span', None)
        if span is None:
            return

        span.set_tag('db.result', str(cursor._rows))
        span.finish()

    return handler


def _handle_error(exception_context):
    """Handle SQLAlchemy errors"""

    span = getattr(exception_context, '_span', None)
    if span is None:
        return

    original_exception = str(exception_context.original_exception)
    chained_exception = str(exception_context.chained_exception)

    span.set_tag('sqlalchemy.original_exception', original_exception)
    span.set_tag('sqlalchemy.chained_exception', chained_exception)
    span.set_tag('error', 'true')

    span.finish()


def add_tracing(sqlalchemy, engine):
    """Add tracing to all sqlalchemy calls."""

    if not _DISABLED:
        sqlalchemy.event.listen(engine, "before_cursor_execute", _before_cursor_execute())
        sqlalchemy.event.listen(engine, "after_cursor_execute", _after_cursor_execute())
        sqlalchemy.event.listen(engine, "handle_error", _handle_error)


def wrap_parent(sqlalchemy, session):
    """DEPRECATED"""

    sqlalchemy.event.listen(session, 'after_begin', _after_begin_handler)


def _after_begin_handler(session, transaction, conn):
    """DEPRECATED"""

    if getattr(session, '_traced', False):
        conn._traced = True
        parent_span = getattr(session, '_parent_span', None)
        if parent_span is not None:
            conn._parent_span = parent_span


@contextlib.contextmanager
def wrap_session(sqlalchemy, session):
    """Mark a session to be traced"""

    with session as sess:
        if not getattr(sess, "_traced", False):
            parent_span = global_tracer().active_span
            sess.bind._parent_span = parent_span
            sess._traced = True
            add_tracing(sqlalchemy, sess.bind)
        yield sess
