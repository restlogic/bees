# -*- coding: utf-8 -*-

import eventlet
from opentracing import Scope, ScopeManager
from opentracing.scope_managers.constants import ACTIVE_ATTR


class EventletScopeManager(ScopeManager):
    def activate(self, span, finish_on_close):
        scope = _EventletScope(self, span, finish_on_close)
        self._set_scope(scope)
        return scope

    @property
    def active(self):
        return self._get_scope()

    def _get_scope(self, greenthread=None):
        if greenthread is None:
            greenthread = eventlet.getcurrent()

        return getattr(greenthread, ACTIVE_ATTR, None)

    def _set_scope(self, scope, greenthread=None):
        if greenthread is None:
            greenthread = eventlet.getcurrent()

        setattr(greenthread, ACTIVE_ATTR, scope)


class _EventletScope(Scope):
    def __init__(self, manager, span, finish_on_close):
        super(_EventletScope, self).__init__(manager, span)
        self._finish_on_close = finish_on_close
        self._to_restore = manager.active

    def close(self):
        if self.manager.active is not self:
            return

        self.manager._set_scope(self._to_restore)

        if self._finish_on_close:
            self.span.finish()
