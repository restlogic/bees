from __future__ import absolute_import

import webob.dec
from opentracing import global_tracer
from opentracing.ext import tags
from opentracing.propagation import Format

import eventlet

_DISABLED = False


def disable():
    """Disable middleware."""

    global _DISABLED
    _DISABLED = True


def enable():
    """Enable middleware."""

    global _DISABLED
    _DISABLED = False


class WsgiMiddleware(object):
    """WSGI Middleware that enables tracing for an application."""

    def __init__(self, application, enabled=False, **kwargs):
        """Initialize middleware with api-paste.ini arguments."""

        self.application = application
        self.name = "wsgi"
        self.enabled = enabled

    @classmethod
    def factory(cls, global_conf, **local_conf):
        def filter_(app):
            return cls(app, **local_conf)

        return filter_

    @webob.dec.wsgify
    def __call__(self, request):
        if (_DISABLED is not None and _DISABLED
                or _DISABLED is None and not self.enabled):
            return request.get_response(self.application)

        tracer = global_tracer()
        span_ctx = tracer.extract(Format.HTTP_HEADERS, request.headers)
        span_tags = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER}

        with tracer.start_active_span(operation_name=self.name, child_of=span_ctx, tags=span_tags) as scope:
            span = scope.span
            span.set_tag('http.method', request.method)
            span.set_tag('http.scheme', request.scheme)
            span.set_tag('http.url', request.path)
            span.set_tag('http.target', request.query_string)
            span.set_tag('http.headers', dict(request.headers))

            response = request.get_response(self.application)
            span.log_kv({'request.response': response})  # 时间戳

        return response


class EventletWsgiMiddleware(object):
    """WSGI Middleware that enables tracing for an application.(adapted to eventlet)"""

    def __init__(self, application, enabled=False, **kwargs):
        """Initialize middleware with api-paste.ini arguments."""

        self.application = application
        self.name = "wsgi"
        self.enabled = enabled

    @classmethod
    def factory(cls, global_conf, **local_conf):
        def filter_(app):
            return cls(app, **local_conf)

        return filter_

    @webob.dec.wsgify
    def __call__(self, request):
        if (_DISABLED is not None and _DISABLED
                or _DISABLED is None and not self.enabled):
            return request.get_response(self.application)

        tracer = global_tracer()
        span_ctx = tracer.extract(Format.HTTP_HEADERS, request.headers)
        span_tags = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER}

        with tracer.start_active_span(operation_name=self.name, child_of=span_ctx, tags=span_tags) as scope:
            span = scope.span
            span.set_tag('http.method', request.method)
            span.set_tag('http.scheme', request.scheme)
            span.set_tag('http.url', request.path)
            span.set_tag('http.target', request.query_string)
            span.set_tag('http.headers', dict(request.headers))

            response = request.get_response(self.application)
            span.log_kv({'request.response': response})  # 时间戳

        eventlet.sleep()
        return response
