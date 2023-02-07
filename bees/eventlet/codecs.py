# -*- coding: utf-8 -*-

"""
:param tracer: optional tracer instance to use. If not specified
    the global opentracing.tracer will be used.
:param span_context: the :class:`SpanContext` instance to inject
:param format: a python object instance that represents a given
    carrier format. `format` may be of any type, and `format` equality
    is defined by python ``==`` equality.
:param carrier: the format-specific carrier object to inject into
:param carrier: the format-specific carrier object to extract from
"""

import json

import six
from jaeger_client.codecs import span_context_to_string, span_context_from_string
from jaeger_client.span import Span
from jaeger_client.span_context import SpanContext
from opentracing import (
    UnsupportedFormatException,
    InvalidCarrierException,
)
from six.moves import urllib_parse

#: Http header that will contain the needed traces data.
X_TRACE_INFO = "X-Trace-Info"

#: Http header that will contain the traces data hmac (that will be validated).
X_TRACE_HMAC = "X-Trace-HMAC"

data = {
    X_TRACE_INFO: [0],
    X_TRACE_HMAC: [1]
}


def inject(tracer, span_context, format, carrier):
    codec = tracer.codecs.get(format, None)  # TextCodec 对象
    if codec is None:
        raise UnsupportedFormatException(format)
    if isinstance(span_context, Span):
        # be flexible and allow Span as argument, not only SpanContext
        span_context = span_context.context
    if not isinstance(span_context, SpanContext):
        raise ValueError(
            'Expecting Jaeger SpanContext, not %s', type(span_context))
    _inject(codec, span_context, carrier)


def _inject(codec, span_context, carrier):
    if not isinstance(carrier, dict): 
        raise InvalidCarrierException('carrier not a collection')
    # Note: we do not url-encode the trace ID because the ':' separator
    # is not a problem for HTTP header values
    # carrier[codec.trace_id_header] = span_context_to_string(
    #     trace_id=span_context.trace_id, span_id=span_context.span_id,
    #     parent_id=span_context.parent_id, flags=span_context.flags)
    carrier[X_TRACE_INFO] = json.dumps({codec.trace_id_header: span_context_to_string(
        trace_id=span_context.trace_id, span_id=span_context.span_id,
        parent_id=span_context.parent_id, flags=span_context.flags)
    })
    baggage = span_context.baggage
    if baggage:
        for key, value in six.iteritems(baggage):
            encoded_key = key
            if codec.url_encoding:
                if six.PY2 and isinstance(value, six.text_type):
                    encoded_value = urllib_parse.quote(value.encode('utf-8'))
                else:
                    encoded_value = urllib_parse.quote(value)
                # we assume that self.url_encoding means we are injecting
                # into HTTP headers. httplib does not like unicode strings
                # so we convert the key to utf-8. The URL-encoded value is
                # already a plain string.
                if six.PY2 and isinstance(key, six.text_type):
                    encoded_key = key.encode('utf-8')
            else:
                if six.PY3 and isinstance(value, six.binary_type):
                    encoded_value = str(value, 'utf-8')
                else:
                    encoded_value = value
            if six.PY3 and isinstance(key, six.binary_type):
                encoded_key = str(key, 'utf-8')
            # Leave the below print(), you will thank me next time you debug unicode strings
            # print('adding baggage', key, '=>', value, 'as', encoded_key, '=>', encoded_value)
            header_key = '%s%s' % (codec.baggage_prefix, encoded_key)
            # carrier[header_key] = encoded_value
            carrier[X_TRACE_HMAC] = json.dumps({header_key: encoded_value})


def extract(tracer, format, carrier):
    codec = tracer.codecs.get(format, None)
    if codec is None:
        raise UnsupportedFormatException(format)
    return _extract(codec, carrier)


def _extract(codec, carrier):
    if not hasattr(carrier, 'items'):
        raise InvalidCarrierException('carrier not a collection')
    trace_id, span_id, parent_id, flags = None, None, None, None
    baggage = None
    debug_id = None
    for k, v in six.iteritems(carrier):
        if k == X_TRACE_INFO or k == X_TRACE_HMAC:
            for key, value in six.iteritems(json.loads(v)):
                uc_key = key.lower()
                if uc_key == codec.trace_id_header:
                    if codec.url_encoding:
                        value = urllib_parse.unquote(value)
                    trace_id, span_id, parent_id, flags = \
                        span_context_from_string(value)
                elif uc_key.startswith(codec.baggage_prefix):
                    if codec.url_encoding:
                        value = urllib_parse.unquote(value)
                    attr_key = key[codec.prefix_length:]
                    if baggage is None:
                        baggage = {attr_key.lower(): value}
                    else:
                        baggage[attr_key.lower()] = value
                elif uc_key == codec.debug_id_header:
                    if codec.url_encoding:
                        value = urllib_parse.unquote(value)
                    debug_id = value
                elif uc_key == codec.baggage_header:
                    if codec.url_encoding:
                        value = urllib_parse.unquote(value)
                    baggage = _parse_baggage_header(value, baggage)
    if not trace_id or not span_id:
        # reset all IDs
        trace_id, span_id, parent_id, flags = None, None, None, None
    if not trace_id and not debug_id and not baggage:
        return None
    return SpanContext(trace_id=trace_id, span_id=span_id,
                       parent_id=parent_id, flags=flags,
                       baggage=baggage, debug_id=debug_id)


def _parse_baggage_header(header, baggage):
    for part in header.split(','):
        kv = part.strip().split('=')
        if len(kv) == 2:
            if not baggage:
                baggage = {}
            baggage[kv[0]] = kv[1]
    return baggage
