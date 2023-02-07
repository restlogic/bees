# -*- coding: utf-8 -*-

import logging
import os

from opentracing import global_tracer
from opentracing.ext import tags
from opentracing.propagation import Format
from oslo_messaging.rpc.client import _BaseCallContext
from oslo_messaging.rpc.dispatcher import RPCDispatcher
from oslo_service.service import Launcher

from ..eventlet.config import BeesConfig

REPORTING_HOST = os.environ.get("REPORTING_HOST") or "127.0.0.1"

_BaseCallContext_call = _BaseCallContext.call
_BaseCallContext_cast = _BaseCallContext.cast
_RPCDispatcher_dispatch = RPCDispatcher.dispatch
_Launcher_launch_service = Launcher.launch_service

logger = logging.getLogger(__name__)


def extract_parent_span(context):
    parent_ctx = None

    try:
        if 'carrier' in context:
            carrier = context['carrier']
            logger.debug("Parent carrier extracted:{}".format(carrier))
            tracer = global_tracer()
            parent_ctx = tracer.extract(format=Format.TEXT_MAP, carrier=carrier)
    except Exception as e:
        logger.exception('Carrier extraction failed in before_dispatcher: %s' % e)
    return parent_ctx


def create_child_span(target, context, method, kwargs, operation):
    """Create child span before sending RPC messaging"""

    tracer = global_tracer()
    parent_ctx = tracer.active_span
    span = tracer.start_span(operation_name=operation, child_of=parent_ctx)

    span.set_tag(tags.SPAN_KIND, tags.SPAN_KIND_RPC_CLIENT)
    span.set_tag('rpc.method', method)
    span.set_tag('rpc.kwargs', kwargs)

    request_id = context.request_id
    if request_id:
        span.set_tag('request.id', request_id)

    carrier = {}
    tracer.inject(span_context=span.context,
                  format=Format.TEXT_MAP,
                  carrier=carrier)

    context.carrier = carrier
    logger.debug("Injected carrier into context object: {}".format(context.carrier))

    return span


def call_wrapper(self, ctxt, method, **kwargs):
    """Wraps oslo_messaging.rpc.client._BaseCallContext.call"""

    logger.debug("RPC CALL method: {}, kwargs: {}".format(method, kwargs))
    span = create_child_span(self.target, ctxt, method, kwargs, 'RPC_CALL')
    resp = _BaseCallContext_call(self, ctxt, method, **kwargs)  # serialize_context
    span.set_tag('rpc.result', resp)
    span.finish()
    return resp


def cast_wrapper(self, ctxt, method, **kwargs):
    """Wraps oslo_messaging.rpc.client._BaseCallContext.cast"""

    logger.debug("RPC CAST method: {}, kwargs: {}".format(method, kwargs))
    span = create_child_span(self.target, ctxt, method, kwargs, 'RPC_CAST')
    _BaseCallContext_cast(self, ctxt, method, **kwargs)  # serialize_context
    span.finish()


def dispatch_wrapper(self, incoming):
    """Wraps oslo_messaging.rpc.dispatcher.RPCDispatcher"""

    message = incoming.message
    ctxt = incoming.ctxt

    method = message.get('method')
    args = message.get('args', {})
    kwargs = message.get('kwargs', {})
    namespace = message.get('namespace')

    logger.debug("dispatch target method: {}, namespace: {}, args : {}".format(method, namespace, args))

    tracer = global_tracer()

    # we need to prepare tags upfront, mainly because RPC_SERVER tag must be
    # set when starting the span, to support Zipkin's one-span-per-RPC model
    tags_dict = {
        tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER,
        'rpc.method': method,
        'rpc.args': args,
        'rpc.kwargs': kwargs,
    }

    request_id = ctxt.get('request_id', None)
    if request_id:
        tags_dict['request.id'] = request_id
    if incoming.msg_id:
        operation = 'RPC_CALL'  # RPC Call
    else:
        operation = 'RPC_CAST'  # RPC cast

    # parent_ctx = tracer.active_span or extract_parent_span(ctxt)
    parent_ctx = extract_parent_span(ctxt)
    with tracer.start_active_span(operation_name=operation, child_of=parent_ctx, tags=tags_dict) as scope:
        ret = _RPCDispatcher_dispatch(self, incoming)  # deserialize_context
        scope.span.set_tag('rpc.result', ret)
        logger.debug("Dispatch done")
        return ret


def launch_service_wrapper(self, service, workers=1):
    """Wraps oslo_service.service.Launcher.launch_service"""

    if hasattr(service, 'binary'):
        service_name = service.binary
    elif hasattr(service, '_service'):
        service_name = service._service.name
    elif hasattr(service, '_services'):  # periodic
        for obj in service._services:
            for attr in dir(obj):
                if not attr.startswith('__'):
                    logger.info("obj.%s = %r" % (attr, getattr(obj, attr)))
        service_name = 'neutron-multiple-services'
    elif hasattr(service, 'start_listeners_method'):  # neutron-server
        service_name = service.start_listeners_method
    else:
        for attr in dir(service):
            if not attr.startswith('__'):
                logger.info("obj.%s = %r" % (attr, getattr(service, attr)))
        service_name = 'unknown-service'

    if service_name == 'Neutron':
        service_name = 'neutron-server'
    elif service_name.startswith('start'):  # 两种 rpc
        service_name = 'neutron-server-' + service_name

    BeesConfig(
        config={
            'sampler': {
                'type': 'const',
                'param': 1,
            },
            'logging': True,
            'reporter_batch_size': 1,
            'local_agent': {
                'reporting_host': REPORTING_HOST,
                'reporting_port': '6831'
            }
        },
        service_name=service_name,
    ).initialize_tracer()

    _Launcher_launch_service(self, service, workers=workers)


def bees_rpc_patch():
    setattr(_BaseCallContext, 'call', call_wrapper)
    setattr(_BaseCallContext, 'cast', cast_wrapper)
    setattr(RPCDispatcher, 'dispatch', dispatch_wrapper)
    setattr(Launcher, 'launch_service', launch_service_wrapper)
