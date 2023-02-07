# -*- coding: utf-8 -*-

import logging

import sqlalchemy
from neutron_lib.db import api
from neutron_lib.rpc import RequestContextSerializer

from .oslo_rpc import bees_rpc_patch
from ..sql import add_tracing

RequestContextSerializer_serialize_context = RequestContextSerializer.serialize_context
RequestContextSerializer_deserialize_context = RequestContextSerializer.deserialize_context

logger = logging.getLogger(__name__)


# serialize neutron_lib
def serialize_context_wrapper(self, context):
    ctxt = RequestContextSerializer_serialize_context(self, context)
    if hasattr(context, 'carrier'):
        ctxt['carrier'] = context.carrier
        logger.debug('Serialize Context Object Carrier: {}'.format(context.carrier))
    return ctxt


# unserialize neutron_lib
def deserialize_context_wrapper(self, context):
    ctxt = RequestContextSerializer_deserialize_context(self, context)
    if 'carrier' in context:
        ctxt.carrier = context['carrier']
        logger.debug('Deserialize Context Object Carrier: {}'.format(context['carrier']))
    return ctxt


# RPC
def rpc_patch():
    setattr(RequestContextSerializer, 'serialize_context', serialize_context_wrapper)
    setattr(RequestContextSerializer, 'deserialize_context', deserialize_context_wrapper)
    bees_rpc_patch()


# neutron_lib
def _set_hook_wrapper(engine):
    api._set_hook(engine)
    logger.debug('Add database tracing')
    add_tracing(sqlalchemy, engine)


# sql
def sql_patch():
    api._CTX_MANAGER.append_on_engine_create(_set_hook_wrapper)


# neutron.agent.metadata.agent.MetadataProxyHandler
from opentracing import global_tracer
from opentracing.ext import tags
from opentracing.propagation import Format
from neutron.agent.metadata.agent import _, LOG
from neutron.common import ipv6_utils
from neutron.agent.metadata.agent import MetadataProxyHandler
import webob
import requests
import urllib


@webob.dec.wsgify(RequestClass=webob.Request)
def __call__wrapper(self, req):
    try:
        LOG.debug("Request: %s", req)

        tracer = global_tracer()
        span_ctx = tracer.extract(Format.HTTP_HEADERS, req.headers)
        span_tags = {tags.SPAN_KIND: tags.SPAN_KIND_RPC_SERVER}

        with tracer.start_active_span(operation_name="MetadataProxyHandler", child_of=span_ctx,
                                      tags=span_tags) as scope:
            span = scope.span
            span.set_tag('http.method', req.method)
            span.set_tag('http.scheme', req.scheme)
            span.set_tag('http.url', req.path)
            span.set_tag('http.headers', dict(req.headers))
            instance_id, tenant_id = self._get_instance_and_tenant_id(req)
            if instance_id:
                res = self._proxy_request(instance_id, tenant_id, req)
                if isinstance(res, webob.exc.HTTPNotFound):
                    LOG.info("The instance: %s is not present anymore, "
                             "skipping cache...", instance_id)
                    instance_id, tenant_id = self._get_instance_and_tenant_id(
                        req, skip_cache=True)
                    if instance_id:
                        return self._proxy_request(instance_id, tenant_id, req)
                return res
            else:
                return webob.exc.HTTPNotFound()
    except Exception:
        LOG.exception("Unexpected error.")
        msg = _('An unknown error has occurred. '
                'Please try your request again.')
        explanation = str(msg)
        return webob.exc.HTTPInternalServerError(explanation=explanation)


def _proxy_request(self, instance_id, tenant_id, req):
    headers = {
        'X-Forwarded-For': req.headers.get('X-Forwarded-For'),
        'X-Instance-ID': instance_id,
        'X-Tenant-ID': tenant_id,
        'X-Instance-ID-Signature': self._sign_instance_id(instance_id)
    }

    tracer = global_tracer()
    span = tracer.active_span
    if span:
        tracer.inject(span, Format.HTTP_HEADERS, headers)

    nova_host_port = ipv6_utils.valid_ipv6_url(
        self.conf.nova_metadata_host,
        self.conf.nova_metadata_port)

    url = urllib.parse.urlunsplit((
        self.conf.nova_metadata_protocol,
        nova_host_port,
        req.path_info,
        req.query_string,
        ''))

    disable_ssl_certificate_validation = self.conf.nova_metadata_insecure
    if self.conf.auth_ca_cert and not disable_ssl_certificate_validation:
        verify_cert = self.conf.auth_ca_cert
    else:
        verify_cert = not disable_ssl_certificate_validation

    client_cert = None
    if self.conf.nova_client_cert and self.conf.nova_client_priv_key:
        client_cert = (self.conf.nova_client_cert,
                       self.conf.nova_client_priv_key)

    resp = requests.request(method=req.method, url=url,
                            headers=headers,
                            data=req.body,
                            cert=client_cert,
                            verify=verify_cert)

    if resp.status_code == 200:
        req.response.content_type = resp.headers['content-type']
        req.response.body = resp.content
        LOG.debug(str(resp))
        return req.response
    elif resp.status_code == 403:
        LOG.warning(
            'The remote metadata server responded with Forbidden. This '
            'response usually occurs when shared secrets do not match.'
        )
        return webob.exc.HTTPForbidden()
    elif resp.status_code == 400:
        return webob.exc.HTTPBadRequest()
    elif resp.status_code == 404:
        return webob.exc.HTTPNotFound()
    elif resp.status_code == 409:
        return webob.exc.HTTPConflict()
    elif resp.status_code == 500:
        msg = _(
            'Remote metadata server experienced an internal server error.'
        )
        LOG.warning(msg)
        explanation = str(msg)
        return webob.exc.HTTPInternalServerError(explanation=explanation)
    else:
        raise Exception(_('Unexpected response code: %s') %
                        resp.status_code)


def proxy_patch():
    setattr(MetadataProxyHandler, '__call__', __call__wrapper)
    setattr(MetadataProxyHandler, '_proxy_request', _proxy_request)
