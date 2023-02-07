# -*- coding: utf-8 -*-


from keystoneauth1.session import Session
from opentracing import global_tracer
from opentracing.propagation import Format

_Session_request = Session.request


def request_wrapper(self, url, method, json=None, original_ip=None,
                    user_agent=None, redirect=None, authenticated=None,
                    endpoint_filter=None, auth=None, requests_auth=None,
                    raise_exc=True, allow_reauth=True, log=True,
                    endpoint_override=None, connect_retries=None, logger=None,
                    allow=None, client_name=None, client_version=None,
                    microversion=None, microversion_service_type=None,
                    status_code_retries=0, retriable_status_codes=None,
                    rate_semaphore=None, global_request_id=None,
                    connect_retry_delay=None, status_code_retry_delay=None,
                    **kwargs):
    """Wraps keystoneauth1.session.Session.request"""

    headers = kwargs.setdefault('headers', dict())
    headers = dict(headers)

    tracer = global_tracer()
    span = tracer.active_span
    if span:
        tracer.inject(span, Format.HTTP_HEADERS, headers)
        kwargs['headers'] = headers

    resp = _Session_request(self, url, method, json, original_ip,
                            user_agent, redirect, authenticated,
                            endpoint_filter, auth, requests_auth,
                            raise_exc, allow_reauth, log,
                            endpoint_override, connect_retries, logger,
                            allow, client_name, client_version,
                            microversion, microversion_service_type,
                            status_code_retries, retriable_status_codes,
                            rate_semaphore, global_request_id,
                            connect_retry_delay, status_code_retry_delay,
                            **kwargs)

    return resp
