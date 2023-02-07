# -*- coding: utf-8 -*-

from __future__ import absolute_import

import json
import random
from threading import Lock

import eventlet
from jaeger_client.constants import DEFAULT_SAMPLING_INTERVAL
from jaeger_client.metrics import Metrics, LegacyMetricsFactory
from jaeger_client.sampler import (
    DEFAULT_MAX_OPERATIONS,
    DEFAULT_SAMPLING_PROBABILITY,
    OPERATION_SAMPLING_STR,
    STRATEGY_TYPE_STR,
    PROBABILISTIC_SAMPLING_STRATEGY,
    RATE_LIMITING_SAMPLING_STRATEGY,
)
from jaeger_client.sampler import Sampler, AdaptiveSampler, RateLimitingSampler, ProbabilisticSampler, SamplerMetrics, \
    default_logger
from jaeger_client.sampler import get_rate_limit, get_sampling_probability
from jaeger_client.utils import ErrorReporter


class BeesRemoteControlledSampler(Sampler):
    """Periodically loads the sampling strategy from a remote server."""

    def __init__(self, channel, service_name, **kwargs):
        """
        :param channel: channel for communicating with jaeger-agent
        :param service_name: name of this application
        :param kwargs: optional parameters
            - init_sampler: initial value of the sampler,
                else ProbabilisticSampler(0.001)
            - sampling_refresh_interval: interval in seconds for polling
              for new strategy
            - logger: Logger instance
            - metrics: metrics facade, used to emit metrics on errors.
                This parameter has been deprecated, please use
                metrics_factory instead.
            - metrics_factory: used to generate metrics for errors
            - error_reporter: ErrorReporter instance
            - max_operations: maximum number of unique operations the
              AdaptiveSampler will keep track of
        :param init:
        :return:
        """
        super(BeesRemoteControlledSampler, self).__init__()
        self._channel = channel
        self.service_name = service_name
        self.logger = kwargs.get('logger', default_logger)
        self.sampler = kwargs.get('init_sampler')
        self.sampling_refresh_interval = \
            kwargs.get('sampling_refresh_interval') or DEFAULT_SAMPLING_INTERVAL
        self.metrics_factory = kwargs.get('metrics_factory') \
                               or LegacyMetricsFactory(kwargs.get('metrics') or Metrics())
        self.metrics = SamplerMetrics(self.metrics_factory)
        self.error_reporter = kwargs.get('error_reporter') or \
                              ErrorReporter(Metrics())
        self.max_operations = kwargs.get('max_operations') or \
                              DEFAULT_MAX_OPERATIONS

        if not self.sampler:
            self.sampler = ProbabilisticSampler(DEFAULT_SAMPLING_PROBABILITY)
        else:
            self.sampler.is_sampled(0)  # assert we got valid sampler API

        self.lock = Lock()
        self.running = True
        # self.periodic = None

        eventlet.spawn_n(self._init_polling())
        print("_init_polling")

        # self.io_loop = channel.io_loop  # ThreadLoop
        # if not self.io_loop:
        #     self.logger.error(
        #         'Cannot acquire IOLoop, sampler will not be updated')
        # else:
        #     # according to IOLoop docs, it's not safe to use timeout methods
        #     # unless already running in the loop, so we use `add_callback`
        #     self.io_loop.add_callback(self._init_polling) 

    def is_sampled(self, trace_id, operation=''):
        with self.lock:
            return self.sampler.is_sampled(trace_id, operation)

    def _init_polling(self):
        """
        Bootstrap polling for sampling strategy.

        To avoid spiky traffic from the samplers, we use a random delay
        before the first poll.
        """
        with self.lock:
            if not self.running:
                return
            r = random.Random()
            delay = r.random() * self.sampling_refresh_interval
            eventlet.spawn_after(delay, self._delayed_polling())
            print("_delayed_polling")
            # self.io_loop.call_later(delay=delay,
            #                         callback=self._delayed_polling)
            self.logger.info(
                'Delaying sampling strategy polling by %d sec', delay)

    def _delayed_polling(self):
        while True:
            with self.lock:
                if not self.running:
                    return
                eventlet.spawn_n(self._sampling_request)
                print("_sampling_request")
                self.logger.info(
                    'Tracing sampler started with sampling refresh '
                    'interval %d sec', self.sampling_refresh_interval)
            delay = self.sampling_refresh_interval * 1000
            eventlet.sleep(delay)

        # periodic = self._create_periodic_callback()
        # self._poll_sampling_manager()  # Initialize sampler now
        # with self.lock:
        #     if not self.running:
        #         return
        #     self.periodic = periodic
        #     periodic.start()  # start the periodic cycle
        #     self.logger.info(
        #         'Tracing sampler started with sampling refresh '
        #         'interval %d sec', self.sampling_refresh_interval)

    # def _create_periodic_callback(self):
    #     return PeriodicCallback(
    #         callback=self._poll_sampling_manager,
    #         # convert interval to milliseconds
    #         callback_time=self.sampling_refresh_interval * 1000)

    def _sampling_request(self):
        import requests
        DEFAULT_TIMEOUT = 15
        path = "sampling"
        args = {'service': self.service_name}
        url = 'http://%s:%d/%s' % (self._channel.agent_http_host, self._channel.agent_http_port, path)
        response = requests.get(url, params=args, timeout=DEFAULT_TIMEOUT)
        response_body = response.json()
        try:
            sampling_strategies_response = json.loads(response_body)
            self.metrics.sampler_retrieved(1)
        except Exception as e:
            self.metrics.sampler_query_failure(1)
            self.error_reporter.error(
                'Fail to parse sampling strategy '
                'from jaeger-agent: %s [%s]', e, response_body)
            return

        self._update_sampler(sampling_strategies_response)
        self.logger.debug('Tracing sampler set to %s', self.sampler)

    def _sampling_request_callback(self, future):
        exception = future.exception()
        if exception:
            self.metrics.sampler_query_failure(1)
            self.error_reporter.error(
                'Fail to get sampling strategy from jaeger-agent: %s',
                exception)
            return

        response = future.result()

        # In Python 3.5 response.body is of type bytes and json.loads() does only support str
        # See: https://github.com/jaegertracing/jaeger-client-python/issues/180
        if hasattr(response.body, 'decode') and callable(response.body.decode):
            response_body = response.body.decode('utf-8')
        else:
            response_body = response.body

        try:
            sampling_strategies_response = json.loads(response_body)
            self.metrics.sampler_retrieved(1)
        except Exception as e:
            self.metrics.sampler_query_failure(1)
            self.error_reporter.error(
                'Fail to parse sampling strategy '
                'from jaeger-agent: %s [%s]', e, response_body)
            return

        self._update_sampler(sampling_strategies_response)
        self.logger.debug('Tracing sampler set to %s', self.sampler)

    def _update_sampler(self, response):
        with self.lock:
            try:
                if response.get(OPERATION_SAMPLING_STR):
                    self._update_adaptive_sampler(response.get(OPERATION_SAMPLING_STR))
                else:
                    self._update_rate_limiting_or_probabilistic_sampler(response)
            except Exception as e:
                self.metrics.sampler_update_failure(1)
                self.error_reporter.error(
                    'Fail to update sampler'
                    'from jaeger-agent: %s [%s]', e, response)

    def _update_adaptive_sampler(self, per_operation_strategies):
        if isinstance(self.sampler, AdaptiveSampler):
            self.sampler.update(per_operation_strategies)
        else:
            self.sampler = AdaptiveSampler(per_operation_strategies, self.max_operations)
        self.metrics.sampler_updated(1)

    def _update_rate_limiting_or_probabilistic_sampler(self, response):
        s_type = response.get(STRATEGY_TYPE_STR)
        new_sampler = self.sampler
        if s_type == PROBABILISTIC_SAMPLING_STRATEGY:
            sampling_rate = get_sampling_probability(response)
            new_sampler = ProbabilisticSampler(rate=sampling_rate)
        elif s_type == RATE_LIMITING_SAMPLING_STRATEGY:
            mtps = get_rate_limit(response)
            if mtps < 0 or mtps >= 500:
                raise ValueError(
                    'Rate limiting parameter not in [0, 500) range: %s' % mtps)
            if isinstance(self.sampler, RateLimitingSampler):
                if self.sampler.update(max_traces_per_second=mtps):
                    self.metrics.sampler_updated(1)
            else:
                new_sampler = RateLimitingSampler(max_traces_per_second=mtps)
        else:
            raise ValueError('Unsupported sampling strategy type: %s' % s_type)

        if self.sampler != new_sampler:
            self.sampler = new_sampler
            self.metrics.sampler_updated(1)

    def _poll_sampling_manager(self):
        # LocalAgentHTTP -> request_sampling_strategy -> _request -> tornado.httpclient.AsyncHTTPClien
        # fetch url -> _sampling_request
        self.logger.debug('Requesting tracing sampler refresh')
        fut = self._channel.request_sampling_strategy(self.service_name)
        fut.add_done_callback(self._sampling_request_callback)

    def close(self):
        with self.lock:
            self.running = False
            # if self.periodic:
            #     self.periodic.stop()
