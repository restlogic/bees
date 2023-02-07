# -*- coding: utf-8 -*-

from __future__ import absolute_import

import socket

import eventlet
from jaeger_client.reporter import (
    NullReporter,
    ReporterMetrics,
    LegacyMetricsFactory,
    ErrorReporter,
    Metrics,
    default_logger,
    Agent,
    DEFAULT_FLUSH_INTERVAL,
    TCompactProtocol,
    thrift
)


class BeesReporter(NullReporter):
    """Receives completed spans from Tracer and submits them out of process."""

    def __init__(self, channel, queue_capacity=100, batch_size=10,
                 flush_interval=DEFAULT_FLUSH_INTERVAL, io_loop=None,
                 error_reporter=None, metrics=None, metrics_factory=None,
                 **kwargs):
        """
        :param channel: a communication channel to jaeger-agent
        :param queue_capacity: how many spans we can hold in memory before
            starting to drop spans
        :param batch_size: how many spans we can submit at once to Collector
        :param flush_interval: how often the auto-flush is called (in seconds)
        :param io_loop: which IOLoop to use. If None, try to get it from
            channel (only works if channel is tchannel.sync)
        :param error_reporter:
        :param metrics: an instance of Metrics class, or None. This parameter
            has been deprecated, please use metrics_factory instead.
        :param metrics_factory: an instance of MetricsFactory class, or None.
        :param kwargs:
            'logger'
        :return:
        """
        from threading import Lock

        self._channel = channel
        self.queue_capacity = queue_capacity
        self.batch_size = batch_size
        self.metrics_factory = metrics_factory or LegacyMetricsFactory(metrics or Metrics())
        self.metrics = ReporterMetrics(self.metrics_factory)
        self.error_reporter = error_reporter or ErrorReporter(Metrics())
        self.logger = kwargs.get('logger', default_logger)
        self.agent = Agent.Client(self._channel, self)

        if queue_capacity < batch_size:
            raise ValueError('Queue capacity cannot be less than batch size')

        self.queue = eventlet.queue.Queue(maxsize=queue_capacity)
        self.stop = object()
        self.stopped = False
        self.stop_lock = Lock()
        self.flush_interval = flush_interval or None
        self._process = None

    def set_process(self, service_name, tags, max_length):
        self._process = thrift.make_process(
            service_name=service_name, tags=tags, max_length=max_length,
        )

    # tracer io_loop.add_callback
    def report_span(self, span):
        try:
            with self.stop_lock:
                stopped = self.stopped
            if stopped:
                self.metrics.reporter_dropped(1)
            else:
                eventlet.spawn(self.queue.put_nowait, span)
                eventlet.sleep()
                if self.queue.qsize() >= self.batch_size:
                    eventlet.spawn(self._consume_queue)
                    eventlet.sleep()
        except eventlet.queue.Full:
            self.metrics.reporter_dropped(1)

    def _consume_queue(self):
        spans = []
        stopped = False
        while not stopped:
            while len(spans) < self.batch_size:
                try:
                    # using timeout allows periodic flush with smaller packet
                    timeout = self.flush_interval
                    # span = self.queue.get(timeout=timeout)
                    gt = eventlet.spawn(self.queue.get, {'timeout': timeout})
                    span = gt.wait()
                except eventlet.TimeoutError:
                    break
                else:
                    if span == self.stop:
                        stopped = True
                        self.queue.task_done()
                        # don't return yet, submit accumulated spans first
                        break
                    else:
                        spans.append(span)
            if spans:
                self._submit(spans)
                # eventlet.spawn(self._submit, spans)
                for _ in spans:
                    self.queue.task_done()
                print(self.queue)
                spans = spans[:0]
                eventlet.sleep()
            self.metrics.reporter_queue_length(self.queue.qsize())
        self.logger.info('Span publisher exited')

    # method for protocol factory
    def getProtocol(self, transport):
        """
        Implements Thrift ProtocolFactory interface
        :param: transport:
        :return: Thrift compact protocol
        """
        return TCompactProtocol.TCompactProtocol(transport)

    def _submit(self, spans):
        print("submit")
        print(spans)
        if not spans:
            return
        try:
            batch = thrift.make_jaeger_batch(spans=spans, process=self._process)
            self._send(batch)
            if self.queue.empty():
                print("send over")
            self.metrics.reporter_success(len(spans))
        except socket.error as e:
            self.metrics.reporter_failure(len(spans))
            self.error_reporter.error(
                'Failed to submit traces to jaeger-agent socket: %s', e)
        except Exception as e:
            self.metrics.reporter_failure(len(spans))
            self.error_reporter.error(
                'Failed to submit traces to jaeger-agent: %s', e)

    def _send(self, batch):
        """
        Send batch of spans out via thrift transport. Any exceptions thrown
        will be caught above in the exception handler of _submit().
        """
        print("sending")
        return self.agent.emitBatch(batch)

    def close(self):
        """
        Ensure that all spans from the queue are submitted.
        Returns Future that will be completed once the queue is empty.
        """
        print("closing")
        self._flush()
        # return ioloop_util.submit(self._flush, io_loop=self.io_loop)
        import asyncio
        return asyncio.Future()

    def _flush(self):
        # stopping here ensures we don't lose spans from pending _report_span_from_ioloop callbacks
        with self.stop_lock:
            self.stopped = True
        self.queue.put(self.stop)
        self.queue.join()
        self._consume_queue()
