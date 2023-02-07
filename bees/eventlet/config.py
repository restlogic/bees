# -*- coding: utf-8 -*-

from __future__ import absolute_import

from jaeger_client import Config
from jaeger_client.config import (
    CompositeReporter,
    LoggingReporter,
    logger
)

from .remote_controlled_sampler import BeesRemoteControlledSampler
from .reporter import BeesReporter


class BeesConfig(Config):

    def __init__(self, config, metrics=None, service_name=None, metrics_factory=None,
                 validate=False, scope_manager=None):
        """
        :param metrics: an instance of Metrics class, or None. This parameter
            has been deprecated, please use metrics_factory instead.
        :param service_name: default service name.
            Can be overwritten by config['service_name'].
        :param metrics_factory: an instance of MetricsFactory class, or None.
        :param scope_manager: an instance of a scope manager, or None for
            default (ThreadLocalScopeManager).
        """
        super(BeesConfig, self).__init__(config, metrics=metrics, service_name=service_name,
                                         metrics_factory=metrics_factory, validate=validate,
                                         scope_manager=scope_manager)

    def new_tracer(self, io_loop=None):
        """
        Create a new Jaeger Tracer based on the passed `jaeger_client.Config`.
        Does not set `opentracing.tracer` global variable.
        """
        channel = self._create_local_agent_channel(io_loop=io_loop)
        sampler = self.sampler
        if not sampler:
            sampler = BeesRemoteControlledSampler(
                channel=channel,
                service_name=self.service_name,
                logger=logger,
                metrics_factory=self._metrics_factory,
                error_reporter=self.error_reporter,
                sampling_refresh_interval=self.sampling_refresh_interval,
                max_operations=self.max_operations)
        logger.info('Using sampler %s', sampler)

        reporter = BeesReporter( 
            channel=channel,
            queue_capacity=self.reporter_queue_size,
            batch_size=self.reporter_batch_size,
            flush_interval=self.reporter_flush_interval,
            logger=logger,
            metrics_factory=self._metrics_factory,
            error_reporter=self.error_reporter)

        if self.logging:
            reporter = CompositeReporter(reporter, LoggingReporter(logger))

        return self.create_tracer(
            reporter=reporter,
            sampler=sampler,
        )
