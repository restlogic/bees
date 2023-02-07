from __future__ import absolute_import

from datetime import datetime
from unittest import TestCase

from opentracing import global_tracer

from bees.initializer import init_from_conf


class TestInitializer(TestCase):

    def test_init_from_conf(self):
        now = datetime.now()
        name = 'test-profiler ' + now.strftime('%H:%M:%S')
        tracer = init_from_conf(conf='../config.yaml', service=name)
        assert tracer == global_tracer()
