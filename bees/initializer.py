from __future__ import absolute_import

import os

import yaml
from jaeger_client import Config

from .eventlet.config import BeesConfig
from .eventlet.scope_manager import EventletScopeManager

REPORTING_HOST = os.environ.get("REPORTING_HOST") or "127.0.0.1"
REPORTING_PORT = os.environ.get("REPORTING_PORT") or "6831"


def init_from_conf(service, conf=None, eventlet=False, eventlet_scope_manager=False):
    """ Initialize global tracer 

    :param service: trace service name
    :param conf:
    :param eventlet:
    :eventlet_scope_manager:

    """
    # with open(conf) as f:
    #     c = yaml.full_load(f)
    #
    # config = Config(config=c, service_name=service)

    if eventlet:
        if eventlet_scope_manager:
            config = BeesConfig(
                config={
                    'sampler': {
                        'type': 'const',
                        'param': 1,
                    },
                    'logging': True,
                    'reporter_batch_size': 1,
                    'local_agent': {
                        'reporting_host': REPORTING_HOST,
                        'reporting_port': REPORTING_PORT
                    }
                },
                service_name=service,
                scope_manager=EventletScopeManager(),
            )
        else:
            config = BeesConfig(
                config={
                    'sampler': {
                        'type': 'const',
                        'param': 1,
                    },
                    'logging': True,
                    'reporter_batch_size': 1,
                    'local_agent': {
                        'reporting_host': REPORTING_HOST,
                        'reporting_port': REPORTING_PORT
                    }
                },
                service_name=service,
            )
    else:
        config = Config(
            config={  # usually read from some yaml config
                'sampler': {
                    'type': 'const',
                    'param': 1,
                },
                'logging': True,
                'reporter_batch_size': 1,
                'local_agent': {
                    'reporting_host': REPORTING_HOST,
                    'reporting_port': REPORTING_PORT
                }
            },
            service_name=service,
        )

    return config.initialize_tracer()


if __name__ == '__main__':
    with open('./config.yaml') as f:
        c = yaml.full_load(f)
        print(c)
