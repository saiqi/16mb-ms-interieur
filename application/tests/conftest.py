import eventlet
eventlet.monkey_patch()

import pytest
from nameko.containers import ServiceContainer

@pytest.yield_fixture
def container_factory():

    all_containers = []

    def make_container(service_cls, config):
        container = ServiceContainer(service_cls, config)
        all_containers.append(container)
        return container

    yield make_container

    for c in all_containers:
        try:
            c.stop()
        except:
            pass