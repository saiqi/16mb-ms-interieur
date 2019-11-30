import pytest
import vcr
import itertools

from nameko.testing.services import worker_factory
from application.dependencies.elections import ElectionURLBundle
from application.services.election_collector import ElectionCollectorService

class TestElectionURLBundle(ElectionURLBundle):

    def results(self, election_id):
        return itertools.islice(super().results(election_id), 2)

@pytest.fixture
def election():
    return TestElectionURLBundle()

@vcr.use_cassette('application/tests/vcr_cassettes/publish.yaml')
def test_publish(election):
    service = worker_factory(ElectionCollectorService, election=election)
    service.publish('ER2019')