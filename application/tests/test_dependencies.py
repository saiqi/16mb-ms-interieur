import eventlet
eventlet.monkey_patch()

import vcr
from nameko.testing.services import dummy, entrypoint_hook
from application.dependencies.elections import Election


class DummyService(object):
    name = 'dummy_service'
    election = Election()

    @dummy
    def get_results(self, election_id):
        return self.election.results(election_id)


@vcr.use_cassette('application/tests/vcr_cassettes/a_result.yaml')
def test_end_to_end(container_factory):
    container = container_factory(DummyService, {})
    container.start()

    with entrypoint_hook(container, 'get_results') as get_results:
        results = get_results('ER2019')
        result = next(results)
        assert result.feed_id == 'ER2019_975com'
        doc = result.call()
        assert 'Election' in doc