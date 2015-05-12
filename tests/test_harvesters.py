import logging

import vcr
import pytest
from freezegun import freeze_time

from scrapi import registry, requests

logger = logging.getLogger(__name__)


@freeze_time("2007-12-21")
@pytest.mark.parametrize('harvester_name', sorted(map(str, registry.keys())))
def test_harvester(monkeypatch, harvester_name, *args, **kwargs):
    monkeypatch.setattr(requests.time, 'sleep', lambda *_, **__: None)

    with vcr.use_cassette('tests/vcr/{}.yaml'.format(harvester_name), match_on=['host'], record_mode='none'):
        harvester = registry[harvester_name]
        try:
            normalized = [harvester.normalize(doc) for doc in harvester.harvest()]
        except Exception as e:
            logger.exception(e)
            assert False
    assert len(normalized) > 0
