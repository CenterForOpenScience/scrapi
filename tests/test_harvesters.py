import logging

import vcr
import pytest
from freezegun import freeze_time

from scrapi import base
from scrapi import registry, requests
from scrapi.base.helpers import compose

logger = logging.getLogger(__name__)


@freeze_time("2007-12-21")
#@pytest.mark.parametrize('harvester_name', filter(lambda x: x != 'test', sorted(map(str, registry.keys()))))
def test_harvester(monkeypatch, harvester_name='lwbin', *args, **kwargs):
    monkeypatch.setattr(requests.time, 'sleep', lambda *_, **__: None)
    base.settings.RAISE_IN_TRANSFORMER = True

    harvester = registry[harvester_name]

    with vcr.use_cassette('tests/vcr/{}.yaml'.format(harvester_name), match_on=['host'], record_mode='once'):
        harvested = harvester.harvest()
        assert len(harvested) > 0

    normalized = list(filter(lambda x: x is not None, map(harvester.normalize, harvested[:25])))
    assert len(normalized) > 0
