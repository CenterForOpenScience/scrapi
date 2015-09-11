import logging

import vcr
import pytest

from scrapi import base
from scrapi import settings
from scrapi import registry, requests

logger = logging.getLogger(__name__)


@pytest.mark.parametrize('harvester_name', filter(lambda x: x != 'test', sorted(map(str, registry.keys()))))
def test_harvester(monkeypatch, harvester_name, *args, **kwargs):
    monkeypatch.setattr(requests.time, 'sleep', lambda *_, **__: None)
    base.settings.RAISE_IN_TRANSFORMER = True
    base.settings.RECORD_HTTP_TRANSACTIONS = False

    harvester = registry[harvester_name]

    with vcr.use_cassette('tests/vcr/{}.yaml'.format(harvester_name), match_on=['path'], record_mode=settings.TEST_RECORD_MODE):
        harvested = harvester.harvest()
        assert len(harvested) > 0

    normalized = list(filter(lambda x: x is not None, map(harvester.normalize, harvested[:25])))
    assert len(normalized) > 0
