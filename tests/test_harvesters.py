import logging

import pytest
from freezegun import freeze_time

from scrapi import registry

logger = logging.getLogger(__name__)

@freeze_time("2015-04-23")
@pytest.mark.parametrize('harvester_name', map(str, registry.keys()))
def test_harvester(harvester_name):
    harvester = registry[harvester_name]
    try:
        [harvester.normalize(doc) for doc in harvester.harvest()]
        assert True
    except Exception as e:
        logger.exception(e)
        assert False
