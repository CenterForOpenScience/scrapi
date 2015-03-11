import pytest

from celery.schedules import crontab

from scrapi import _Registry
from scrapi.base import BaseHarvester
from scrapi.base import HarvesterMeta


@pytest.fixture
def mock_registry(monkeypatch):
    registry = _Registry()
    monkeypatch.setattr('scrapi.base.registry', registry)
    return registry


@pytest.fixture
def test_harvester():
    class TestHarvester(BaseHarvester):
        short_name = 'test'
        long_name = 'test'
        file_format = 'test'
        harvest = lambda x: x
        normalize = lambda x: x
    return TestHarvester


class TestHarvesterMeta(object):

    def test_meta_records(self, mock_registry):

        class TestClass(object):
            __metaclass__ = HarvesterMeta
            short_name = 'test'

        assert isinstance(mock_registry['test'], TestClass)

    def test_beat_schedule(self, mock_registry):
        assert mock_registry.beat_schedule == {}

    def test_beat_schedule_adds(self, mock_registry):
        class TestClass(object):
            __metaclass__ = HarvesterMeta
            short_name = 'test'
            run_at = {
                'hour': 1,
                'minute': 1,
                'day_of_week': 'mon',
            }

        assert mock_registry.beat_schedule == {
            'run_test': {
                'args': ['test'],
                'task': 'scrapi.tasks.run_harvester',
                'schedule': crontab(**TestClass.run_at),
            }
        }


class TestHarvesterBase(object):

    def test_requires_short_name(self, monkeypatch, test_harvester):
        # monkeypatch.delattr(test_harvester, 'short_name')
        import ipdb; ipdb.set_trace()
        test_harvester()

        # assert 'short_name' in e.value.message

    def test_requires_long_name(self):

        class TestHarvester(BaseHarvester):
            short_name = 'test'
            long_name = 'test'
            file_format = 'test'
            harvest = lambda x: x
            normalize = lambda x: x

        with pytest.raises(TypeError) as e:
            TestHarvester()

        assert 'short_name' in e.value.message
