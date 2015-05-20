import pytest

from celery.schedules import crontab

from scrapi import registry
from scrapi.base import BaseHarvester
from scrapi.base import HarvesterMeta


@pytest.fixture
def mock_registry(monkeypatch):
    return registry


class TestHarvesterMeta(object):

    def test_meta_records(self, mock_registry):

        class TestClass(object):
            __metaclass__ = HarvesterMeta
            long_name = 'test'
            short_name = 'test'
            url = 'test'
            run_at = {}

        assert isinstance(mock_registry['test'], TestClass)

    def test_beat_schedule(self, mock_registry):
        for key, val in mock_registry.items():
            assert(val.short_name)
            assert(val.long_name)
            assert(val.url)
            assert(isinstance(val.run_at, dict))

    def test_beat_schedule_adds(self, mock_registry):
        class TestClass(object, metaclass=HarvesterMeta):
            short_name = 'test'
            run_at = {
                'hour': 1,
                'minute': 1,
                'day_of_week': 'mon',
            }

        assert mock_registry.beat_schedule['run_test'] == {
            'args': ['test'],
            'task': 'scrapi.tasks.run_harvester',
            'schedule': crontab(**TestClass.run_at),
        }

    def test_raises_key_error(self, mock_registry):
        with pytest.raises(KeyError) as e:
            mock_registry['FabianVF']
        assert 'No harvester named "FabianVF"' in str(e.value)


class TestHarvesterBase(object):
    ERR_MSG = 'Can\'t instantiate abstract class TestHarvester with abstract methods {}'

    def test_requires_short_name(self):
        with pytest.raises(TypeError) as e:
            class TestHarvester(BaseHarvester):
                long_name = 'test'
                file_format = 'test'
                url = 'test'
                harvest = lambda x: x
                normalize = lambda x: x

            TestHarvester()

        assert self.ERR_MSG.format('short_name') in str(e.value)

    def test_requires_long_name(self):
        with pytest.raises(TypeError) as e:
            class TestHarvester(BaseHarvester):
                short_name = 'test'
                file_format = 'test'
                url = 'test'
                harvest = lambda x: x
                normalize = lambda x: x

            TestHarvester()

        assert self.ERR_MSG.format('long_name') in str(e.value)

    def test_requires_url(self):
        with pytest.raises(TypeError) as e:
            class TestHarvester(BaseHarvester):
                short_name = 'test'
                long_name = 'test'
                file_format = 'test'
                harvest = lambda x: x
                normalize = lambda x: x

            TestHarvester()

        assert self.ERR_MSG.format('url') in str(e.value)

    def test_requires_file_format(self):
        with pytest.raises(TypeError) as e:
            class TestHarvester(BaseHarvester):
                long_name = 'test'
                short_name = 'test'
                url = 'test'
                harvest = lambda x: x
                normalize = lambda x: x

            TestHarvester()

        assert self.ERR_MSG.format('file_format') in str(e.value)

    def test_requires_harvest(self):
        with pytest.raises(TypeError) as e:
            class TestHarvester(BaseHarvester):
                long_name = 'test'
                short_name = 'test'
                url = 'test'
                file_format = 'test'
                normalize = lambda x: x

            TestHarvester()

        assert self.ERR_MSG.format('harvest') in str(e.value)

    def test_requires_normalize(self):
        with pytest.raises(TypeError) as e:
            class TestHarvester(BaseHarvester):
                long_name = 'test'
                short_name = 'test'
                url = 'test'
                file_format = 'test'
                harvest = lambda x: x

            TestHarvester()

        assert self.ERR_MSG.format('normalize') in str(e.value)
