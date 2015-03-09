import mock
import pytest

from scrapi import settings
from scrapi import database

database._manager = database.DatabaseManager(keyspace='test')


settings.DEBUG = True
settings.CELERY_ALWAYS_EAGER = True
settings.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True


@pytest.fixture(autouse=True)
def harvester(monkeypatch):
    import_mock = mock.MagicMock()
    harvester_mock = mock.MagicMock()

    import_mock.return_value = harvester_mock

    monkeypatch.setattr('scrapi.util.import_harvester', import_mock)
    monkeypatch.setattr('scrapi.tasks.import_harvester', import_mock)

    return harvester_mock


@pytest.fixture(autouse=True)
def timestamp_patch(monkeypatch):
    monkeypatch.setattr('scrapi.util.timestamp', lambda: 'TIME')
    monkeypatch.setattr('scrapi.tasks.timestamp', lambda: 'TIME')


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'cassandra: Handles setup and teardown for tests using cassandra'
    )


def pytest_runtest_setup(item):
    marker = item.get_marker('cassandra')
    if marker is not None:
        if not database.setup():
            pytest.skip('No connection to Cassandra')


def pytest_runtest_teardown(item, nextitem):
    marker = item.get_marker('cassandra')
    if marker is not None:
        database.tear_down(force=True)
