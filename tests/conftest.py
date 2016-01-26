import time

import mock
import pytest

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError, TransportError

from cassandra.cluster import NoHostAvailable


import scrapi
from scrapi import settings
from scrapi.processing.cassandra import CassandraProcessor, DatabaseManager


settings.DEBUG = True
settings.CELERY_ALWAYS_EAGER = True
settings.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
CassandraProcessor.manager = DatabaseManager(keyspace='test')
database  = CassandraProcessor.manager

try:
    con = Elasticsearch(settings.ELASTIC_URI, request_timeout=settings.ELASTIC_TIMEOUT)
    con.cluster.health(wait_for_status='yellow')
    use_es = True
except ConnectionError:
    use_es = False


@pytest.fixture(autouse=True)
def harvester(monkeypatch):
    mock_registry = mock.MagicMock()
    mock_harvester = mock.MagicMock()
    mock_registry.__getitem__.return_value = mock_harvester

    monkeypatch.setattr('scrapi.tasks.registry', mock_registry)

    return mock_harvester


@pytest.fixture(autouse=True)
def timestamp_patch(monkeypatch):
    monkeypatch.setattr('scrapi.tasks.timestamp', lambda: 'TIME')


def pytest_configure(config):
    config.addinivalue_line(
        'markers',
        'cassandra: Handles setup and teardown for tests using cassandra'
    )
    config.addinivalue_line(
        'markers',
        'elasticsearch: tests that rely on an elasticsearch connection'
    )


def pytest_runtest_setup(item):
    TIMEOUT = 20

    marker = item.get_marker('cassandra')
    if marker is not None:
        from scrapi.processing.cassandra import DocumentModel
        if not database.setup():
            pytest.skip('No connection to Cassandra')

        start = time.time()
        while True:
            try:
                DocumentModel.all().limit(1).get()
                break
            except NoHostAvailable as e:
                now = time.time()
                if (now - start) > TIMEOUT:
                    raise e
                continue
            except Exception:
                break


    marker = item.get_marker('elasticsearch')
    if marker is not None:
        if not use_es:
            pytest.skip('No connection to Elasticsearch')
        con.indices.create(index='test', body={}, ignore=400)

        # This is done to let the test index finish being created before connecting to search
        start = time.time()
        while True:
            try:
                scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es.search(index='test')
                break
            except TransportError as e:
                now = time.time()
                if (now - start) > TIMEOUT:
                    raise e
                continue


def pytest_runtest_teardown(item, nextitem):
    marker = item.get_marker('cassandra')
    if marker is not None:
        database.clear(force=True)

    marker = item.get_marker('elasticsearch')
    if marker is not None:
        if not use_es:
            pytest.skip('No connection to Elasticsearch')
        con.indices.delete(index='test', ignore=400)
