# from __future__ import absolute_import

# import os
# os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import mock
import pytest

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

# from django.db import connection
# from django.db.utils import DatabaseError

from psycopg2 import connect
from psycopg2 import DatabaseError
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

from scrapi import settings
from scrapi import database


settings.DEBUG = True
settings.CELERY_ALWAYS_EAGER = True
settings.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True
database._manager.keyspace = 'test'

postgres_exc = None

try:
    con = Elasticsearch(settings.ELASTIC_URI, request_timeout=settings.ELASTIC_TIMEOUT)
    con.cluster.health(wait_for_status='yellow')
    use_es = True
except ConnectionError:
    use_es = False

try:
    # Need to create a test database
    postgres_con = connect(dbname='postgres', host='localhost')
    postgres_con.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = postgres_con.cursor()
    cur.close()
    postgres_con.close()
    use_pg = True
except DatabaseError as e:
    postgres_exc = e
    use_pg = False


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
    config.addinivalue_line(
        'markers',
        'postgres: tests that rely on a postgres connection'
    )


def pytest_runtest_setup(item):
    marker = item.get_marker('cassandra')
    if marker is not None:
        if not database.setup():
            pytest.skip('No connection to Cassandra')

    marker = item.get_marker('elasticsearch')
    if marker is not None:
        if not use_es:
            pytest.skip('No connection to Elasticsearch')

    marker = item.get_marker('postgres')
    if marker is not None:
        if use_pg:
            global cursor
            cursor = postgres_con.cursor()
            cursor.execute("CREATE DATABASE test")
        else:
            raise postgres_exc
            pytest.skip(postgres_exc)


def pytest_runtest_teardown(item, nextitem):
    marker = item.get_marker('cassandra')
    if marker is not None:
        database._manager.clear_keyspace(force=True)

    marker = item.get_marker('postgres')
    if marker is not None:
        if use_pg:
            cur.execute('DROP DATABASE test')
            cur.close()
