from __future__ import absolute_import

import logging

import cqlengine
from cqlengine import management

from cassandra import connection
from cassandra.cluster import NoHostAvailable

from celery.signals import worker_process_init

from scrapi import settings


logger = logging.getLogger(__name__)


class DatabaseManager(object):
    def __init__(self, uri=None, keyspace=None):
        self._models = []
        self._setup = False

        self.uri = uri or settings.CASSANDRA_URI
        self.keyspace = keyspace or settings.CASSANDRA_KEYSPACE

    def setup(self, force=False, throw=False):
        if self._setup and not force:
            return True

        try:
            cqlengine.connection.setup(self.uri, self.keyspace)
            management.create_keyspace(self.keyspace, replication_factor=1, strategy_class='SimpleStrategy')
            for model in self._models:
                management.sync_table(model)
        except NoHostAvailable:
            logger.error('Could not connect to Cassandra, expect errors.')
            return False

        # Note: return values are for test skipping
        self._setup = True
        return True

    def tear_down(self, force=False):
        if not self._setup:
            logger.warning('Attempting to tear down a database that was never setup')

        assert force, 'tear_down must be called with force=True'

        management.delete_keyspace(self.keyspace)
        self._setup = False

    def register_model(self, model):
        if not self._setup:
            self.setup()

        self._models.append(model)
        management.sync_table(model)
        return model


def cassandra_init(*args, **kwargs):
    if connection.cluster is not None:
        connection.cluster.shutdown()
    if connection.session is not None:
        connection.session.shutdown()
    connection.setup(settings.CASSANDRA_URI, settings.CASSANDRA_KEYSPACE)


_manager = DatabaseManager()

setup = _manager.setup
tear_down = _manager.tear_down
register_model = _manager.register_model

worker_process_init.connect(cassandra_init)
