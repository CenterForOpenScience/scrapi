from __future__ import absolute_import

import logging

from cqlengine import connection
from cqlengine import management
from cassandra.cluster import NoHostAvailable

from celery.signals import worker_process_init
from celery.signals import worker_process_shutdown

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
            connection.setup(self.uri, self.keyspace)
            management.create_keyspace(self.keyspace, replication_factor=1, strategy_class='SimpleStrategy')
            for model in self._models:
                management.sync_table(model)
        except NoHostAvailable:
            logger.error('Could not connect to Cassandra, expect errors.')
            return False

        # Note: return values are for test skipping
        self._setup = True
        return True

    def tear_down(self):
        if not self._setup:
            logger.warning('Attempting to tear down a database that was never setup')

        if connection.cluster is not None:
            connection.cluster.shutdown()
        if connection.session is not None:
            connection.session.shutdown()

        self._setup = False

    def clear_keyspace(self, force=False):
        assert force, 'clear_keyspace must be called with force'
        management.delete_keyspace(self.keyspace)
        self.tear_down()
        return self.setup()

    def register_model(self, model):
        model. __keyspace__ = self.keyspace
        self._models.append(model)
        if self._setup:
            management.sync_table(model)
        return model

    def celery_setup(self, *args, **kwargs):
        self.setup()

    def celery_teardown(self, *arg, **kwargs):
        self.tear_down()


_manager = DatabaseManager()

setup = _manager.setup
tear_down = _manager.tear_down
register_model = _manager.register_model
worker_process_init.connect(_manager.celery_setup)
worker_process_shutdown.connect(_manager.celery_teardown)
