from __future__ import absolute_import

import logging

import cqlengine
from cqlengine import management

from cassandra import connection
from cassandra.cluster import NoHostAvailable

from celery.signals import worker_process_init

from scrapi import settings


logger = logging.getLogger(__name__)


try:
    cqlengine.connection.setup(settings.CASSANDRA_URI, settings.CASSANDRA_KEYSPACE)
    management.create_keyspace(settings.CASSANDRA_KEYSPACE, replication_factor=1, strategy_class='SimpleStrategy')
except NoHostAvailable:
    logger.error('Could not connect to Cassandra, expect errors.')


def cassandra_init(*args, **kwargs):
    if connection.cluster is not None:
        connection.cluster.shutdown()
    if connection.session is not None:
        connection.session.shutdown()
    connection.setup(settings.CASSANDRA_URI, settings.CASSANDRA_KEYSPACE)

worker_process_init.connect(cassandra_init)
