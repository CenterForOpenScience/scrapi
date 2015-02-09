import logging

from dateutil import parser

from base64 import b64decode

from celery import Celery
from celery.signals import worker_process_init

from cqlengine import connection
from cqlengine.connection import cluster, session

from scrapi import settings
from scrapi.linter import RawDocument
from scrapi.tasks import normalize

from scrapi.util.storage import store

from scrapi.processing.cassandra import CassandraProcessor

logger = logging.getLogger(__name__)


def cassandra_init(*args, **kwargs):
    if cluster is not None:
        cluster.shutdown()
    if session is not None:
        session.shutdown()
    connection.setup(settings.CASSANDRA_URI, settings.CASSANDRA_KEYSPACE)

worker_process_init.connect(cassandra_init)

app = Celery()
app.config_from_object(settings)

cass = CassandraProcessor()


@app.task
def process_one_to_cassandra(consumer_name, consumer, raw_path):
    try:
        date = parser.parse(raw_path.split('/')[-2])

        timestamp = date.isoformat()

        raw_file = store.get_as_string(raw_path)

        raw_doc = RawDocument({
            'doc': raw_file,
            'timestamps': {
                'consumeFinished': timestamp
            },
            'docID': b64decode(raw_path.split('/')[-3]).decode('utf-8'),
            'source': consumer_name,
            'filetype': consumer['fileFormat'],
        })

        cass.process_raw(raw_doc)

        normalized = normalize(raw_doc, consumer_name)

        cass.process_normalized(raw_doc, normalized)
    except Exception as e:
        logger.exception(e)
        return e
