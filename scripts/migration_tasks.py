import os
import json
import errno
import shutil
import logging

from dateutil import parser

from base64 import b64decode

from celery import Celery
from celery.signals import worker_process_init

from cqlengine import connection
from cqlengine.connection import cluster, session

from scrapi import settings
from scrapi import database  # noqa
from scrapi.tasks import normalize
from scrapi.linter import RawDocument, NormalizedDocument

from scrapi.util.storage import store

from scrapi.processing.cassandra import CassandraProcessor
from scrapi.processing.elastic_search import ElasticsearchProcessor

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
es = ElasticsearchProcessor()


@app.task
def process_one(harvester_name, harvester, raw_path):
    date = parser.parse(raw_path.split('/')[-2])

    timestamp = date.isoformat()

    raw_file = store.get_as_string(raw_path)

    raw_doc = RawDocument({
        'doc': raw_file,
        'timestamps': {
            'harvestFinished': timestamp
        },
        'docID': b64decode(raw_path.split('/')[-3]).decode('utf-8'),
        'source': harvester_name,
        'filetype': harvester['fileFormat'],
    })

    try:
        raw_list = raw_path.split('/')
        raw_list[-1] = 'normalized.json'
        normalized_path = '/'.join(raw_list)
        with open(normalized_path, 'r') as f:
            normalized = NormalizedDocument(json.load(f))
    except Exception:
        normalized = normalize(raw_doc, harvester_name)

    (
        process_to_cassandra.si(raw_doc, normalized) |
        process_to_elasticsearch.si(raw_doc, normalized) |
        move_to_backup.si(raw_path)
    ).apply_async()


@app.task
def process_to_cassandra(raw_doc, normalized):
    cass.process_raw(raw_doc)
    if normalized:
        cass.process_normalized(raw_doc, normalized)


@app.task
def process_to_elasticsearch(raw_doc, normalized):
    if normalized:
        es.process_normalized(raw_doc, normalized)


def mkdir_p(path):
    path = os.path.split(path)[0]
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno != errno.EEXIST or not os.path.isdir(path):
            raise


@app.task
def move_to_backup(raw_path):
    normalized_path = '/'.join(raw_path.split('/')[:-1] + ['normalized.json'])

    finished_raw_path = raw_path.replace(
        settings.ARCHIVE_DIRECTORY,
        os.path.join(settings.ARCHIVE_DIRECTORY, 'migrationfinished/')
    )
    finished_normalized_path = raw_path.replace(
        settings.ARCHIVE_DIRECTORY,
        os.path.join(settings.ARCHIVE_DIRECTORY, 'migrationfinished/')
    )

    mkdir_p(finished_raw_path)
    mkdir_p(finished_normalized_path)

    shutil.move(raw_path, finished_raw_path)

    try:
        shutil.move(normalized_path, finished_normalized_path)
    except IOError:
        pass  # No normalized file to be found
