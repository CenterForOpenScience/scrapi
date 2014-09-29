import os
import logging
from datetime import datetime

from celery import Celery

from scrapi_tools import RawDocument

from scrapi import settings
from scrapi.util.storage import store
from scrapi.util import import_consumer

app = Celery()
app.config_from_object(settings)

logger = logging.getLogger(__name__)


@app.task
def run_consumer(consumer_name):
    logger.info('Runing consumer "{}"'.format(consumer_name))
    # Form and start a celery chain
    chain = (consume.si(consumer_name) | begin_normalization.s(consumer_name))
    chain.apply_async()


@app.task
def consume(consumer_name):
    logger.info('Consumer "{}" has begun consumption'.format(consumer_name))

    consumer = import_consumer(consumer_name)
    result = consumer.consume()

    logger.info('Consumer "{}" has finished consumption'.format(consumer_name))

    return result


@app.task
def begin_normalization(raw_docs, consumer_name):
    logger.info('Normalizing {} documents for consumer "{}"'
                .format(len(raw_docs), consumer_name))

    for raw in raw_docs:
        timestamp = datetime.now().isoformat()

        process_raw.si(raw, timestamp).apply_async()

        chain = (normalize.si(raw, timestamp, consumer_name) |
                 process_normalized.s(raw))

        chain.apply_async()


@app.task
def process_raw(raw_doc, timestamp):
    raw_doc.attributes['timestamp'] = timestamp
    store.store_raw(raw_doc)
    # This is where the raw_doc should be dumped to disc
    # And anything else that may need to happen to it


@app.task
def normalize(raw_doc, timestamp, consumer_name):
    consumer = import_consumer(consumer_name)
    normalized = consumer.normalize(raw_doc, timestamp)
    logger.info('Document {} normalized sucessfully'.format(raw_doc.get('doc_id')))

    # Not useful if using just the osf but may need to be included for
    # A standalone scrapi
    # normalized.attributes['location'] = 'TODO'
    normalized.attributes['timestamp'] = timestamp

    return normalized


@app.task
def process_normalized(normalized_doc, raw_doc):
    raise Exception()
    store.store_normalized(raw_doc, normalized_doc)
    # This is where the normalized doc should be dumped to disc
    # And then sent to OSF
    # And anything that may need to occur


@app.task
def check_archives(reprocess):
    for consumer in settings.MANIFESTS.keys():
        check_archive(consumer, reprocess).apply_async()


@app.task
def check_archive(consumer_name, reprocess):
    # TODO Remote filestorage stuff @fabianvf
    consumer = settings.MANIFESTS[consumer_name]

    for raw_path in store.iter_raws(consumer_name, include_normalized=reprocess):
        timestamp = raw_path.split('/')[-2], '%Y-%m-%d %H:%M:%S.%f'
        timestamp = datetime.datetime.strptime(timestamp)

        raw_file = store.get_as_string(raw_path)

        raw_doc = RawDocument({
            'doc': raw_file.read(),
            'doc_id': raw_path.split('/')[-3],
            'source': consumer_name,
            'filetype': consumer['file_format']
        })

        normalize.si(raw_doc, timestamp, consumer_name).apply_async()


@app.task
def tar_archive():
    os.system('tar -czvf website/static/archive.tar.gz archive/')
