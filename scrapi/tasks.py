import os
import logging
from dateutil import parser
from datetime import datetime

from celery import Celery

from scrapi import settings
from scrapi import processing
from scrapi.util import import_consumer
from scrapi.util.storage import store
from scrapi.linter.document import RawDocument


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
        raw.attributes['timestamp'] = timestamp

        process_raw.delay(raw)

        chain = (normalize.si(raw, timestamp, consumer_name) |
                 process_normalized.s(raw))

        chain.apply_async()


@app.task
def process_raw(raw_doc):
    processing.process_raw(raw_doc)
    # This is where the raw_doc should be dumped to disc
    # And anything else that may need to happen to it


@app.task
def normalize(raw_doc, timestamp, consumer_name):
    consumer = import_consumer(consumer_name)

    normalized = consumer.normalize(raw_doc, timestamp)

    logger.debug('Document {}/{} normalized sucessfully'.format(
        consumer_name, raw_doc['docID']))

    # Not useful if using just the osf but may need to be included for
    # A standalone scrapi
    # normalized.attributes['location'] = 'TODO'
    normalized.attributes['timestamp'] = timestamp

    return normalized


@app.task
def process_normalized(normalized_doc, raw_doc, **kwargs):
    if not normalized_doc:
        logger.warning('Not processing document with id {}'.format(raw_doc['docID']))
        return

    processing.process_normalized(raw_doc, normalized_doc, kwargs)
    # This is where the normalized doc should be dumped to disc
    # And then sent to OSF
    # And anything that may need to occur


@app.task
def check_archives(reprocess):
    for consumer in settings.MANIFESTS.keys():
        check_archive.delay(consumer, reprocess)


@app.task
def check_archive(consumer_name, reprocess):
    consumer = settings.MANIFESTS[consumer_name]
    extras = {
        'overwrite': True
    }
    for raw_path in store.iter_raws(consumer_name, include_normalized=reprocess):
        timestamp = parser.parse(raw_path.split('/')[-2]).isoformat()
        raw_file = store.get_as_string(raw_path)

        raw_doc = RawDocument({
            'doc': raw_file,
            'timestamp': timestamp,
            'docID': raw_path.split('/')[-3],
            'source': consumer_name,
            'filetype': consumer['fileFormat'],
        })

        chain = (normalize.si(raw_doc, timestamp, consumer_name) |
                 process_normalized.s(raw_doc, storage=extras))
        chain.apply_async()


@app.task
def tar_archive():
    os.system('tar -czvf website/static/archive.tar.gz archive/')
