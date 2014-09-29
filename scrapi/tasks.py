import os
import logging
from datetime import datetime

from celery import Celery

from scrapi import settings


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
def begin_normalization(raw_docs, consumer_name):
    logger.info('Normalizing {} documents for consumer "{}"'
                .format(len(raw_docs), consumer_name))

    for raw in raw_docs:
        timestamp = datetime.now()

        process_raw.si(raw, timestamp).apply_async()

        chain = (normalize.si(raw, timestamp, consumer_name) | process_normalized.s())
        chain.apply_async()


@app.task
def consume(consumer_name):
    logger.info('Consumer "{}" has begun consumption'.format(consumer_name))

    consumer = import_consumer(consumer_name)
    result = consumer.consume()

    logger.info('Consumer "{}" has finished consumption'.format(consumer_name))

    return result


@app.task
def process_raw(raw_doc, timestamp):
    # This is where the raw_doc should be dumped to disc
    # And anything else that may need to happen to it
    pass


@app.task
def normalize(raw_doc, timestamp, consumer_name):
    consumer = import_consumer(consumer_name)
    normalized = consumer.normalize(raw_doc, timestamp)
    logger.info('Document {} normalized sucessfully'.format(raw_doc.get('doc_id')))

    normalized.attributes['location'] = 'TODO'
    normalized.attributes['isoTimestamp'] = str(timestamp.isoformat())

    return normalized


@app.task
def process_normalized(normalized_doc):
    # This is where the normalized doc should be dumped to disc
    # And then sent to OSF
    # And anything that may need to occur
    pass


@app.task
def check_archives(reprocess):
    for consumer in settings.MANIFESTS.keys():
        check_archive(consumer, reprocess).apply_async()


@app.task
def check_archive(consumer_name, reprocess):
    pass  # TODO


@app.task
def tar_archive():
    os.system('tar -czvf website/static/archive.tar.gz archive/')
