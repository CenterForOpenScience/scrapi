import os
import logging
import importlib
from datetime import datetime

from celery import Celery

import settings


app = Celery()
app.config_from_object(settings)

logger = logging.getLogger(__name__)


def import_consumer(consumer_name):
    return importlib.import_module('scrapi.consumers.{}'.format(consumer_name))


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
def normalize(raw_doc, timestamp, consumer_name):
    consumer = import_consumer(consumer_name)
    normalized = consumer.normalize(raw_doc, timestamp)
    # Do other things here
    return normalized


@app.task
def process_raw(raw_doc, timestamp):
    pass


@app.task
def process_normalized(normalized_doc):
    pass


@app.task
def check_archive():
    pass


@app.task
def tar_archive():
    os.system('tar -czvf website/static/archive.tar.gz archive/')
