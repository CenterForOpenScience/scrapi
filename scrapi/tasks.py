import logging

import requests
from celery import Celery

from scrapi import util
from scrapi import events
from scrapi import settings
from scrapi import processing
from scrapi.util import timestamp
from scrapi.util import import_consumer


app = Celery()
app.config_from_object(settings)

logger = logging.getLogger(__name__)


@app.task
@events.creates_task(events.CONSUMER_RUN)
def run_consumer(consumer_name, days_back=1):
    logger.info('Running consumer "{}"'.format(consumer_name))

    normalization = begin_normalization.s(consumer_name)
    start_consumption = consume.si(consumer_name, timestamp(), days_back=days_back)

    # Form and start a celery chain
    (start_consumption | normalization).apply_async()


@app.task
@events.logged(events.CONSUMER_RUN)
def consume(consumer_name, job_created, days_back=1):
    consume_started = timestamp()
    consumer = import_consumer(consumer_name)

    with util.maybe_recorded(consumer_name):
        result = consumer.consume(days_back=days_back)

    # result is a list of all of the RawDocuments consumed
    return result, {
        'consumeFinished': timestamp(),
        'consumeTaskCreated': job_created,
        'consumeStarted': consume_started,
    }


@app.task
def begin_normalization((raw_docs, timestamps), consumer_name):
    '''consume_ret is consume return value:
        a tuple contaiing list of rawDocuments and
        a dictionary of timestamps
    '''

    logger.info('Normalizing {} documents for consumer "{}"'
                .format(len(raw_docs), consumer_name))
    # raw is a single raw document
    for raw in raw_docs:
        spawn_tasks(raw, timestamps, consumer_name)


@events.creates_task(events.PROCESSING)
@events.creates_task(events.NORMALIZATION)
def spawn_tasks(raw, timestamps, consumer_name):
        raw['timestamps'] = timestamps
        raw['timestamps']['normalizeTaskCreated'] = timestamp()
        chain = (normalize.si(raw, consumer_name) | process_normalized.s(raw))

        chain.apply_async()
        process_raw.delay(raw)


@app.task
@events.logged(events.PROCESSING, 'raw')
def process_raw(raw_doc, **kwargs):
    processing.process_raw(raw_doc, kwargs)


@app.task
@events.logged(events.NORMALIZATION)
def normalize(raw_doc, consumer_name):
    normalized_started = timestamp()
    consumer = import_consumer(consumer_name)

    with events.logged_failure(events.NORMALIZATION):
        normalized = consumer.normalize(raw_doc)

    if not normalized:
        raise events.Skip('Did not normalize document with id {}'.format(raw_doc['docID']))

    normalized['timestamps'] = util.stamp_from_raw(raw_doc, normalizeStarted=normalized_started)
    normalized['raw'] = util.build_raw_url(raw_doc, normalized)

    return normalized  # returns a single normalized document


@app.task
@events.logged(events.PROCESSING, 'normalized')
def process_normalized(normalized_doc, raw_doc, **kwargs):
    if not normalized_doc:
        raise events.Skip('Not processing document with id {}'.format(raw_doc['docID']))
    processing.process_normalized(raw_doc, normalized_doc, kwargs)


@app.task
def update_pubsubhubbub():
    payload = {'hub.mode': 'publish', 'hub.url': '{url}rss/'.format(url=settings.OSF_APP_URL)}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    return requests.post('https://pubsubhubbub.appspot.com', headers=headers, params=payload)
