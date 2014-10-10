import os
import logging
from dateutil import parser
from base64 import b64decode

import vcr

import requests

from celery import Celery

from scrapi import events
from scrapi import settings
from scrapi import processing
from scrapi.util import timestamp
from scrapi.util import import_consumer
from scrapi.util.storage import store
from scrapi.linter.document import RawDocument


app = Celery()
app.config_from_object(settings)

logger = logging.getLogger(__name__)


@app.task
def run_consumer(consumer_name, days_back=1):
    logger.info('Running consumer "{}"'.format(consumer_name))

    # Form and start a celery chain
    chain = (consume.si(consumer_name, timestamp(), days_back=days_back)
             | begin_normalization.s(consumer_name))

    chain.apply_async()

    # Note: Dispatch events only after they run

    events.dispatch(events.CONSUMER_RUN, events.CREATED, consumer=consumer_name)


@app.task
def consume(consumer_name, job_created, days_back=1):
    logger.info('Consumer "{}" has begun consumption'.format(consumer_name))
    events.dispatch(events.CONSUMER_RUN, events.STARTED, consumer=consumer_name)

    timestamps = {
        'consumeTaskCreated': job_created,
        'consumeStarted': timestamp()
    }

    consumer = import_consumer(consumer_name)

    try:
        if settings.STORE_HTTP_TRANSACTIONS:
            cassette = os.path.join(settings.RECORD_DIRECTORY,
                                    consumer_name, timestamp() + '.yml')

            logger.debug('Recording HTTP consumption request for {} to {}'
                         .format(consumer_name, cassette))

            with vcr.use_cassette(cassette, record_mode='all'):
                result = consumer.consume(days_back=days_back)
        else:
            result = consumer.consume(days_back=days_back)
    except Exception as e:
        events.dispatch(events.CONSUMER_RUN, events.FAILED,
                        consumer=consumer_name, exception=str(e))
        raise

    timestamps['consumeFinished'] = timestamp()

    logger.info('Consumer "{}" has finished consumption'.format(consumer_name))
    events.dispatch(events.CONSUMER_RUN, events.COMPLETED, consumer=consumer_name)

    return result, timestamps


@app.task
def begin_normalization(consume_ret, consumer_name):
    raw_docs, timestamps = consume_ret

    logger.info('Normalizing {} documents for consumer "{}"'
                .format(len(raw_docs), consumer_name))

    for raw in raw_docs:
        raw['timestamps'] = timestamps
        raw['timestamps']['normalizeTaskCreated'] = timestamp()

        logger.debug('Created the process raw task for {}/{}'
                     .format(consumer_name, raw['docID']))

        process_raw.delay(raw)

        chain = (normalize.si(raw, consumer_name) |
                 process_normalized.s(raw))

        logger.debug('Created the process normalized task for {}/{}'
                     .format(consumer_name, raw['docID']))

        chain.apply_async()

        # Note: Dispatch events only AFTER the event has actually happened

        events.dispatch(events.NORMALIZATION, events.CREATED,
                        consumer=consumer_name, docID=raw['docID'])

        events.dispatch(events.PROCESSING, events.CREATED,
                        consumer=consumer_name, docID=raw['docID'])


@app.task
def process_raw(raw_doc):
    events.dispatch(events.PROCESSING, events.STARTED,
                    _index='raw', docID=raw_doc['docID'])

    processing.process_raw(raw_doc)

    events.dispatch(events.PROCESSING, events.COMPLETED,
                    _index='raw', docID=raw_doc['docID'])


@app.task
def normalize(raw_doc, consumer_name):
    consumer = import_consumer(consumer_name)

    raw_doc['timestamps']['normalizeStarted'] = timestamp()

    logger.debug('Document {}/{} normalization began'.format(
        consumer_name, raw_doc['docID']))

    events.dispatch(events.NORMALIZATION, events.STARTED,
                    consumer=consumer_name, docID=raw_doc['docID'])
    try:
        normalized = consumer.normalize(raw_doc)
    except Exception as e:
        events.dispatch(events.NORMALIZATION, events.FAILED,
                consumer=consumer_name, docID=raw_doc['docID'], exception=str(e))
        raise

    if not normalized:
        events.dispatch(events.NORMALIZATION, events.SKIPPED, consumer=consumer_name, docID=raw_doc['docID'])
        logger.warning('Did not normalize document [{}]{}'.format(consumer_name, raw_doc['docID']))
        return None

    logger.debug('Document {}/{} normalized sucessfully'.format(
        consumer_name, raw_doc['docID']))

    events.dispatch(events.NORMALIZATION, events.COMPLETED,
                    consumer=consumer_name, docID=raw_doc['docID'])

    normalized['timestamps'] = raw_doc['timestamps']
    normalized['timestamps']['normalizeFinished'] = timestamp()
    return normalized


@app.task
def process_normalized(normalized_doc, raw_doc, **kwargs):
    if not normalized_doc:
        events.dispatch(events.PROCESSING, events.SKIPPED, docID=raw_doc['docID'])
        logger.warning('Not processing document with id {}'.format(raw_doc['docID']))
        return

    events.dispatch(events.PROCESSING, events.STARTED, docID=raw_doc['docID'], _index='normalized')

    processing.process_normalized(raw_doc, normalized_doc, kwargs)

    events.dispatch(events.PROCESSING, events.COMPLETED, docID=raw_doc['docID'], _index='normalized')


@app.task
def check_archives(reprocess):
    for consumer in settings.MANIFESTS.keys():
        check_archive.delay(consumer, reprocess)

        events.dispatch(events.CHECK_ARCHIVE, events.CREATED,
                        consumer=consumer, reprocess=reprocess)


@app.task
def check_archive(consumer_name, reprocess):
    events.dispatch(events.CHECK_ARCHIVE, events.STARTED,
                    consumer=consumer_name, reprocess=reprocess)

    consumer = settings.MANIFESTS[consumer_name]
    extras = {
        'overwrite': True
    }
    for raw_path in store.iter_raws(consumer_name, include_normalized=reprocess):
        timestamp = parser.parse(raw_path.split('/')[-2]).isoformat()
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

        chain = (normalize.si(raw_doc, consumer_name) |
                 process_normalized.s(raw_doc, storage=extras))
        chain.apply_async()

        events.dispatch(events.NORMALIZATION, events.CREATED,
                        consumer=consumer_name, docID=raw_doc['docID'])

    events.dispatch(events.CHECK_ARCHIVE, events.COMPLETED,
                    consumer=consumer_name, reprocess=reprocess)


@app.task
def update_pubsubhubbub():
    payload = {'hub.mode': 'publish', 'hub.url': '{url}rss/'.format(url=settings.OSF_APP_URL)}
    headers = {'Content-Type' : 'application/x-www-form-urlencoded'}
    return requests.post('https://pubsubhubbub.appspot.com', headers=headers, params=payload)


# TODO Fix me @fabianvf @chrisseto
@app.task
def tar_archive():
    os.system('tar -czvf website/static/archive.tar.gz archive/')
