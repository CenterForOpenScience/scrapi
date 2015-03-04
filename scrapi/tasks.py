import os
import logging
from dateutil import parser
from base64 import b64decode
from datetime import datetime

import vcr

import requests

from celery import Celery

from base64 import b64encode

from scrapi import events
from scrapi import settings
from scrapi import processing
from scrapi.util import timestamp
from scrapi.util.storage import store
from scrapi.util import import_harvester
from scrapi.linter.document import RawDocument


app = Celery()
app.config_from_object(settings)

logger = logging.getLogger(__name__)


@app.task
def run_harvester(harvester_name, days_back=1):
    logger.info('Running harvester "{}"'.format(harvester_name))

    # Form and start a celery chain
    chain = (harvest.si(harvester_name, timestamp(), days_back=days_back)
             | begin_normalization.s(harvester_name))

    chain.apply_async()

    # Note: Dispatch events only after they run
    events.dispatch(
        events.HARVESTER_RUN,
        events.CREATED,
        days_back=days_back,
        harvester=harvester_name,
    )


@app.task
def harvest(harvester_name, job_created, days_back=1):
    logger.info('harvester "{}" has begun consumption'.format(harvester_name))
    events.dispatch(
        events.HARVESTER_RUN,
        events.STARTED,
        days_back=days_back,
        harvester=harvester_name,
        job_created=job_created
    )

    timestamps = {
        'harvestTaskCreated': job_created,
        'harvestStarted': timestamp()
    }

    harvester = import_harvester(harvester_name)

    try:
        if settings.STORE_HTTP_TRANSACTIONS:
            cassette = os.path.join(settings.RECORD_DIRECTORY,
                                    harvester_name, timestamp() + '.yml')

            logger.debug('Recording HTTP consumption request for {} to {}'
                         .format(harvester_name, cassette))

            with vcr.use_cassette(cassette, record_mode='all'):
                result = harvester.harvest(days_back=days_back)
        else:
            result = harvester.harvest(days_back=days_back)
    except Exception as e:
        events.dispatch(
            events.HARVESTER_RUN,
            events.FAILED,
            exception=repr(e),
            days_back=days_back,
            harvester=harvester_name,
            job_created=job_created
        )
        raise

    timestamps['harvestFinished'] = timestamp()

    logger.info('harvester "{}" has finished consumption'.format(harvester_name))
    events.dispatch(
        events.HARVESTER_RUN,
        events.COMPLETED,
        harvester=harvester_name,
        number=len(result)
    )

    # result is a list of all of the RawDocuments harvested
    return result, timestamps


@app.task
def begin_normalization((raw_docs, timestamps), harvester_name):
    '''harvest_ret is harvest return value:
        a tuple contaiing list of rawDocuments and
        a dictionary of timestamps
    '''
    logger.info('Normalizing {} documents for harvester "{}"'
                .format(len(raw_docs), harvester_name))

    # raw is a single raw document
    for raw in raw_docs:
        raw['timestamps'] = timestamps
        raw['timestamps']['normalizeTaskCreated'] = timestamp()

        logger.debug('Created the process raw task for {}/{}'
                     .format(harvester_name, raw['docID']))

        process_raw.delay(raw)

        chain = (normalize.si(raw, harvester_name) |
                 process_normalized.s(raw))

        logger.debug('Created the process normalized task for {}/{}'
                     .format(harvester_name, raw['docID']))

        chain.apply_async()

        # Note: Dispatch events only AFTER the event has actually happened

        events.dispatch(events.NORMALIZATION, events.CREATED,
                        harvester=harvester_name, **raw.attributes)

        events.dispatch(events.PROCESSING, events.CREATED,
                        harvester=harvester_name, **raw.attributes)


@app.task
def process_raw(raw_doc, **kwargs):
    events.dispatch(events.PROCESSING, events.STARTED,
                    _index='raw', **raw_doc.attributes)
    processing.process_raw(raw_doc, kwargs)

    events.dispatch(events.PROCESSING, events.COMPLETED,
                    _index='raw', **raw_doc.attributes)


@app.task
def normalize(raw_doc, harvester_name):
    harvester = import_harvester(harvester_name)

    raw_doc['timestamps']['normalizeStarted'] = timestamp()

    logger.debug('Document {}/{} normalization began'.format(
        harvester_name, raw_doc['docID']))

    events.dispatch(events.NORMALIZATION, events.STARTED,
                    harvester=harvester_name, **raw_doc.attributes)
    try:
        normalized = harvester.normalize(raw_doc)
    except Exception as e:
        events.dispatch(
            events.NORMALIZATION,
            events.FAILED,
            harvester=harvester_name,
            exception=repr(e),
            **raw_doc.attributes
        )
        raise

    if not normalized:
        events.dispatch(events.NORMALIZATION, events.SKIPPED,
                        harvester=harvester_name, **raw_doc.attributes)
        logger.warning
        (
            'Did not normalize document [{}]{}'.format
            (
                harvester_name,
                raw_doc['docID']
            )
        )
        return None

    logger.debug('Document {}/{} normalized sucessfully'.format(
        harvester_name, raw_doc['docID']))

    events.dispatch(events.NORMALIZATION, events.COMPLETED,
                    harvester=harvester_name, **raw_doc.attributes)

    normalized['timestamps'] = raw_doc['timestamps']
    normalized['timestamps']['normalizeFinished'] = timestamp()

    normalized['dateCollected'] = normalized['timestamps']['harvestFinished']

    normalized['raw'] = '{url}/{archive}{source}/{doc_id}/{harvestFinished}/raw.{raw_format}'.format(
        url=settings.SCRAPI_URL,
        archive=settings.ARCHIVE_DIRECTORY,
        source=normalized['source'],
        doc_id=b64encode(raw_doc['docID']),
        harvestFinished=normalized['timestamps']['harvestFinished'],
        raw_format=raw_doc['filetype']
    )

    # returns a single normalized document
    return normalized


@app.task
def process_normalized(normalized_doc, raw_doc, **kwargs):
    if not normalized_doc:
        events.dispatch(
            events.PROCESSING,
            events.SKIPPED,
            **raw_doc.attributes
        )
        logger.warning('Not processing document with id {}'.format(raw_doc['docID']))
        return

    events.dispatch(
        events.PROCESSING,
        events.STARTED,
        _index='normalized',
        **raw_doc.attributes
    )

    processing.process_normalized(raw_doc, normalized_doc, kwargs)

    events.dispatch(
        events.PROCESSING,
        events.COMPLETED,
        _index='normalized',
        **raw_doc.attributes
    )


@app.task
def check_archives(reprocess, days_back=None):
    for harvester in settings.MANIFESTS.keys():
        check_archive.delay(harvester, reprocess, days_back=days_back)

        events.dispatch(events.CHECK_ARCHIVE, events.CREATED,
                        harvester=harvester, reprocess=reprocess)


@app.task
def check_archive(harvester_name, reprocess, days_back=None):
    events.dispatch(events.CHECK_ARCHIVE, events.STARTED, **{
        'harvester': harvester_name,
        'reprocess': reprocess,
        'daysBack': str(days_back) if days_back else 'All'
    })

    harvester = settings.MANIFESTS[harvester_name]
    extras = {
        'overwrite': True
    }
    for raw_path in store.iter_raws(harvester_name, include_normalized=reprocess):
        date = parser.parse(raw_path.split('/')[-2])

        if (days_back and (datetime.now() - date).days > days_back):
            continue

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

        chain = (normalize.si(raw_doc, harvester_name) |
                 process_normalized.s(raw_doc, storage=extras))
        chain.apply_async()

        events.dispatch(events.NORMALIZATION, events.CREATED, **{
            'harvester': harvester_name,
            'reprocess': reprocess,
            'daysBack': str(days_back) if days_back else 'All',
            'docID': raw_doc['docID']
        })

    events.dispatch(events.CHECK_ARCHIVE, events.COMPLETED, **{
        'harvester': harvester_name,
        'reprocess': reprocess,
        'daysBack': str(days_back) if days_back else 'All'
    })


@app.task
def update_pubsubhubbub():
    payload = {'hub.mode': 'publish', 'hub.url': '{url}rss/'.format(url=settings.OSF_APP_URL)}
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    return requests.post('https://pubsubhubbub.appspot.com', headers=headers, params=payload)


# TODO Fix me @fabianvf @chrisseto
@app.task
def tar_archive():
    os.system('tar -czvf website/static/archive.tar.gz archive/')
