import logging
from base64 import b64decode
from datetime import datetime

import requests
from celery import Celery
from dateutil import parser

from scrapi import util
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
@events.creates_task(events.HARVESTER_RUN)
def run_harvester(harvester_name, days_back=1):
    logger.info('Running harvester "{}"'.format(harvester_name))

    normalization = begin_normalization.s(harvester_name)
    start_consumption = harvest.si(harvester_name, timestamp(), days_back=days_back)

    # Form and start a celery chain
    (start_consumption | normalization).apply_async()


@app.task
@events.logged(events.HARVESTER_RUN)
def harvest(harvester_name, job_created, days_back=1):
    harvest_started = timestamp()
    harvester = import_harvester(harvester_name)
    logger.info('Harvester "{}" has begun harvesting'.format(harvester_name))

    with util.maybe_recorded(harvester_name):
        result = harvester.harvest(days_back=days_back)

    # result is a list of all of the RawDocuments consumed
    return result, {
        'harvestFinished': timestamp(),
        'harvestTaskCreated': job_created,
        'harvestStarted': harvest_started,
    }


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
        spawn_tasks(raw, timestamps, harvester_name)


@events.creates_task(events.PROCESSING)
@events.creates_task(events.NORMALIZATION)
def spawn_tasks(raw, timestamps, harvester_name):
        raw['timestamps'] = timestamps
        raw['timestamps']['normalizeTaskCreated'] = timestamp()
        chain = (normalize.si(raw, harvester_name) | process_normalized.s(raw))

        chain.apply_async()
        process_raw.delay(raw)


@app.task
@events.logged(events.PROCESSING, 'raw')
def process_raw(raw_doc, **kwargs):
    processing.process_raw(raw_doc, kwargs)


@app.task
@events.logged(events.NORMALIZATION)
def normalize(raw_doc, harvester_name):
    normalized_started = timestamp()
    harvester = import_harvester(harvester_name)

    with events.logged_failure(events.NORMALIZATION):
        normalized = harvester.normalize(raw_doc)

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
