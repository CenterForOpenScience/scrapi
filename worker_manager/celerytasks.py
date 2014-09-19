"""
    Contains all of the scheduled tasks for scrAPI.

    These tasks are run by the celery worker, and scheduled by
    the celery beat, as described in  worker_manager/celeryconfig.py
"""

import os
import json
import logging
import requests
import datetime
import importlib
import subprocess

import yaml
from celery import Celery
from scrapi_tools.document import RawDocument
from scrapi_tools.exceptions import MissingAttributeError

from website import search
from api import process_docs
from worker_manager import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Celery('worker_manager/celerytasks')
app.config_from_object('worker_manager.celeryconfig')

HEADERS = {'Content-type': 'application/json', 'Accept': 'text/plain'}

@app.task
def run_consumers():
    """
        Run every consumer with a manifest file in worker_manager/manifests
    """
    logger.info("Celery worker executing task run_scrapers")
    failures = []
    for manifest in os.listdir('worker_manager/manifests/'):
        if manifest != 'test.yml':
            logger.info(manifest)
            try:
                run_consumer('worker_manager/manifests/' + manifest)
            except Exception as e:
                failures.append((manifest, e))
    logger.info('run_consumers finished')
    logger.info('Failures: ')
    for failure in failures:
        logger.info(failure[0])
        logger.exception(failure[1])


@app.task
def run_consumer(manifest_file):
    """
        Run the consume and normalize functions of the module specified in the manifest

        Take a manifest file location, load the corresponding module from the
        consumers/ directory, call the consume and normalize functions for that module,
        and add the normalized documents to the elastic search index.
        Return the list of normalized documents
    """
    manifest = _load_config(manifest_file)
    logger.info('run_scraper executing for service {}'.format(manifest['directory']))
    logger.info('worker_manager.consumers.{0}'.format(manifest['directory']))

    results, registry, consumer_version = _consume(manifest['directory'])

    docs = []
    for result in results:
        timestamp = process_docs.process_raw(result, consumer_version)
        docs.append(_normalize(result, timestamp, registry, manifest))
    return docs


def _consume(directory):
    consumer_module = importlib.import_module('worker_manager.consumers.{0}'.format(directory))
    registry = consumer_module.registry
    consumer_version = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd='worker_manager/consumers/{0}'
                                               .format(directory))
    results = registry[directory]['consume']()

    return results, registry, consumer_version


def _normalize(result, timestamp, registry, manifest):
    iso_timestamp = timestamp.isoformat()
    normalized = registry[manifest['directory']]['normalize'](result, timestamp)
    logger.info('Document {0} normalized successfully'.format(result.get("doc_id")))
    doc = process_docs.process(normalized, timestamp)
    if doc is not None:
        doc.attributes['source'] = manifest['name']
        doc.attributes['location'] = "archive/{source}/{doc_id}/{timestamp}/normalized.json"\
            .format(source=manifest['directory'], doc_id=doc.get('id').get('service_id'), timestamp=doc.get('timestamp')),
        doc.attributes['iso_timestamp'] = str(iso_timestamp)
        logger.info('Document {0} processed successfully'.format(result.get("doc_id")))
        search.update('scrapi', doc.attributes, manifest['directory'], result.get("doc_id"))
        _send_to_osf(doc.attributes)
    return doc


def _send_to_osf(doc):
    if not settings.OSF_ENABLED:
        logger.warn("scrAPI is not configured to interact with the Open Science Framework")
        return

    ret = requests.post(settings.NEW_RECORD_URL, data=json.dumps(doc), headers=HEADERS, auth=settings.OSF_AUTH)

    if ret.status_code != 201:
        logger.warn('Failed to created a new record on the Open Science Framework')
        logger.warn('Recieved a response of {} from the Open Science Framework'.format(ret.status_code))
        logger.warn('The document {} has been marked of it failure to send'.format(doc['title']))

    doc['recordCreated'] = ret.status_code == 201


@app.task
def request_normalized():
    """
        Deprecated/on hold until the push service comes back.

        Read a file storing a list of the most recently consumed documents, and
        request normalization for those documents from the appropriate consumer module.
    """
    if not os.path.isfile('worker_manager/recent_files.txt'):
        return "No documents waiting for normalization"

    with open('worker_manager/recent_files.txt', 'r') as recent_files:
        for line in recent_files:
            info = line.split(',')
            source = info[0].replace(' ', '')
            manifest = _load_config('worker_manager/manifests/{}.yml'.format(source))
            doc_id = info[1].replace(' ', '').replace('/', '')
            timestamp = info[2].replace(' ', '', 1).replace('\n', '')

            filepath = 'archive/' + source + '/' + doc_id + '/' + timestamp + '/raw' + manifest['file-format']
            try:
                with open(filepath, 'r') as f:
                    doc = f.read()
            except IOError as e:
                logger.error(e)
                continue

            consumer_module = importlib.import_module('worker_manager.consumers.{0}'.format(manifest['directory']))
            registry = consumer_module.registry

            _normalize(doc, timestamp, registry, manifest)
    try:
        os.remove('worker_manager/recent_files.txt')
    except IOError:
        pass  # Doesn't matter

    return "Scanning complete"


@app.task
def check_archive(directory='', reprocess=False):
    """
        Normalize every non-normalized document in the archive.

        Does a directory walk over the the entire archive/ directory, and requests
        a normalized document for every raw file with no normalized neighbor.
    """
    manifests = {}
    for filename in os.listdir('worker_manager/manifests/'):
        manifest = _load_config('worker_manager/manifests/' + filename)
        manifests[manifest['directory']] = manifest

    for dirname, dirnames, filenames in os.walk('archive/' + directory):
        for filename in filenames:
            if 'raw' in filename and (not (os.path.isfile(dirname + '/normalized.json')) or reprocess):
                timestamp = datetime.datetime.strptime(dirname.split('/')[-1], '%Y-%m-%d %H:%M:%S.%f')
                service = dirname.split('/')[1]
                doc_id = dirname.split('/')[2]
                with open(os.path.join(dirname, filename), 'r') as f:
                    logger.info("worker_manager.consumers.{0}".format(manifests[service]['directory']))
                    consumer_module = importlib.import_module('worker_manager.consumers.{0}'.format(manifests[service]['directory']))
                    registry = consumer_module.registry
                    raw_file = RawDocument({
                        'doc': f.read(),
                        'doc_id': doc_id,
                        'source': service,
                        'filetype': manifests[service]['file-format'],
                    })
                    try:
                        _normalize(raw_file, timestamp, registry, manifests[service])
                    except MissingAttributeError as e:
                        logger.exception(e)


@app.task
def tar_archive():
    os.system('tar -czvf website/static/archive.tar.gz archive/')


@app.task
def heartbeat(message):
    """
        Heartbeat for the celery worker
    """
    return message


def _load_config(config_file):
    """
        Load a specified yaml file into a dictionary
    """
    with open(config_file) as f:
        info = yaml.load(f)
    return info
