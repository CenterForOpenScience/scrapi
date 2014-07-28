"""
    Contains all of the scheduled tasks for scrAPI.

    These tasks are run by the celery worker, and scheduled by
    the celery beat, as described in  worker_manager/celeryconfig.py
"""

from celery import Celery
import yaml
import os
import sys
import requests
import logging
import importlib
import subprocess
sys.path.insert(1, os.path.abspath('worker_manager/consumers'))  # TODO may be unnecessary
from api import process_docs
from website import search
from scrapi_tools.consumer import RawFile
from scrapi_tools.exceptions import MissingAttributeError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Celery('worker_manager/celerytasks')
app.config_from_object('worker_manager.celeryconfig')


@app.task
def run_consumers():
    """
        Run every consumer with a manifest file in worker_manager/manifests
    """
    logger.info("Celery worker executing task run_scrapers")
    for manifest in os.listdir('worker_manager/manifests/'):
        logger.info(manifest)
        run_consumer('worker_manager/manifests/' + manifest)
    logger.info('run_consumers finished')


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
    if manifest.get('url'):
        url = manifest['url'] + 'consume'
        ret = requests.post(url)
    else:
        logger.info('worker_manager.consumers.{0}'.format(manifest['directory']))
        consumer_module = importlib.import_module('worker_manager.consumers.{0}'.format(manifest['directory']))
        consumer = consumer_module.get_consumer()
        consumer_version = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd='worker_manager/consumers/{0}'
                                                   .format(manifest['directory']))
        results = consumer.consume()

        ret = []
        for result in results:
            timestamp = process_docs.process_raw(result, consumer_version)
            normalized = consumer.normalize(result, timestamp)
            logger.info('Document {0} normalized successfully'.format(result.get("doc_id")))
            doc = process_docs.process(normalized, timestamp)
            if doc is not None:
                doc.attributes['source'] = manifest['name']
                logger.info('Document {0} processed successfully'.format(result.get("doc_id")))
                search.update('scrapi', doc.attributes, manifest['directory'], result.get("doc_id"))  # TODO unhardcode 'article'
                ret.append(doc)
    return ret


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
            i = importlib.import_module('worker_manager.consumers.{0}.normalize'.format(manifest['directory']))
            normalized = i.normalize(doc, timestamp)['doc']
            logger.info('Document {0} normalized successfully'.format(doc_id))
            doc = process_docs.process(normalized, timestamp)
            if doc is not None:
                doc['source'] = manifest['name']
                logger.info('Document {0} processed successfully'.format(doc_id))
                search.update('scrapi', doc, 'article', doc_id)
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
                timestamp = dirname.split('/')[-1]
                service = dirname.split('/')[1]
                doc_id = dirname.split('/')[2]
                with open(os.path.join(dirname, filename), 'r') as f:
                    logger.info("worker_manager.consumers.{0}".format(manifests[service]['directory']))
                    consumer_module = importlib.import_module('worker_manager.consumers.{0}'.format(manifests[service]['directory']))
                    consumer = consumer_module.get_consumer()
                    raw_file = RawFile({
                        'doc': f.read(),
                        'doc_id': doc_id,
                        'source': service,
                        'filetype': manifests[service]['file-format'],
                    })
                    try:
                        normalized = consumer.normalize(raw_file, timestamp)
                    except MissingAttributeError as e:
                        logger.exception(e)
                        continue
                    logger.info('Document {0} normalized successfully'.format(doc_id))
                    result = process_docs.process(normalized, timestamp)
                    if result is not None:
                        logger.info('Document {0} processed successfully'.format(doc_id))
                        search.update('scrapi', result.attributes, manifests[service]['directory'], doc_id)


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
