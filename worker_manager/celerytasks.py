from celery import Celery
import yaml
import os
import sys
import requests
import logging
import importlib
sys.path.insert(1, os.path.abspath('worker_manager/consumers'))  # TODO may be unnecessary
from api import process_docs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Celery('worker_manager/celerytasks')
app.config_from_object('worker_manager.celeryconfig')


@app.task
def run_scrapers():
    logger.info("Celery worker executing task run_scrapers")
    responses = []
    for manifest in os.listdir('worker_manager/manifests/'):
        logger.info(manifest)
        responses.append(run_scraper('worker_manager/manifests/' + manifest))
    logger.info('run_scraper got response: {}'.format(responses))
    return responses


@app.task
def run_scraper(manifest_file):
    manifest = _load_config(manifest_file)
    logger.info('run_scraper executing for service {}'.format(manifest['directory']))
    if manifest.get('url'):
        url = manifest['url'] + 'consume'
        ret = requests.post(url)
    else:
        logger.info('worker_manager.consumers.{0}.website.consume'.format(manifest['directory']))
        i = importlib.import_module('worker_manager.consumers.{0}.website.consume'.format(manifest['directory']))  # TODO '.website.' not necessary
        results = i.consume()
        ret = []
        with open('worker_manager/recent_files.txt', 'a+') as f:
            for result in results:
                doc, source, doc_id, filetype, consumer_version = result
                timestamp = process_docs.process_raw(doc, source, doc_id, filetype, consumer_version)
                ret.append(doc_id)
                f.write(', '.join([source, doc_id, str(timestamp)]))
                f.write('\n')
    return ret


@app.task
def request_parses():
    if not os.path.isfile('worker_manager/recent_files.txt'):
        return
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
            i = importlib.import_module('worker_manager.consumers.{0}.website.parse'.format(manifest['directory']))
            parsed = i.parse(doc, timestamp)['doc']
            logger.info('Document {0} parsed successfully'.format(doc_id))
            process_docs.process(parsed, timestamp)
            logger.info('Document {0} processed successfully'.format(doc_id))
    try:
        pass  # os.remove('worker_manager/recent_files.txt')
    except IOError:
        pass  # Doesn't matter


@app.task
def add(x, y):
    result = x + y
    logger.info(result)
    return result


def _load_config(config_file):
    with open(config_file) as f:
        info = yaml.load(f)
    return info
