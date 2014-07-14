from celery import Celery
import yaml
import os
import sys
import requests
import logging
import importlib
sys.path.insert(1, os.path.abspath('consumers'))
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))

from api import process_docs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Celery('celerytasks')
app.config_from_object('celeryconfig')


@app.task
def run_scrapers():
    logger.info("Celery worker executing task run_scrapers")
    responses = []
    for manifest in os.listdir('manifests/'):
        logger.info(manifest)
        responses.append(run_scraper('manifests/' + manifest))
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
        i = importlib.import_module('consumers.{0}.website.consume'.format(manifest['directory']))  # TODO '.website.' not necessary
        results = i.consume()
        ret = []
        for result in results:
            ret.append(process_docs.process_raw(result[0], result[1], result[2], result[3], result[4]))
    return ret


@app.task
def add(x, y):
    result = x + y
    logger.info(result)
    return result


def _load_config(config_file):
    with open(config_file) as f:
        info = yaml.load(f)
    return info

if __name__ == '__main__':
    run_scraper('manifests/plos-manifest.yml')
