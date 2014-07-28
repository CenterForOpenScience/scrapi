import os
import sys
import json
sys.path.insert(1, os.path.abspath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
)))
from website import search
from pyelasticsearch.exceptions import ElasticHttpNotFoundError, ElasticHttpError
import logging
logger = logging.getLogger(__name__)


def migrate():
    try:
        search.delete_all('scrapi')
    except ElasticHttpNotFoundError:
        pass
    for dirname, dirnames, filenames in os.walk('archive/'):
        if os.path.isfile(dirname + '/normalized.json'):
            with open(dirname + '/normalized.json') as f:
		try:
                    doc = json.load(f)
                except ValueError as e:
                    logger.exception(e)
                    continue

                try:
                    search.update('scrapi', doc, dirname.split('/')[1], dirname.split('/')[2])
                except ElasticHttpError:
                    pass

if __name__ == '__main__':
    migrate()
