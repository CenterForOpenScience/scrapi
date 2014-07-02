# -*- coding: utf-8 -*-

import logging
import pyelasticsearch
import settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# These are the doc_types that exist in the search database
TYPES = ['article', 'citation']

try:
    elastic = pyelasticsearch.ElasticSearch(
        settings.ELASTIC_URI,
        timeout=settings.ELASTIC_TIMEOUT
    )
    logging.getLogger('pyelasticsearch').setLevel(logging.DEBUG)
    logging.getLogger('requests').setLevel(logging.DEBUG)
    elastic.health()
except pyelasticsearch.exceptions.ConnectionError as e:
    logger.error(e)
    logger.warn("The SEARCH_ENGINE setting is set to 'elastic', but there "
                "was a problem starting the elasticsearch interface. Is "
                "elasticsearch running?")
    elastic = None


def search(query, start=0, end=10):
    query = {
        'query': {
            'match_all': {}
        }
    }
    raw_results = elastic.search(query, index='scrapi')
    results = [hit['_source'] for hit in raw_results['hits']['hits']]
    return results


def update(index, document, category):
    elastic.update(index, category, doc=document, upsert=document, refresh=True)


def delete_all(index):
    try:
        elastic.delete_index(index)
    except pyelasticsearch.exceptions.ElasticHttpNotFoundError as e:
        logger.error(e)
        logger.error("The index '{}' was not deleted from elasticsearch".format(index))


def delete_doc(index, category, doc_id):
    try:
        elastic.delete(index, category, doc_id, refresh=True)
    except pyelasticsearch.exceptions.ElasticHttpNotFoundError:
        logger.warn("Document with id {} not found in database".format(doc_id))
