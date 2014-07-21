# -*- coding: utf-8 -*-
"""
    Search module for the scrAPI website.
"""
import logging
import pyelasticsearch
import search_settings

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


# These are the doc_types that exist in the search database
TYPES = ['article', 'citation']

try:
    elastic = pyelasticsearch.ElasticSearch(
        search_settings.ELASTIC_URI,
        timeout=search_settings.ELASTIC_TIMEOUT
    )
    logging.getLogger('pyelasticsearch').setLevel(logging.WARN)
    logging.getLogger('requests').setLevel(logging.WARN)
    elastic.health()
except pyelasticsearch.exceptions.ConnectionError as e:
    logger.error(e)
    logger.warn("The SEARCH_ENGINE setting is set to 'elastic', but there "
                "was a problem starting the elasticsearch interface. Is "
                "elasticsearch running?")
    elastic = None


def requires_search(func):
    def wrapped(*args, **kwargs):
        if elastic is not None:
            return func(*args, **kwargs)
    return wrapped


@requires_search
def search(index, query, start=0, size=10):
    if not query:
        query = {
            'query': {
                'match_all': {}
            },
            'from': start,
            'size': size
        }
    else:
        query = {
            'query': {
                'match': {
                    '_all': query
                }
            },
            'from': start,
            'size': size
        }
    raw_results = elastic.search(query, index=index)
    results = [hit['_source'] for hit in raw_results['hits']['hits']]
    return results


@requires_search
def update(index, document, category, id):
    elastic.update(index, category, id, doc=document, upsert=document, refresh=True)


@requires_search
def delete_all(index):
    try:
        elastic.delete_index(index)
    except pyelasticsearch.exceptions.ElasticHttpNotFoundError as e:
        logger.error(e)
        logger.error("The index '{}' was not deleted from elasticsearch".format(index))


@requires_search
def delete_doc(index, category, doc_id):
    try:
        elastic.delete(index, category, doc_id, refresh=True)
    except pyelasticsearch.exceptions.ElasticHttpNotFoundError:
        logger.warn("Document with id {} not found in database".format(doc_id))
