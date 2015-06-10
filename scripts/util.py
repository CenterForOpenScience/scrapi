import time
import logging

from cassandra.cqlengine.query import Token

from scrapi import registry
from scrapi.database import _manager
from scrapi.processing.cassandra import DocumentModel, DocumentModelOld

_manager.setup()
logger = logging.getLogger(__name__)


def ModelIteratorFactory(model, next_page, default_args=None):
    def model_iterator(*sources):
        sources = sources or default_args
        q = model.objects.timeout(500).allow_filtering().all().limit(1000)
        querysets = (q.filter(source=source) for source in sources) if sources else [q]
        for query in querysets:
            page = try_forever(list, query)
            while len(page) > 0:
                for doc in page:
                    yield doc
                page = try_forever(next_page, query, page)
    return model_iterator


def next_page_old(query, page):
    return list(query.filter(pk__token__gt=Token(page[-1].pk)))


def next_page_source_partition(query, page):
    return list(query.filter(docID__gt=page[-1].docID))

documents_old = ModelIteratorFactory(DocumentModelOld, next_page_old)
documents = ModelIteratorFactory(DocumentModel, next_page_source_partition, default_args=registry.keys())


def try_forever(action, *args, **kwargs):
    while True:
        try:
            return action(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            time.sleep(5)
            logger.info("Trying again...")
