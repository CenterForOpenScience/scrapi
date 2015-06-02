import time
import logging

from cassandra.cqlengine.query import Token

from scrapi.database import _manager
from scrapi.processing.cassandra import DocumentModel, DocumentModelV2

_manager.setup()
logger = logging.getLogger(__name__)


def ModelIteratorFactory(model, next_page):
    def model_iterator(*sources):
        q = model.objects.timeout(500).allow_filtering().all().limit(1000)
        querysets = (q.filter(source=source) for source in sources) if sources else [q]
        for query in querysets:
            page = try_forever(list, query)
            while len(page) > 0:
                for doc in page:
                    yield doc
                page = try_forever(next_page, query, page)
    return model_iterator


def next_page_v1(query, page):
    return list(query.filter(pk__token__gt=Token(page[-1].pk)))


def next_page_v2(query, page):
    return list(query.filter(docID__gt=page[-1].docID))

documents_v1 = ModelIteratorFactory(DocumentModel, next_page_v1)
documents_v2 = ModelIteratorFactory(DocumentModelV2, next_page_v2)


def try_forever(action, *args, **kwargs):
    while True:
        try:
            return action(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            time.sleep(5)
            logger.info("Trying again...")
