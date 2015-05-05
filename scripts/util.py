import time
import logging

from cqlengine import Token

from scrapi.database import _manager
from scrapi.processing.cassandra import DocumentModel

_manager.setup()
logger = logging.getLogger(__name__)


def documents(*sources):
    q = DocumentModel.objects.all().limit(1000)
    querysets = (q.filter(source=source) for source in sources) if sources else [q]
    for query in querysets:
        page = list(query)
        while len(page) > 0:
            for doc in page:
                yield doc
            page = next_page(query, page)


def next_page(query, page):
    while True:
        try:
            return list(query.filter(pk__token__gt=Token(page[-1].pk)))
        except Exception as e:
            logger.exception(e)
            time.sleep(5)
            logger.info("Trying again...")
