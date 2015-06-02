import logging

from scripts.util import documents_v2 as documents

from scrapi import settings
from scrapi.processing.elasticsearch import es

logger = logging.getLogger(__name__)


def delete_by_source(source):
    count = 0
    exceptions = []
    for doc in documents(source):
        count += 1
        try:
            doc.delete()
            es.delete(index=settings.ELASTIC_INDEX, doc_type=source, id=doc.docID, ignore=[404])
            es.delete(index='share_v1', doc_type=source, id=doc.docID, ignore=[404])
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)

    for ex in exceptions:
        logger.exception(e)
    logger.info('{} documents processed, with {} exceptions'.format(count, len(exceptions)))
