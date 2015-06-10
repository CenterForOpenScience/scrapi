import logging

from cassandra.cqlengine.connection import log

from scripts.util import documents_old

from scrapi.processing.cassandra import DocumentModel

logger = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def migrate_to_source_partition():
    count = 0
    exceptions = []
    for doc in documents_old():
        count += 1
        try:
            DocumentModel.create(**dict(doc)).save()
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)

    for ex in exceptions:
        logger.exception(e)
    logger.info('{} documents processed, with {} exceptions'.format(count, len(exceptions)))

if __name__ == '__main__':
    migrate_to_source_partition()
