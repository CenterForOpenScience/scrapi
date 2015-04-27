import logging


from scripts.util import documents

from scrapi import settings
from scrapi.linter import RawDocument
from scrapi.processing.elasticsearch import es
from scrapi.tasks import normalize, process_normalized, process_raw

logger = logging.getLogger(__name__)


def rename(source, target, dry=True):
    count = 0
    exceptions = []

    for doc in documents([source]):
        count += 1
        try:
            raw = RawDocument({
                'doc': doc.doc,
                'docID': doc.docID,
                'source': target,
                'filetype': doc.filetype,
                'timestamps': doc.timestamps,
                'versions': doc.versions
            })
            if not dry:
                process_raw(raw)
                process_normalized(normalize(raw, raw['source']), raw)
            else:
                logger.info('Processing document from {} with id {}'.format(source, raw['docID']))
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)
        else:
            if not dry:
                doc.delete()
                es.delete(index=settings.ELASTIC_INDEX, doc_type=source, id=raw['docID'])
            else:
                logger.info('Deleting document from {} with id {}'.format(source, raw['docID']))

    for ex in exceptions:
        logger.exception(e)
    logger.info('{} documents processed, with {} exceptions'.format(count, len(exceptions)))
