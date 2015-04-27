import logging

from scripts.util import documents

from scrapi.linter import RawDocument
from scrapi.tasks import normalize, process_normalized

logger = logging.getLogger(__name__)


def renormalize(sources=None):
    count = 0
    exceptions = []
    for doc in documents(sources):
        count += 1
        try:
            raw = RawDocument({
                'doc': doc.doc,
                'docID': doc.docID,
                'source': doc.source,
                'filetype': doc.filetype,
                'timestamps': doc.timestamps,
                'versions': doc.versions
            })
            process_normalized(normalize(raw, raw['source']), raw)
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)

    for ex in exceptions:
        logger.exception(e)
    logger.info('{} documents processed, with {} exceptions'.format(count, len(exceptions)))
