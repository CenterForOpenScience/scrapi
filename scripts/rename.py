import logging

from cqlengine import Token

from scrapi import settings
from scrapi.database import _manager
from scrapi.linter import RawDocument
from scrapi.processing.elasticsearch import es
from scrapi.processing.cassandra import DocumentModel
from scrapi.tasks import normalize, process_normalized, process_raw

_manager.setup()
logger = logging.getLogger(__name__)


def document_generator(source, target):
    query = DocumentModel.objects.all().filter(source=source).limit(1000)
    page = list(query)
    while len(page) > 0:
        for doc in page:
            try:
                yield RawDocument({
                    'doc': doc.doc,
                    'docID': doc.docID,
                    'source': target,
                    'filetype': doc.filetype,
                    'timestamps': doc.timestamps
                })
            except Exception as e:
                logger.exception(e)
        page = list(query.filter(pk__token__gt=Token(page[-1].pk)))


def rename(source, target):
    count = 0
    exceptions = []
    for raw in document_generator(source, target):
        count += 1
        try:
            process_raw(raw)
            process_normalized(normalize(raw, raw['source']), raw)
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)
    for ex in exceptions:
        logger.exception(e)
    logger.error('{} documents processed, with {} exceptions'.format(count, len(exceptions)))

    assert es.count(settings.ELASTIC_INDEX, doc_type=source) == es.count(settings.ELASTIC_INDEX, doc_type=target)
