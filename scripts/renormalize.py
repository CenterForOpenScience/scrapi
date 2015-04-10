import logging
import datetime as dt

from cqlengine import Token

from scrapi.database import _manager
from scrapi.processing.cassandra import DocumentModel
from scrapi.tasks import normalize, process_normalized
from scrapi.linter import RawDocument

_manager.setup()
logger = logging.getLogger(__name__)


def document_generator():
    query = DocumentModel.objects.all().limit(1000)
    page = list(query)
    while len(page) > 0:
        for doc in page:
            if not isinstance(doc.providerUpdatedDateTime, dt.datetime):
                try:
                    yield RawDocument({
                        'doc': doc.doc,
                        'docID': doc.docID,
                        'source': doc.source,
                        'filetype': doc.filetype,
                        'timestamps': doc.timestamps
                    })
                except Exception as e:
                    logger.exception(e)
        page = list(query.filter(pk__token__gt=Token(page[-1].pk)))


def main():
    for raw in document_generator():
        try:
            process_normalized(normalize(raw, raw['source']), raw)
        except Exception as e:
            logger.exception(e)


if __name__ == '__main__':
    main()
