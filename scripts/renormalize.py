from cqlengine import Token

from scrapi.database import _manager
from scrapi.processing.cassandra import DocumentModel
from scrapi.tasks import normalize, process_normalized
from scrapi.linter import RawDocument

_manager.setup()


def document_generator():
    count = 0
    query = DocumentModel.objects.all().limit(1000)
    page = list(query)
    while len(page) > 0:
        for doc in page:
            count += 1
            try:
                yield RawDocument({
                    'doc': doc.doc,
                    'docID': doc.docID,
                    'source': doc.source,
                    'filetype': doc.filetype,
                    'timestamps': doc.timestamps
                })
            except Exception as e:
                print(e)
        page = list(query.filter(pk__token__gt=Token(page[-1].pk)))


for raw in document_generator():
    try:
        process_normalized(normalize(raw, raw['source']), raw)
    except Exception as e:
        print(e)
