from cqlengine import Token

from scrapi.database import _manager
from scrapi.processing.cassandra import DocumentModel

_manager.setup()


def documents(sources):
    q = DocumentModel.objects.all().limit(1000)
    querysets = (q.filter(source=source) for source in sources)
    for query in querysets:
        page = list(query)
        while len(page) > 0:
            for doc in page:
                yield doc
            page = list(query.filter(pk__token__gt=Token(page[-1].pk)))
