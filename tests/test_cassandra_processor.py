import cqlengine

import utils

from scrapi.linter.document import NormalizedDocument, RawDocument


NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = RawDocument(utils.RAW_DOC)


from scrapi import settings

settings.CASSANDRA_KEYSPACE = 'test'
from scrapi.processing.cassandra import CassandraProcessor, DocumentModel, VersionModel

test_db = CassandraProcessor()


def test_process_raw():
    test_db.process_raw(RAW)
    queryset = DocumentModel.objects(docID='someID', source='tests')
    assert(len(queryset) == 1)
    # cqlengine.management.delete_keyspace(settings.CASSANDRA_KEYSPACE)


def test_process_normalized():
    test_db.process_normalized(RAW, NORMALIZED)
    queryset = DocumentModel.objects(docID=NORMALIZED['id']['serviceID'], source=NORMALIZED['source'])

    assert(queryset[0].title == utils.RECORD['title'])


def test_versions():
    pass
