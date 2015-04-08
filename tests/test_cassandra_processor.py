import pytest

from scrapi.linter.document import NormalizedDocument, RawDocument

# Need to force cassandra to ignore set keyspace
from scrapi.processing.cassandra import CassandraProcessor, DocumentModel, VersionModel

from . import utils


test_db = CassandraProcessor()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = RawDocument(utils.RAW_DOC)


@pytest.mark.cassandra
def test_process_raw():
    test_db.process_raw(RAW)
    queryset = DocumentModel.objects(docID='someID', source=RAW['source'])
    assert(len(queryset) == 1)


@pytest.mark.cassandra
def test_process_normalized():
    test_db.process_normalized(RAW, NORMALIZED)
    queryset = DocumentModel.objects(docID=RAW['docID'], source=NORMALIZED['source'])

    assert(queryset[0].title == utils.RECORD['title'])


@pytest.mark.cassandra
def test_versions():
    test_db.process_normalized(RAW, NORMALIZED)
    queryset = DocumentModel.objects(docID=RAW['docID'], source=NORMALIZED['source'])

    assert (len(queryset) == 1)

    old_title = NORMALIZED['title']

    NORMALIZED['title'] = 'some new title'
    test_db.process_normalized(RAW, NORMALIZED)
    doc = DocumentModel.objects(docID=RAW['docID'], source=NORMALIZED['source'])[0]
    assert (doc.title == 'some new title')
    assert len(doc.versions) == 1

    version = VersionModel.objects(key=doc.versions[-1])[0]

    assert (version.title == old_title)

    test_db.process_normalized(RAW, NORMALIZED)
    doc = DocumentModel.objects(docID=RAW['docID'], source=NORMALIZED['source'])[0]
    assert (doc.title == 'some new title')
    assert len(doc.versions) == 1
