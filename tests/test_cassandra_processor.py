import utils

from scrapi import settings
from scrapi.linter.document import NormalizedDocument, RawDocument

# Need to force cassandra to ignore set keyspace
from scrapi.processing.cassandra import CassandraProcessor, DocumentModel, VersionModel


test_db = CassandraProcessor()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = RawDocument(utils.RAW_DOC)


def test_process_raw():
    test_db.process_raw(RAW)
    queryset = DocumentModel.objects(docID='someID', source='tests')
    assert(len(queryset) == 1)
    wipe_database()


def test_process_normalized():
    test_db.process_normalized(RAW, NORMALIZED)
    queryset = DocumentModel.objects(docID=NORMALIZED['id']['serviceID'], source=NORMALIZED['source'])

    assert(queryset[0].title == utils.RECORD['title'])

    wipe_database()


def test_versions():
    test_db.process_normalized(RAW, NORMALIZED)
    queryset = DocumentModel.objects(docID=NORMALIZED['id']['serviceID'], source=NORMALIZED['source'])

    assert (len(queryset) == 1)

    old_title = NORMALIZED['title']

    NORMALIZED['title'] = 'some new title'
    test_db.process_normalized(RAW, NORMALIZED)
    doc = DocumentModel.objects(docID=NORMALIZED['id']['serviceID'], source=NORMALIZED['source'])[0]
    assert (doc.title == 'some new title')

    version = VersionModel.objects(key=doc.versions[-1])[0]

    assert (version.title == old_title)

    wipe_database()


def wipe_database():
    for model in DocumentModel.objects():
        model.delete()
    for model in VersionModel.objects():
        model.delete()

    assert (len(DocumentModel.objects) + len(VersionModel.objects)) == 0
