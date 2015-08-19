import copy
import pytest
import mock
import six

import scrapi
from scrapi.linter.document import NormalizedDocument

from scrapi import tasks
from scrapi import registry
from scrapi.migrations import delete
from scrapi.migrations import rename
from scrapi.migrations import renormalize
from scrapi.migrations import DocumentModelOld

# Need to force cassandra to ignore set keyspace
from scrapi.processing.cassandra import CassandraProcessor, DocumentModel
from scrapi.processing.postgres import PostgresProcessor

from . import utils

test_cass = CassandraProcessor()
test_postgres = PostgresProcessor()

test_harvester = utils.TestHarvester()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = test_harvester.harvest()[0]


@pytest.fixture
def harvester():
    pass  # Need to override this


@pytest.mark.cassandra
def test_rename():
    real_es = scrapi.processing.elasticsearch.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()
    test_cass.process_raw(RAW)
    test_cass.process_normalized(RAW, NORMALIZED)

    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    old_source = NORMALIZED['shareProperties']['source']

    assert queryset[0].source == utils.RECORD['shareProperties']['source']
    assert queryset[0].source == old_source

    new_record = copy.deepcopy(utils.RECORD)

    new_record['shareProperties']['source'] = 'wwe_news'

    test_harvester.short_name = 'wwe_news'

    registry['wwe_news'] = test_harvester

    tasks.migrate(rename, sources=[old_source], target='wwe_news', dry=False)

    queryset = DocumentModel.objects(docID=RAW['docID'], source='wwe_news')
    assert queryset[0].source == 'wwe_news'
    assert len(queryset) == 1
    scrapi.processing.elasticsearch.es = real_es


@pytest.mark.cassandra
def test_delete():
    real_es = scrapi.processing.elasticsearch.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()
    test_cass.process_raw(RAW)
    test_cass.process_normalized(RAW, NORMALIZED)

    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert len(queryset) == 1

    tasks.migrate(delete, sources=[RAW['source']], dry=False)
    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert len(queryset) == 0
    scrapi.processing.elasticsearch.es = real_es


@pytest.mark.cassandra
def test_renormalize():
    real_es = scrapi.processing.elasticsearch.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()
    test_cass.process_raw(RAW)
    test_cass.process_normalized(RAW, NORMALIZED)

    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert len(queryset) == 1

    tasks.migrate(renormalize, source=RAW['source'])
    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert len(queryset) == 1
    scrapi.processing.elasticsearch.es = real_es


@pytest.mark.cassandra
def test_migrate_v2():
    try:
        RAW['doc'] = RAW['doc'].encode('utf-8')
    except AttributeError:
        RAW['doc'] = str(RAW['doc'])
    DocumentModelOld.create(**RAW.attributes).save()
    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert len(queryset) == 0
    tasks.migrate_to_source_partition(dry=False)
    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert len(queryset) == 1
