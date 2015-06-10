import copy
import pytest
import mock

import scrapi
from scrapi.linter.document import NormalizedDocument

from scrapi import tasks

from scrapi import registry
from scrapi.migrations import delete
from scrapi.migrations import rename

# Need to force cassandra to ignore set keyspace
from scrapi.processing.cassandra import CassandraProcessor, DocumentModel

from . import utils

test_cass = CassandraProcessor()

harvester = utils.TestHarvester()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = harvester.harvest()[0]


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

    assert(queryset[0].source == utils.RECORD['shareProperties']['source'])
    assert(queryset[0].source == old_source)

    new_record = copy.deepcopy(utils.RECORD)

    new_record['shareProperties']['source'] = 'wwe_news'
    test_info = registry['test'].__class__()

    test_info.short_name = 'wwe_news'

    registry['wwe_news'] = test_info

    tasks.migrate(rename, source=old_source, target='wwe_news')

    queryset = DocumentModel.objects(docID=RAW['docID'], source='wwe_news')
    assert(queryset[0].source == 'wwe_news')
    assert(len(queryset) == 1)
    scrapi.processing.elasticsearch.es = real_es


@pytest.mark.cassandra
def test_delete():
    real_es = scrapi.processing.elasticsearch.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()
    test_cass.process_raw(RAW)
    test_cass.process_normalized(RAW, NORMALIZED)

    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert(len(queryset) == 1)

    tasks.migrate(delete, source=RAW['source'])
    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert(len(queryset) == 0)
    scrapi.processing.elasticsearch.es = real_es
