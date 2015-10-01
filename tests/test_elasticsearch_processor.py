import pytest
from . import utils

from scrapi.linter.document import NormalizedDocument, RawDocument
from scrapi.processing.elasticsearch import ElasticsearchProcessor

test_db = ElasticsearchProcessor()
es = test_db.manager.es

RAW = RawDocument(utils.RAW_DOC)
NORMALIZED = NormalizedDocument(utils.RECORD)


@pytest.mark.elasticsearch
def test_process_normalized():
    test_db.process_normalized(RAW, NORMALIZED, index='test')

    results = es.search(index='test', doc_type=RAW['source'])
    assert (len(results['hits']['hits']) == 1)


@pytest.mark.elasticsearch
def test_versions():
    NORMALIZED['source'] = RAW['source']
    NORMALIZED['_id'] = RAW['docID']
    test_db.process_normalized(RAW, NORMALIZED, index='test')

    old_title = NORMALIZED['title']
    result = es.search(index='test', doc_type=RAW['source'])['hits']['hits'][0]

    assert (result['_source']['title'] == old_title)

    NORMALIZED['title'] = 'a new title'
    test_db.process_normalized(RAW, NORMALIZED, index='test')
    results = es.search(index='test', doc_type=RAW['source'])
    assert (len(results['hits']['hits']) == 1)
    assert (results['hits']['hits'][0]['_source']['title'] == 'a new title')
