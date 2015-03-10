import pytest
import utils

from scrapi.linter.document import NormalizedDocument, RawDocument
from scrapi.processing.elasticsearch import es, ElasticsearchProcessor

test_db = ElasticsearchProcessor()


RAW = RawDocument(utils.RAW_DOC)
NORMALIZED = NormalizedDocument(utils.RECORD)


@pytest.mark.elasticsearch
def test_process_normalized():
    NORMALIZED['source'] = 'test'
    NORMALIZED['_id'] = NORMALIZED['id']['serviceID']
    test_db.process_normalized(RAW, NORMALIZED)

    results = es.search(index='share', doc_type='test')
    assert (len(results['hits']['hits']) == 1)


@pytest.mark.elasticsearch
def test_versions():
    NORMALIZED['source'] = 'test'
    NORMALIZED['_id'] = NORMALIZED['id']['serviceID']
    test_db.process_normalized(RAW, NORMALIZED)

    old_title = NORMALIZED['title']
    result = es.search(index='share', doc_type='test')['hits']['hits'][0]

    assert (result['_source']['title'] == old_title)

    NORMALIZED['title'] = 'a new title'
    test_db.process_normalized(RAW, NORMALIZED)
    results = es.search(index='share', doc_type='test')
    assert (len(results['hits']['hits']) == 1)
    assert (results['hits']['hits'][0]['_source']['title'] == 'a new title')
