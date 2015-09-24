import copy
import mock
import pytest

import scrapi
from scrapi.linter.document import NormalizedDocument

from scrapi import tasks
from scrapi import registry
from scrapi.migrations import delete
from scrapi.migrations import rename
from scrapi.migrations import cross_db
from scrapi.migrations import renormalize

from scrapi.processing import get_processor

from . import utils

# test_cass = CassandraProcessor()

test_harvester = utils.TestHarvester()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = test_harvester.harvest()[0]


@pytest.fixture
def harvester():
    pass  # Need to override this


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.parametrize('processor_name', ['postgres', 'cassandra'])
def test_rename(processor_name, monkeypatch):
    real_es = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()
    monkeypatch.setattr('scrapi.settings.CANONICAL_PROCESSOR', processor_name)

    processor = get_processor(processor_name)
    processor.process_raw(RAW)
    processor.process_normalized(RAW, NORMALIZED)

    queryset = processor.document_query(source=RAW['source'], docID=RAW['docID'])

    old_source = NORMALIZED['shareProperties']['source']

    assert queryset.normalized.attributes['shareProperties']['source'] == utils.RECORD['shareProperties']['source']
    assert queryset.normalized.attributes['shareProperties']['source'] == old_source

    new_record = copy.deepcopy(utils.RECORD)
    new_record['shareProperties']['source'] = 'wwe_news'
    test_harvester.short_name = 'wwe_news'
    registry['wwe_news'] = test_harvester

    tasks.migrate(rename, sources=[old_source], target='wwe_news', dry=False)

    queryset = processor.document_query(source='wwe_news', docID=RAW['docID'])

    assert queryset.normalized.attributes['shareProperties']['source'] == 'wwe_news'

    scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es = real_es
    test_harvester.short_name = RAW['source']
    registry['test'] = test_harvester
    del registry['wwe_news']


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.parametrize('processor_name', ['cassandra', 'postgres'])
def test_delete(processor_name, monkeypatch):
    real_es = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()

    monkeypatch.setattr('scrapi.settings.CANONICAL_PROCESSOR', processor_name)

    print('Canonical Processor is {}'.format(scrapi.settings.CANONICAL_PROCESSOR))
    processor = get_processor(processor_name)
    processor.process_raw(RAW)
    processor.process_normalized(RAW, NORMALIZED)

    queryset = processor.document_query(docID=RAW['docID'], source=RAW['source'])
    assert queryset

    tasks.migrate(delete, sources=[RAW['source']], dry=False)
    queryset = processor.document_query(docID=RAW['docID'], source=RAW['source'])
    assert not queryset
    scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es = real_es


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.parametrize('processor_name', ['postgres'])
def test_renormalize(processor_name):
    # Set up
    real_es = scrapi.processing.elasticsearch.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()

    # Process raw and normalized with fake docs
    processor = get_processor(processor_name)
    processor.process_raw(RAW)
    processor.process_normalized(RAW, NORMALIZED)

    # Check to see those docs were processed
    queryset = processor.document_query(docID=RAW['docID'], source=RAW['source'])
    assert queryset

    # Create a new doucment to be renormalized
    new_raw = copy.deepcopy(RAW)
    new_raw.attributes['docID'] = 'get_the_tables'
    new_raw.attributes['doc'] = new_raw.attributes['doc'].encode('utf-8')

    # This is basically like running the improved harvester right?
    DocumentModel.create(**new_raw.attributes).save()

    tasks.migrate(renormalize, sources=[RAW['source']], dry=False)

    queryset = processor.document_query(docID='get_the_tables', source=RAW['source'])
    assert len(queryset) == 1
    scrapi.processing.elasticsearch.es = real_es


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.parametrize('processor_name', ['postgres'])
def test_cassandra_to_postgres(processor_name):
    real_es = scrapi.processing.elasticsearch.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()

    processor = get_processor(processor_name)
    processor.process_raw(RAW)
    processor.process_normalized(RAW, NORMALIZED)

    cassandra_queryset = processor.document_query(docID=RAW['docID'], source=RAW['source'])
    postgres_queryset = processor.document_query(source=RAW['source'], docID=RAW['docID'])

    assert len(cassandra_queryset) == 1
    assert len(postgres_queryset) == 0
    tasks.migrate(cross_db, target_db='postgres', dry=False, sources=['test'])

    postgres_queryset = processor.document_query(source=RAW['source'], docID=RAW['docID'])
    assert len(postgres_queryset) == 1
    scrapi.processing.elasticsearch.es = real_es


@pytest.mark.cassandra
@pytest.mark.elasticsearch
@pytest.mark.parametrize('processor_name', ['postgres'])
def test_cassandra_to_elasticsearch(processor_name):

    results = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es.search(index='test', doc_type=RAW['source'])

    assert (len(results['hits']['hits']) == 0)

    processor = get_processor(processor_name)

    # create the cassandra documents
    processor.process_raw(RAW)
    processor.process_normalized(RAW, NORMALIZED)

    # run the migration
    tasks.migrate(cross_db, target_db='elasticsearch', dry=False, sources=['test'], index='test')

    # check the elasticsearch results
    results = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es.search(index='test', doc_type=RAW['source'])
    # import ipdb; ipdb.set_trace()
    assert (len(results['hits']['hits']) == 1)
