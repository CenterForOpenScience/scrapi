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
from scrapi.migrations import cross_db
from scrapi.migrations import renormalize
from scrapi.migrations import DocumentModelOld

# Need to force cassandra to ignore set keyspace
from scrapi.processing.postgres import PostgresProcessor, Document
from scrapi.processing.cassandra import CassandraProcessor, DocumentModel
from scrapi.processing.elasticsearch import ElasticsearchProcessor

from . import utils

test_cass = CassandraProcessor()
test_postgres = PostgresProcessor()
test_es = ElasticsearchProcessor()

test_harvester = utils.TestHarvester()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = test_harvester.harvest()[0]


@pytest.fixture
def harvester():
    pass  # Need to override this


@pytest.mark.django_db
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
    test_harvester.short_name = RAW['source']
    registry['test'] = test_harvester
    del registry['wwe_news']


@pytest.mark.django_db
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


@pytest.mark.django_db
@pytest.mark.cassandra
def test_renormalize():
    # Set up
    real_es = scrapi.processing.elasticsearch.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()

    # Process raw and normalized with fake docs
    test_cass.process_raw(RAW)
    test_cass.process_normalized(RAW, NORMALIZED)

    # Check to see those docs were processed
    queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    assert len(queryset) == 1

    # Create a new doucment to be renormalized
    new_raw = copy.deepcopy(RAW)
    new_raw.attributes['docID'] = 'get_the_tables'
    new_raw.attributes['doc'] = new_raw.attributes['doc'].encode('utf-8')

    # This is basically like running the improved harvester right?
    DocumentModel.create(**new_raw.attributes).save()

    tasks.migrate(renormalize, sources=[RAW['source']], dry=False)

    queryset = DocumentModel.objects(docID='get_the_tables', source=RAW['source'])
    assert len(queryset) == 1
    scrapi.processing.elasticsearch.es = real_es


@pytest.mark.django_db
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


@pytest.mark.django_db
@pytest.mark.cassandra
def test_cassandra_to_postgres():

    test_cass.process_raw(RAW)
    test_cass.process_normalized(RAW, NORMALIZED)

    cassandra_queryset = DocumentModel.objects(docID=RAW['docID'], source=RAW['source'])
    postgres_queryset = Document.objects.filter(source=RAW['source'], docID=RAW['docID'])
    assert len(cassandra_queryset) == 1
    assert len(postgres_queryset) == 0
    tasks.migrate(cross_db, target_db='postgres', dry=False, sources=['test'])

    postgres_queryset = Document.objects.filter(source=RAW['source'], docID=RAW['docID'])
    assert len(postgres_queryset) == 1


@pytest.mark.django_db
@pytest.mark.elasticsearch
def test_cassandra_to_elasticsearch():

    # Make sure es has nothing in it
    results = test_es.search(index='test', doc_type=RAW['source'])
    assert (len(results['hits']['hits']) == 0)

    # create the postgres documents
    test_cass.process_raw(RAW)
    test_cass.process_normalized(RAW, NORMALIZED)

    # run the migration
    tasks.migrate(cross_db, target_db='elasticsearch', dry=False)

    # check the elasticsearch results
    results = test_es.search(index='test', doc_type=RAW['source'])
    assert (len(results['hits']['hits']) == 1)
