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

    queryset = processor.get(source=RAW['source'], docID=RAW['docID'])

    old_source = NORMALIZED['shareProperties']['source']

    assert queryset.normalized.attributes['shareProperties']['source'] == utils.RECORD['shareProperties']['source']
    assert queryset.normalized.attributes['shareProperties']['source'] == old_source

    new_record = copy.deepcopy(utils.RECORD)
    new_record['shareProperties']['source'] = 'wwe_news'
    test_harvester.short_name = 'wwe_news'
    registry['wwe_news'] = test_harvester

    tasks.migrate(rename, sources=[old_source], target='wwe_news', dry=False)

    queryset = processor.get(source='wwe_news', docID=RAW['docID'])

    assert queryset.normalized.attributes['shareProperties']['source'] == 'wwe_news'

    scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es = real_es
    test_harvester.short_name = RAW['source']
    registry['test'] = test_harvester
    del registry['wwe_news']


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.parametrize('processor_name', ['postgres', 'cassandra'])
def test_delete(processor_name, monkeypatch):
    real_es = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()

    monkeypatch.setattr('scrapi.settings.CANONICAL_PROCESSOR', processor_name)

    print('Canonical Processor is {}'.format(scrapi.settings.CANONICAL_PROCESSOR))
    processor = get_processor(processor_name)
    processor.process_raw(RAW)
    processor.process_normalized(RAW, NORMALIZED)

    queryset = processor.get(docID=RAW['docID'], source=RAW['source'])
    assert queryset

    tasks.migrate(delete, sources=[RAW['source']], dry=False)
    queryset = processor.get(docID=RAW['docID'], source=RAW['source'])
    assert not queryset
    scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es = real_es


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.parametrize('processor_name', ['postgres', 'cassandra'])
def test_renormalize(processor_name, monkeypatch):
    # Set up
    # real_es = scrapi.processing.elasticsearch.es
    real_es = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es

    scrapi.processing.elasticsearch.es = mock.MagicMock()
    monkeypatch.setattr('scrapi.settings.CANONICAL_PROCESSOR', processor_name)

    # Process raw and normalized with fake docs
    processor = get_processor(processor_name)
    processor.process_raw(RAW)
    processor.process_normalized(RAW, NORMALIZED)

    # Check to see those docs were processed
    queryset = processor.get(docID=RAW['docID'], source=RAW['source'])
    assert queryset

    # Create a new doucment to be renormalized
    new_raw = copy.deepcopy(RAW)
    new_raw.attributes['docID'] = 'get_the_tables'
    new_raw.attributes['doc'] = new_raw.attributes['doc'].encode('utf-8')

    # This is basically like running the improved harvester right?
    processor.create(new_raw.attributes)

    tasks.migrate(renormalize, sources=[RAW['source']], dry=False)

    queryset = processor.get(docID='get_the_tables', source=RAW['source'])
    assert queryset
    scrapi.processing.elasticsearch.es = real_es
    processor.delete(docID='get_the_tables', source=RAW['source'])


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.parametrize('canonical', ['postgres', 'cassandra'])
@pytest.mark.parametrize('destination', ['postgres', 'cassandra'])
def test_cross_db_with_versions(canonical, destination, monkeypatch, index='test'):
    new_title = 'How to be really good at Zoo Tycoon: The Definitive Guide'

    if canonical == destination:
        return

    monkeypatch.setattr('scrapi.settings.CANONICAL_PROCESSOR', canonical)

    # Get the test documents into the canonical processor
    canonical_processor = get_processor(canonical)
    canonical_processor.process_raw(RAW)
    canonical_processor.process_normalized(RAW, NORMALIZED)

    # Get a version in there too
    new_normalized = copy.deepcopy(NORMALIZED.attributes)
    new_normalized['title'] = new_title
    canonical_processor.process_normalized(RAW, NormalizedDocument(new_normalized))

    destination_processor = get_processor(destination)

    # Check to see canonical_processor versions are there, and destination are not
    canonical_versions = list(canonical_processor.get_versions(docID=RAW['docID'], source=RAW['source']))
    assert len(canonical_versions) == 3
    assert canonical_versions[1].normalized['title'] == NORMALIZED['title']
    assert canonical_versions[2].normalized['title'] == new_title

    destination_doc = destination_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert not destination_doc

    # Migrate from the canonical to the destination
    tasks.migrate(cross_db, target_db=destination, dry=False, sources=['test'], index=index, versions=True)

    # Check to see if the document made it to the destinaton, and is still in the canonical
    destination_versions = list(destination_processor.get_versions(docID=RAW['docID'], source=RAW['source']))
    assert len(destination_versions) == 3
    assert destination_versions[1].normalized['title'] == NORMALIZED['title']
    assert destination_versions[2].normalized['title'] == new_title

    canonical_doc = canonical_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert canonical_doc


@pytest.mark.django_db
@pytest.mark.cassandra
@pytest.mark.elasticsearch
@pytest.mark.parametrize('canonical', ['postgres', 'cassandra'])
@pytest.mark.parametrize('destination', ['postgres', 'cassandra', 'elasticsearch'])
def test_cross_db(canonical, destination, monkeypatch, index='test'):

    if canonical == destination:
        return

    monkeypatch.setattr('scrapi.settings.CANONICAL_PROCESSOR', canonical)

    if destination != 'elasticsearch':
        real_es = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es
        scrapi.processing.elasticsearch.es = mock.MagicMock()
    else:
        monkeypatch.setattr('scrapi.settings.ELASTIC_INDEX', 'test')

    # Get the test documents into the caonical processor
    canonical_processor = get_processor(canonical)
    canonical_processor.process_raw(RAW)
    canonical_processor.process_normalized(RAW, NORMALIZED)

    destination_processor = get_processor(destination)

    # Check to see canonical_processor is there, and destination is not
    canonical_doc = canonical_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert canonical_doc

    if destination != 'elasticsearch':
        destination_doc = destination_processor.get(docID=RAW['docID'], source=RAW['source'])
        assert not destination_doc
    else:
        destination_doc = destination_processor.get(docID=RAW['docID'], index=index, source=RAW['source'])
        assert not destination_doc

    # Migrate from the canonical to the destination
    tasks.migrate(cross_db, target_db=destination, dry=False, sources=['test'], index=index)

    # Check to see if the document made it to the destinaton, and is still in the canonical
    if destination != 'elasticsearch':
        destination_doc = destination_processor.get(docID=RAW['docID'], source=RAW['source'])
        assert destination_doc
    else:
        destination_doc = destination_processor.get(docID=RAW['docID'], index=index, source=RAW['source'])
        assert destination_doc.normalized

    canonical_doc = canonical_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert canonical_doc

    if destination != 'elasticsearch':
        scrapi.processing.elasticsearch.es = real_es


@pytest.mark.django_db
@pytest.mark.parametrize('destination', ['postgres', 'cassandra'])
def test_no_normed_cross_db(destination, index='test'):

    if scrapi.settings.CANONICAL_PROCESSOR == destination:
        return

    real_es = scrapi.processing.elasticsearch.ElasticsearchProcessor.manager.es
    scrapi.processing.elasticsearch.es = mock.MagicMock()

    # Get the test documents into the caonical processor, but don't process normalized
    canonical_processor = get_processor(scrapi.settings.CANONICAL_PROCESSOR)
    canonical_processor.process_raw(RAW)
    # canonical_processor.process_normalized(RAW, NORMALIZED)

    destination_processor = get_processor(destination)

    # Check to see canonical_processor raw is there, and destination is not
    canonical_doc = canonical_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert canonical_doc.raw
    assert not canonical_doc.normalized

    destination_doc = destination_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert not destination_doc

    # # Migrate from the canonical to the destination
    tasks.migrate(cross_db, target_db=destination, dry=False, sources=['test'], index=index)

    # Check to see if the document didn't make made it to the destinaton, and is still in the canonical
    destination_doc = destination_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert not destination_doc.normalized

    canonical_doc = canonical_processor.get(docID=RAW['docID'], source=RAW['source'])
    assert canonical_doc

    scrapi.processing.elasticsearch.es = real_es
