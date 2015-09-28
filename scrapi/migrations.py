import copy
import logging

from scrapi import tasks
from scrapi import settings
from scrapi.linter import RawDocument
from scrapi.events import log_to_sentry
from scrapi.processing import get_processor


logger = logging.getLogger()


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def rename(docs, target=None, **kwargs):
    assert target, "To run this migration you need a target."

    for doc in docs:
        raw_doc = doc.raw
        new_doc = copy.deepcopy(raw_doc.attributes)
        new_doc['source'] = target

        raw = RawDocument(new_doc, validate=False)

        assert raw_doc.attributes['source'] != target, "Can't rename {} to {}, names are the same.".format(raw_doc['source'], target)

        if not kwargs.get('dry'):
            tasks.process_raw(raw)
            tasks.process_normalized(tasks.normalize(raw, raw['source']), raw)
            logger.info('Processed document from {} with id {}'.format(raw_doc.attributes['source'], raw['docID']))

            es_processor = get_processor('elasticsearch')
            es_processor.manager.es.delete(index=settings.ELASTIC_INDEX, doc_type=raw_doc.attributes['source'], id=raw['docID'], ignore=[404])
            es_processor.manager.es.delete(index='share_v1', doc_type=raw_doc.attributes['source'], id=raw['docID'], ignore=[404])

        logger.info('Renamed document from {} to {} with id {}'.format(raw_doc.attributes['source'], target, raw['docID']))


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def cross_db(docs, target_db=None, index=None, **kwargs):
    """
    Migration to go between
        cassandra > postgres
        postgres > cassandra
        cassandra > elasticsearch
        postgres > elasticsearch

    source db can be passed in to the migrate task, and will default to the CANONICAL_PROCESSOR specified in settings
    target_db will be specified when the task is called
    """
    assert target_db, 'Please specify a target db for the migration -- either postgres or elasticsearch'
    assert target_db in ['postgres', 'cassandra', 'elasticsearch'], 'Invalid target database - please specify either postgres, cassandra or elasticsearch'
    for doc in docs:
        if not doc.raw.attributes['doc']:
            # corrupted database item has no doc element
            message = 'Could not migrate document from {} with id {}'.format(doc.raw.attributes['source'], doc.raw.attributes['docID'])
            log_to_sentry(message)
            logger.info(message)
            continue

        raw = doc.raw
        normalized = doc.normalized

        target_processor = get_processor(target_db)

        if not kwargs.get('dry'):
            target_processor.process_raw(raw)

            try:
                if target_db == 'elasticsearch':
                    target_processor.process_normalized(raw, normalized, index=index)
                else:
                    target_processor.process_normalized(raw, normalized)
            except AttributeError:
                # This means that the document was harvested but wasn't approved to be normalized
                logger.info('Not storing migrated normalized from {} with id {}, document is not in approved set list.'.format(raw.attributes['source'], raw.attributes['docID']))


@tasks.task_autoretry(default_retry_delay=1, max_retries=5)
def renormalize(docs, *args, **kwargs):
    for doc in docs:
        raw_doc = doc.raw
        if not kwargs.get('dry'):
            tasks.process_normalized(tasks.normalize(raw_doc, raw_doc['source']), raw_doc)


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def delete(docs, sources=None, **kwargs):
    for doc in docs:
        raw_doc = doc.raw
        assert sources, "To run this migration you need a source."

        processor = get_processor(settings.CANONICAL_PROCESSOR)
        processor.delete(source=raw_doc.attributes['source'], docID=raw_doc.attributes['docID'])

        es_processor = get_processor('elasticsearch')
        es_processor.manager.es.delete(index=settings.ELASTIC_INDEX, doc_type=sources, id=raw_doc.attributes['docID'], ignore=[404])
        es_processor.manager.es.delete(index='share_v1', doc_type=sources, id=raw_doc.attributes['docID'], ignore=[404])

        logger.info('Deleted document from {} with id {}'.format(sources, raw_doc.attributes['docID']))
