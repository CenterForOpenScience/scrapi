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
        new_doc = copy.deepcopy(doc.raw.attributes)
        new_doc['source'] = target

        raw = RawDocument(new_doc, validate=False)

        assert doc.raw.attributes['source'] != target, "Can't rename {} to {}, names are the same.".format(doc.raw['source'], target)

        if not kwargs.get('dry'):
            tasks.process_raw(raw)
            tasks.process_normalized(tasks.normalize(raw, raw['source']), raw)
            logger.info('Processed document from {} with id {}'.format(doc.raw.attributes['source'], raw['docID']))

            es_processor = get_processor('elasticsearch')
            es_processor.manager.es.delete(index=settings.ELASTIC_INDEX, doc_type=doc.raw.attributes['source'], id=raw['docID'], ignore=[404])
            es_processor.manager.es.delete(index='share_v1', doc_type=doc.raw.attributes['source'], id=raw['docID'], ignore=[404])

        logger.info('Renamed document from {} to {} with id {}'.format(doc.raw.attributes['source'], target, raw['docID']))


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def cross_db(docs, source_db=None, target_db=None, index=None, versions=False, **kwargs):
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
    source_processor = get_processor(source_db or settings.CANONICAL_PROCESSOR)
    target_processor = get_processor(target_db)
    for doc in docs:
        try:
            if not doc.raw['doc']:
                # corrupted database item has no doc element
                message = 'No doc element in raw doc -- could not migrate document from {} with id {}'.format(doc.raw.attributes['source'], doc.raw.attributes['docID'])
                log_to_sentry(message)
                logger.info(message)
                continue

            raw, normalized = doc.raw, doc.normalized

            if not kwargs.get('dry'):
                if versions:
                    for raw_version, norm_version in source_processor.get_versions(raw['source'], raw['docID']):
                        target_processor.process_raw(raw_version)
                        if norm_version:
                            target_processor.process_normalized(raw_version, norm_version)
                        else:
                            logger.info('Not storing migrated normalized version from {} with id {}, document is not in approved set list.'.format(raw.attributes['source'], raw.attributes['docID']))
                else:
                    target_processor.process_raw(raw)
                    if normalized:
                        target_processor.process_normalized(raw, normalized)
                    else:
                        logger.info('Not storing migrated normalized from {} with id {}, document is not in approved set list.'.format(raw.attributes['source'], raw.attributes['docID']))
        except Exception as e:
            logger.exception(e)
            log_to_sentry(e)


@tasks.task_autoretry(default_retry_delay=1, max_retries=5)
def renormalize(docs, *args, **kwargs):
    for doc in docs:
        if not kwargs.get('dry'):
            tasks.process_normalized(tasks.normalize(doc.raw, doc.raw['source']), doc.raw)


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def delete(docs, sources=None, **kwargs):
    for doc in docs:
        assert sources, "To run this migration you need a source."

        processor = get_processor(settings.CANONICAL_PROCESSOR)
        processor.delete(source=doc.raw.attributes['source'], docID=doc.raw.attributes['docID'])

        es_processor = get_processor('elasticsearch')
        es_processor.manager.es.delete(index=settings.ELASTIC_INDEX, doc_type=sources, id=doc.raw.attributes['docID'], ignore=[404])
        es_processor.manager.es.delete(index='share_v1', doc_type=sources, id=doc.raw.attributes['docID'], ignore=[404])

        logger.info('Deleted document from {} with id {}'.format(sources, doc.raw.attributes['docID']))
