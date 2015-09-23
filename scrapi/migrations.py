import json
import logging
import copy

from scrapi import tasks
from scrapi import settings
from scrapi.events import log_to_sentry
from scrapi.processing import get_processor
from scrapi.linter import RawDocument, NormalizedDocument


logger = logging.getLogger()


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def rename(docs, target=None, **kwargs):
    assert target, "To run this migration you need a target."

    for doc in docs:
        new_doc = copy.copy(doc.attributes)
        new_doc['source'] = target

        raw = RawDocument(new_doc, validate=False)

        assert doc.attributes['source'] != target, "Can't rename {} to {}, names are the same.".format(doc['source'], target)

        if not kwargs.get('dry'):
            tasks.process_raw(raw)
            tasks.process_normalized(tasks.normalize(raw, raw['source']), raw)
            logger.info('Processed document from {} with id {}'.format(doc.attributes['source'], raw['docID']))

            es_processor = get_processor('elasticsearch')
            es_processor.manager.es.delete(index=settings.ELASTIC_INDEX, doc_type=doc.attributes['source'], id=raw['docID'], ignore=[404])
            es_processor.manager.es.delete(index='share_v1', doc_type=doc.attributes['source'], id=raw['docID'], ignore=[404])

        logger.info('Renamed document from {} to {} with id {}'.format(doc.attributes['source'], target, raw['docID']))


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def cross_db(docs, target_db=None, index=None, **kwargs):
    """
    Migration to go between cassandra > postgres, or cassandra > elasticsearch.
    TODO - make this source_db agnostic. Should happen along with larger migration refactor
    """
    assert target_db, 'Please specify a target db for the migration -- either postgres or elasticsearch'
    # assert target_db in ['postgres', 'elasticsearch'], 'Invalid target database - please specify either postgres or elasticsearch'

    for doc in docs:

        if not doc.doc:
            # corrupted database item has no doc element
            message = 'Could not migrate document from {} with id {}'.format(doc.source, doc.docID)
            log_to_sentry(message)
            logger.info(message)
            continue

        raw = RawDocument({
            'doc': doc.doc,
            'docID': doc.docID,
            'source': doc.source,
            'filetype': doc.filetype,
            'timestamps': doc.timestamps,
            'versions': doc.versions
        }, validate=False)

        normed = doc_to_normed_dict(doc)

        # Create the normalized, don't validate b/c its been done once already
        normalized = NormalizedDocument(normed, validate=False)

        processor = get_processor(target_db)

        if not kwargs.get('dry'):
            processor.process_raw(raw)

            try:
                if target_db == 'elasticsearch':
                    processor.process_normalized(raw, normalized, index=index)
                else:
                    processor.process_normalized(raw, normalized)
            except KeyError:
                # This means that the document was harvested but wasn't approved to be normalized
                logger.info('Not storing migrated normalized from {} with id {}, document is not in approved set list.'.format(doc.source, doc.docID))


@tasks.task_autoretry(default_retry_delay=1, max_retries=5)
def renormalize(docs, *args, **kwargs):
    for doc in docs:
        raw = RawDocument({
            'doc': doc.doc,
            'docID': doc.docID,
            'source': doc.source,
            'filetype': doc.filetype,
            'timestamps': doc.timestamps,
            'versions': doc.versions
        })
        if not kwargs.get('dry'):
            tasks.process_normalized(tasks.normalize(raw, raw['source']), raw)


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def delete(docs, sources=None, **kwargs):
    for doc in docs:
        assert sources, "To run this migration you need a source."
        doc.timeout(5).delete()
        es_processor = get_processor('elasticsearch')
        es_processor.manager.es.delete(index=settings.ELASTIC_INDEX, doc_type=sources, id=doc.docID, ignore=[404])
        es_processor.manager.es.delete(index='share_v1', doc_type=sources, id=doc.docID, ignore=[404])

        logger.info('Deleted document from {} with id {}'.format(sources, doc.docID))


def doc_to_normed_dict(doc):
    # make the new dict actually contain real items
    normed = {}
    for key, value in dict(doc).items():
        try:
            normed[key] = json.loads(value)
        except (ValueError, TypeError):
            normed[key] = value

    # Remove empty values and strip down to only normalized fields
    do_not_include = ['docID', 'doc', 'filetype', 'timestamps', 'source']
    for key in list(normed.keys()):
        if not normed[key]:
            del normed[key]

    for key in list(normed.keys()):
        if key in do_not_include:
            del normed[key]

    if normed.get('versions'):
        normed['versions'] = list(map(str, normed['versions']))

    # No datetime means the document wasn't normalized (probably wasn't on the approved list)
    if normed.get('providerUpdatedDateTime'):
        normed['providerUpdatedDateTime'] = normed['providerUpdatedDateTime'].isoformat()

    return normed
