import logging

from scrapi import tasks
from scrapi import settings
from scrapi.linter import RawDocument
from scrapi.processing.elasticsearch import es

logger = logging.getLogger()


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def rename(doc, target=None, **kwargs):
    assert target, "To run this migration you need both a source and a target"

    raw = RawDocument({
        'doc': doc.doc,
        'docID': doc.docID,
        'source': target,
        'filetype': doc.filetype,
        'timestamps': doc.timestamps,
        'versions': doc.versions
    })

    assert doc.source != target, "Can't rename {} to {}, names are the same".format(doc.source, target)

    if not kwargs.get('dry'):
        tasks.process_raw(raw)
        tasks.m(tasks.normalize(raw, raw['source']), raw)
        logger.info('Processed document from {} with id {}'.format(doc.source, raw['docID']))

        es.delete(index=settings.ELASTIC_INDEX, doc_type=doc.source, id=raw['docID'], ignore=[404])
        es.delete(index='share_v1', doc_type=doc.source, id=raw['docID'], ignore=[404])

    logger.info('Deleted document from {} with id {}'.format(doc.source, raw['docID']))


@tasks.task_autoretry(default_retry_delay=1, max_retries=1)
def renormalize(doc, source=None, **kwargs):
    logger.info(doc.doc)
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
def delete(doc, source=None, **kwargs):
    assert source, "To run this migration you need a source."
    doc.timeout(5).delete()
    es.delete(index=settings.ELASTIC_INDEX, doc_type=source, id=doc.docID, ignore=[404])
    es.delete(index='share_v1', doc_type=source, id=doc.docID, ignore=[404])

    logger.info('Deleted document from {} with id {}'.format(source, doc.docID))
