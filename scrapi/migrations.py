import logging

from scrapi import tasks
from scrapi import settings
from scrapi.linter import RawDocument
from scrapi.processing.elasticsearch import es

logger = logging.getLogger()


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def rename(doc, **kwargs):
    source = kwargs.get('source')
    target = kwargs.get('target')
    assert source and target, "To run this migration you need both a source and a target"
    assert source != target, "Can't rename {} to {}, names are the same".format(source, target)

    raw = RawDocument({
        'doc': doc.doc,
        'docID': doc.docID,
        'source': target,
        'filetype': doc.filetype,
        'timestamps': doc.timestamps,
        'versions': doc.versions
    })
    if not kwargs.get('dry'):
        tasks.process_raw(raw)
        tasks.process_normalized(tasks.normalize(raw, raw['source']), raw)
        logger.info('Processed document from {} with id {}'.format(source, raw['docID']))

        es.delete(index=settings.ELASTIC_INDEX, doc_type=source, id=raw['docID'], ignore=[404])
        es.delete(index='share_v1', doc_type=source, id=raw['docID'], ignore=[404])

    logger.info('Deleted document from {} with id {}'.format(source, raw['docID']))
