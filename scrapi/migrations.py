import time
import logging

from six.moves import xrange
from cassandra.cqlengine.query import Token

from scrapi import tasks
from scrapi import registry
from scrapi import settings
from scrapi.database import setup
from scrapi.linter import RawDocument
from scrapi.processing.elasticsearch import es
from scrapi.processing.cassandra import DocumentModel, DocumentModelOld


logger = logging.getLogger()


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def rename(docs, target=None, **kwargs):
    assert target, "To run this migration you need a target."
    for doc in docs:
        raw = RawDocument({
            'doc': doc.doc,
            'docID': doc.docID,
            'source': target,
            'filetype': doc.filetype,
            'timestamps': doc.timestamps,
            'versions': doc.versions
        })

        assert doc.source != target, "Can't rename {} to {}, names are the same.".format(doc.source, target)

        if not kwargs.get('dry'):
            tasks.process_raw(raw)
            tasks.process_normalized(tasks.normalize(raw, raw['source']), raw)
            logger.info('Processed document from {} with id {}'.format(doc.source, raw['docID']))

            es.delete(index=settings.ELASTIC_INDEX, doc_type=doc.source, id=raw['docID'], ignore=[404])
            es.delete(index='share_v1', doc_type=doc.source, id=raw['docID'], ignore=[404])

        logger.info('Deleted document from {} with id {}'.format(doc.source, raw['docID']))


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
        es.delete(index=settings.ELASTIC_INDEX, doc_type=sources, id=doc.docID, ignore=[404])
        es.delete(index='share_v1', doc_type=sources, id=doc.docID, ignore=[404])

        logger.info('Deleted document from {} with id {}'.format(sources, doc.docID))


@tasks.task_autoretry(default_retry_delay=30, max_retries=5)
def document_v2_migration(doc, dry=True):
    logger.warning(
        'This function has been deprecated, and will be removed in future versions of scrapi'
    )
    if not dry:
        try_n_times(5, DocumentModel.create, **dict(doc)).save()


def ModelIteratorFactory(model, next_page, default_args=None):
    def model_iterator(*sources):
        sources = sources or default_args
        q = model.objects.timeout(500).allow_filtering().all().limit(1000)
        querysets = (q.filter(source=source) for source in sources) if sources else [q]
        for query in querysets:
            page = try_n_times(5, list, query)
            while len(page) > 0:
                for doc in page:
                    yield doc
                page = try_n_times(5, next_page, query, page)
    return model_iterator


def next_page_old(query, page):
    return list(query.filter(pk__token__gt=Token(page[-1].pk)))


def next_page_source_partition(query, page):
    return list(query.filter(docID__gt=page[-1].docID))

documents_old = ModelIteratorFactory(DocumentModelOld, next_page_old)
documents = ModelIteratorFactory(DocumentModel, next_page_source_partition, default_args=registry.keys())


def try_n_times(n, action, *args, **kwargs):
    for _ in xrange(n):
        try:
            return action(*args, **kwargs)
        except Exception as e:
            logger.exception(e)
            time.sleep(15)
            connection_open = setup(force=True, sync=False)
            logger.info("Trying again... Cassandra connection open: {}".format(connection_open))
    if e:
        raise e
