from __future__ import absolute_import

import logging

import six

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionError

from scrapi import settings
from scrapi.base.transformer import JSONTransformer
from scrapi.processing import DocumentTuple
from scrapi.processing.base import BaseProcessor, BaseDatabaseManager
from scrapi.linter import NormalizedDocument


logger = logging.getLogger(__name__)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('requests').setLevel(logging.WARN)
logging.getLogger('elasticsearch').setLevel(logging.FATAL)
logging.getLogger('elasticsearch.trace').setLevel(logging.FATAL)


class DatabaseManager(BaseDatabaseManager):

    def __init__(self, uri=None, timeout=None, index=None, **kwargs):
        self.uri = uri or settings.ELASTIC_URI
        self.index = index or settings.ELASTIC_INDEX
        self.es = None
        self.kwargs = {
            'timeout': timeout or settings.ELASTIC_TIMEOUT,
            'retry_on_timeout': True
        }
        self.kwargs.update(kwargs)

    def setup(self):
        '''Sets up the database connection. Returns True if the database connection
            is successful, False otherwise
        '''
        try:
            # If we cant connect to elastic search dont define this class
            self.es = Elasticsearch(self.uri, **self.kwargs)

            self.es.cluster.health(wait_for_status='yellow')
            self.es.indices.create(index=self.index, body={}, ignore=400)
            self.es.indices.create(index='share_v1', ignore=400)
            return True
        except ConnectionError:  # pragma: no cover
            logger.error('Could not connect to Elasticsearch, expect errors.')
            return False

    def tear_down(self):
        '''since it's just http, doesn't do much
        '''
        pass

    def clear(self, force=False):
        assert force, 'Force must be called to clear the database'
        assert self.index != settings.ELASTIC_INDEX, 'Cannot erase the production database'
        self.es.indices.delete(index=self.index, ignore=[400, 404])

    def celery_setup(self, *args, **kwargs):
        pass


class ElasticsearchProcessor(BaseProcessor):
    NAME = 'elasticsearch'

    manager = DatabaseManager()

    def documents(self, *sources):
        raise NotImplementedError

    def process_normalized(self, raw_doc, normalized, index=None):
        index = index or settings.ELASTIC_INDEX
        data = {
            key: value for key, value in normalized.attributes.items()
            if key in (settings.FRONTEND_KEYS or normalized.attributes.keys())
        }
        data['providerUpdatedDateTime'] = self.version(raw_doc, normalized)

        self.manager.es.index(
            body=data,
            refresh=True,
            index=index,
            doc_type=raw_doc['source'],
            id=raw_doc['docID'],
        )
        self.process_normalized_v1(raw_doc, normalized, data['providerUpdatedDateTime'])

    def version(self, raw, normalized):
        try:
            old_doc = self.manager.es.get_source(
                index=settings.ELASTIC_INDEX,
                doc_type=raw['source'],
                id=raw['docID']
            )
        except NotFoundError:  # pragma: no cover
            # Normally I don't like exception-driven logic,
            # but this was the best way to handle missing
            # types, indices and documents together
            date = normalized['providerUpdatedDateTime']
        else:
            date = old_doc.get('providerUpdatedDateTime') or normalized['providerUpdatedDateTime']

        return date

    def process_normalized_v1(self, raw_doc, normalized, date):
        index = 'share_v1'
        transformer = PreserveOldSchema()
        data = transformer.transform(normalized.attributes)
        data['providerUpdatedDateTime'] = date
        self.manager.es.index(
            body=data,
            refresh=True,
            index=index,
            doc_type=raw_doc['source'],
            id=raw_doc['docID']
        )

    def get(self, docID, index, source):
        try:
            results = self.manager.es.get_source(
                index=index,
                doc_type=source,
                id=docID
            )
        except NotFoundError:
            return None

        return DocumentTuple(None, NormalizedDocument(results, validate=False, clean=False))


class PreserveOldContributors(JSONTransformer):
    schema = {
        'given': '/givenName',
        'family': '/familyName',
        'middle': '/additionalName',
        'email': '/email'
    }

    def process_contributors(self, contributors):
        if contributors:
            return [self.transform(contributor) for contributor in contributors]


class PreserveOldSchema(JSONTransformer):
    @property
    def schema(self):
        return {
            'title': '/title',
            'description': '/description',
            'tags': ('/tags', lambda x: x or []),
            'contributors': ('/contributors', PreserveOldContributors().process_contributors),
            'dateUpdated': '/providerUpdatedDateTime',
            'source': '/shareProperties/source',
            'id': {
                'url': ('/uris/canonicalUri', '/uris/descriptorUri', '/uris/providerUris', '/uris/objectUris', self.process_uris)
            }
        }

    def process_uris(self, *uris):
        for uri in filter(lambda x: x, uris):
            return uri if isinstance(uri, six.string_types) else uri[0]
