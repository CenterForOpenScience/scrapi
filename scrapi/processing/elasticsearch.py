from __future__ import absolute_import
import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionError
from elasticsearch_dsl import DocType, String, Date, Object

from scrapi import settings
from scrapi.processing.base import BaseProcessor
from scrapi.base.transformer import JSONTransformer


logger = logging.getLogger(__name__)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('requests').setLevel(logging.WARN)
logging.getLogger('elasticsearch').setLevel(logging.FATAL)
logging.getLogger('elasticsearch.trace').setLevel(logging.FATAL)


try:
    # If we cant connect to elastic search dont define this class
    es = Elasticsearch(settings.ELASTIC_URI, request_timeout=settings.ELASTIC_TIMEOUT)

    # body = {
    #     'mappings': {
    #         harvester: settings.ES_SEARCH_MAPPING
    #         for harvester in registry.keys()
    #     }
    # }

    es.cluster.health(wait_for_status='yellow')
    es.indices.create(index=settings.ELASTIC_INDEX, body={}, ignore=400)
    es.indices.create(index='share_v1', ignore=400)
except ConnectionError:  # pragma: no cover
    logger.error('Could not connect to Elasticsearch, expect errors.')
    if 'elasticsearch' in settings.NORMALIZED_PROCESSING or 'elasticsearch' in settings.RAW_PROCESSING:
        raise


class ElasticsearchProcessor(BaseProcessor):
    NAME = 'elasticsearch'

    def process_normalized(self, raw_doc, normalized, index=settings.ELASTIC_INDEX):
        data = {
            key: value for key, value in normalized.attributes.items()
            if key in settings.FRONTEND_KEYS
        }
        data['providerUpdatedDateTime'] = self.version(raw_doc, normalized)

        es.index(
            body=data,
            refresh=True,
            index=index,
            doc_type=raw_doc['source'],
            id=raw_doc['docID'],
        )
        self.process_normalized_v1(raw_doc, normalized, data['providerUpdatedDateTime'])

    def version(self, raw, normalized):
        try:
            old_doc = es.get_source(
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
        es.index(
            body=data,
            refresh=True,
            index=index,
            doc_type=raw_doc['source'],
            id=raw_doc['docID']
        )


class PreserveOldContributors(JSONTransformer):
    schema = {
        'given': '/givenName',
        'family': '/familyName',
        'middle': '/additionalName',
        'email': '/email'
    }

    def process_contributors(self, contributors):
        return [self.transform(contributor) for contributor in contributors]


class PreserveOldSchema(JSONTransformer):
    schema = {
        'title': '/title',
        'description': '/description',
        'tags': ('/tags', lambda x: x or []),
        'contributors': ('/contributors', PreserveOldContributors().process_contributors),
        'dateUpdated': '/providerUpdatedDateTime',
        'source': '/shareProperties/source',
        'id': {
            'url': '/uris/canonicalUri'
        }
    }


URI_FIELD = String(fields={
    'raw': String(index='not_analyzed')
})


CONTRIBUTOR = Object(
    properties={
        'name': String(),
        'email': URI_FIELD,
        'sameAs': URI_FIELD,
        'familyName': String(),
        'additionalName': String(),
        'givenName': String(),
        'affiliation': URI_FIELD
    }
)


class Normalized(DocType):

    title = String()
    description = String()
    tags = String(index='not_analyzed')
    subjects = String(index='not_analyzed')

    uris = Object(
        properties={
            'canonicalUri': URI_FIELD,
            'objectUris': URI_FIELD,
            'descriptorUris': URI_FIELD,
            'providerUris': URI_FIELD
        }
    )
    contributors = CONTRIBUTOR
    providerUpdatedDateTime = Date()
    description = String()
    freeToRead = Object(
        properties={
            'startDate': Date(),
            'endDate': Date()
        }
    )
    languages = String()
    licenses = Object(
        properties={
            'uri': URI_FIELD,
            'description': String(),
            'startDate': Date(),
            'endDate': Date()
        }
    )
    publisher = CONTRIBUTOR
    sponsorships = Object(
        properties={
            'award': Object(
                properties={
                    'awardName': String(),
                    'awardIdentifier': URI_FIELD
                }
            ),
            'sponsor': Object(
                properties={
                    'sponsorName': String(),
                    'sponsorIdentifier': URI_FIELD
                }
            )
        }
    )
    otherProperties = Object()
    shareProperties = Object()
