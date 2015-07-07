from __future__ import absolute_import
import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError

from elasticsearch_dsl.connections import connections
from elasticsearch_dsl import DocType, String, Date, Object

from scrapi import settings, registry
from scrapi.processing.base import BaseProcessor
from scrapi.base.transformer import JSONTransformer


logger = logging.getLogger(__name__)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('requests').setLevel(logging.WARN)
logging.getLogger('elasticsearch').setLevel(logging.FATAL)
logging.getLogger('elasticsearch.trace').setLevel(logging.FATAL)


def gen(name):
    klass = type(
        str(name),
        (NormalizedDocument, ),
        dict(Meta=type(
            'Meta',
            (),
            dict(index=settings.ELASTIC_INDEX, doc_type=name)
        ))
    )
    klass._doc_type.mapping.save(settings.ELASTIC_INDEX)
    return klass


def setup():
    try:
        es = Elasticsearch(settings.ELASTIC_URI, request_timeout=settings.ELASTIC_TIMEOUT)
        connections.add_connection('default', es)

        es.cluster.health(wait_for_status='yellow')
        es.indices.create(index=settings.ELASTIC_INDEX, body={}, ignore=400)
        es.indices.create(index='share_v1', ignore=400)

        model_registry = {
            name: gen(name) for name in registry.keys()
        }
        return es, model_registry
    except ConnectionError:  # pragma: no cover
        if 'elasticsearch' in settings.NORMALIZED_PROCESSING or 'elasticsearch' in settings.RAW_PROCESSING:
            raise
        logger.error('Could not connect to Elasticsearch, expect errors.')
        return None, {}


# This must remain until the next elasticsearch-dsl update
def update(self, **kwargs):
    for k, v in kwargs.items():
        self.__dict__['_d_'][k] = v
    self.save()

DocType.update = update


class ElasticsearchProcessor(BaseProcessor):
    NAME = 'elasticsearch'

    def process_normalized(self, raw_doc, normalized, index=settings.ELASTIC_INDEX):
        doc_type = model_registry.get(raw_doc['source'])
        if not doc_type:
            model_registry[raw_doc['source']] = gen(raw_doc['source'])
            doc_type = model_registry[raw_doc['source']]

        data = {
            key: value for key, value in normalized.attributes.items()
            if key in settings.FRONTEND_KEYS
        }

        doc = doc_type.get(id=raw_doc['docID'], ignore=404)
        if doc:
            if doc.providerUpdatedDateTime:
                data['providerUpdatedDateTime'] = doc.providerUpdatedDateTime
            doc.update(**data)
        else:
            doc = doc_type(**data)
            doc.meta.id = raw_doc['docID']
            doc.meta.type = raw_doc['source']
            doc.save()
        self.process_normalized_v1(raw_doc, normalized, data['providerUpdatedDateTime'])

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
        'tags': ('/tags', '/subjects', lambda x, y: (x + y) if (x and y) else (x or y or [])),
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


class NormalizedDocument(DocType):

    class Meta:
        index = settings.ELASTIC_INDEX

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

es, model_registry = setup()
