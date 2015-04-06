from __future__ import absolute_import
import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch.exceptions import ConnectionError

from scrapi import settings
from scrapi import registry
from scrapi.processing.base import BaseProcessor


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
    # es.cluster.health(wait_for_status='yellow')
    es.indices.create(index=settings.ELASTIC_INDEX, body={}, ignore=400)
except ConnectionError:  # pragma: no cover
    logger.error('Could not connect to Elasticsearch, expect errors.')
    if 'elasticsearch' in settings.NORMALIZED_PROCESSING or 'elasticsearch' in settings.RAW_PROCESSING:
        raise


class ElasticsearchProcessor(BaseProcessor):
    NAME = 'elasticsearch'

    def process_normalized(self, raw_doc, normalized):
        normalized['releaseDate'] = self.version(raw_doc, normalized)
        data = {
            key: value for key, value in normalized.attributes.items()
            if key in settings.FRONTEND_KEYS
        }

        es.index(
            body=data,
            refresh=True,
            index=settings.ELASTIC_INDEX,
            doc_type=raw_doc['source'],
            id=raw_doc['docID'],
        )

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
            date = normalized['releaseDate']
        else:
            date = old_doc.get('releaseDate') or normalized['releaseDate']

        return date
