import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import NotFoundError

from scrapi import settings
from scrapi.processing.base import BaseProcessor

es = Elasticsearch(
    settings.ELASTIC_URI,
    request_timeout=settings.ELASTIC_TIMEOUT
)


logger = logging.getLogger(__name__)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('requests').setLevel(logging.WARN)
logging.getLogger('elasticsearch').setLevel(logging.WARN)
logging.getLogger('elasticsearch.trace').setLevel(logging.WARN)


es.cluster.health(wait_for_status='yellow')


class ElasticsearchProcessor(BaseProcessor):
    NAME = 'elasticsearch'

    def process_normalized(self, raw_doc, normalized):
        normalized['dateUpdated'] = self.version_dateUpdated(normalized)
        data = {
            key: value for key, value in normalized.attributes.items()
            if key in settings.FRONTEND_KEYS
        }

        es.index(
            body=data,
            refresh=True,
            index=settings.ELASTIC_INDEX,
            doc_type=normalized['source'],
            id=normalized['id']['serviceID'],
        )

    def version_dateUpdated(self, normalized):
        try:
            old_doc = es.get_source(
                index=settings.ELASTIC_INDEX,
                doc_type=normalized['source'],
                id=normalized['id']['serviceID'],
            )
        except NotFoundError:
            # Normally I don't like exception-driven logic,
            # but this was the best way to handle missing
            # types, indices and documents together
            date = normalized['dateUpdated']
        else:
            date = old_doc['dateUpdated']

        return date


def create_index():
    body = {
        "mappings": {
            harvester: {
                "properties": {
                    "id": {
                        "properties": {
                            "doi": {
                                "type": "multi_field",
                                "index": "not_analyzed",
                                "fields": {
                                    "analyzed": {
                                        "type": "string",
                                        "index": "analyzed"
                                    }
                                }
                            },
                            "url": {
                                "type": "multi_field",
                                "index": "not_analyzed",
                                "fields": {
                                    "analyzed": {
                                        "type": "string",
                                        "index": "analyzed"
                                    }
                                }
                            },
                            "serviceID": {
                                "type": "multi_field",
                                "index": "not_analyzed",
                                "fields": {
                                    "analyzed": {
                                        "type": "string",
                                        "index": "analyzed"
                                    }
                                }
                            }
                        }
                    }
                }
            } for harvester in settings.MANIFESTS.keys()
        }
    }
    es.indices.create(index=settings.ELASTIC_INDEX, body=body, ignore=400)

create_index()
