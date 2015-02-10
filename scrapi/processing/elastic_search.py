import logging

from elasticsearch import Elasticsearch

from scrapi import settings
from scrapi.processing.base import BaseProcessor

es = Elasticsearch(
    settings.ELASTIC_URI,
    request_timeout=settings.ELASTIC_TIMEOUT
)

logging.getLogger('elasticsearch').setLevel(logging.WARN)
logging.getLogger('elasticsearch.trace').setLevel(logging.WARN)
logging.getLogger('urllib3').setLevel(logging.WARN)
logging.getLogger('requests').setLevel(logging.WARN)
es.cluster.health(wait_for_status='yellow')


class ElasticsearchProcessor(BaseProcessor):
    NAME = 'elasticsearch'

    def process_raw(*args, **kwargs):
        pass

    def process_normalized(self, raw_doc, normalized):
        data = {
            key: value for key, value in normalized.attributes.items()
            if key in settings.FRONTEND_KEYS
        }
        es.index(
            body=data,
            refresh=True,
            index='share',
            doc_type=normalized['source'],
            id=normalized['id']['serviceID'],
        )
