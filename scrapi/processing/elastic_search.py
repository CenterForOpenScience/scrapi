import json
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

logger = logging.getLogger(__name__)


class ElasticsearchProcessor(BaseProcessor):
    NAME = 'elasticsearch'

    def process_normalized(self, raw_doc, normalized):
        data = {
            key: value for key, value in normalized.attributes.items()
            if key in settings.FRONTEND_KEYS
        }

        normalized['dateUpdated'] = self.version_dateUpdated(normalized)

        es.index(
            body=data,
            refresh=True,
            index='share',
            doc_type=normalized['source'],
            id=normalized['id']['serviceID'],
        )

    def version_dateUpdated(self, normalized):
        old_doc = es.get_source(
            index='share',
            doc_type=normalized['source'],
            id=normalized['id']['serviceID'],
            ignore=[404]
        )

        logger.info(json.dumps(old_doc, indent=4))

        return old_doc['dateUpdated'] if old_doc else normalized['dateUpdated']
