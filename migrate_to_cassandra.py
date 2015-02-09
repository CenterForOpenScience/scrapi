import logging

from dateutil import parser

from base64 import b64decode

from scrapi import settings
from scrapi.linter import RawDocument
from scrapi.tasks import normalize
from scrapi.util.storage import store
from scrapi.processing.cassandra import CassandraProcessor

logger = logging.getLogger(__name__)

cass = CassandraProcessor()

exceptions = []
for consumer_name in settings.MANIFESTS.keys():
    consumer = settings.MANIFESTS[consumer_name]
    for raw_path in store.iter_raws(consumer_name, include_normalized=True):
        try:
            date = parser.parse(raw_path.split('/')[-2])

            timestamp = date.isoformat()

            raw_file = store.get_as_string(raw_path)

            raw_doc = RawDocument({
                'doc': raw_file,
                'timestamps': {
                    'consumeFinished': timestamp
                },
                'docID': b64decode(raw_path.split('/')[-3]).decode('utf-8'),
                'source': consumer_name,
                'filetype': consumer['fileFormat'],
            })

            cass.process_raw(raw_doc)

            normalized = normalize(raw_doc, consumer_name)

            cass.process_normalized(raw_doc, normalized)
        except Exception as e:
            logger.exception(e)
            exceptions.append(e)

for exception in exceptions:
    logger.exception(exception)
