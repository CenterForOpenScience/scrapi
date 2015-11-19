"""Adds a backend for storing documents as flat files.

To use add this to settings.local:

    NORMALIZED_PROCESSING = ['storage']
    RAW_PROCESSING = ['storage']
"""

import os
import json

from scrapi.util import json_without_bytes
from scrapi.processing.base import BaseProcessor


class StorageProcessor(BaseProcessor):
    NAME = 'storage'

    def process_raw(self, raw):
        self.write(raw['source'], raw['docID'], 'raw', raw.attributes)

    def process_normalized(self, raw, normalized):
        self.write(raw['source'], raw['docID'], 'normalized', normalized.attributes)

    def write(self, source, doc_id, filename, content):
        filepath = 'archive/{}/{}/{}.json'.format(source, doc_id, filename)

        if not os.path.exists(os.path.dirname(filepath)):
            os.makedirs(os.path.dirname(filepath))

        with open(filepath, 'w') as f:
            f.write(json.dumps(json_without_bytes(content), indent=4))

    def documents(self, *sources):
        raise NotImplementedError

    def get_versions(self, source, docID):
        raise NotImplementedError
