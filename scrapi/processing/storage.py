"""Adds a backend for storing documents as flat files.

To use add this to settings.local:

    NORMALIZED_PROCESSING = ['storage']
    RAW_PROCESSING = ['storage']
"""

import os
import copy
import json

from scrapi.processing.base import BaseProcessor


class StorageProcessor(BaseProcessor):
    NAME = 'storage'

    def process_raw(self, raw):
        filename = 'archive/{}/{}/raw.{}'.format(raw['source'], raw['docID'], raw['filetype'])
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        new_attrs = copy.deepcopy(raw.attributes)
        if new_attrs.get('versions'):

            if isinstance(new_attrs['versions'], list):
                new_attrs['versions'] = map(str, new_attrs['versions'])
            else:
                new_attrs['verisons'] = str(new_attrs['versions'])

        with open(filename, 'w') as f:
            f.write(json.dumps(new_attrs, indent=4))

    def process_normalized(self, raw, normalized):
        filename = 'archive/{}/{}/normalized.json'.format(raw['source'], raw['docID'], raw['filetype'])
        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

        with open(filename, 'w') as f:
            f.write(json.dumps(normalized.attributes, indent=4))
