from __future__ import absolute_import

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import copy
import logging

from scrapi import events
from scrapi.processing.base import BaseProcessor

from api.webview.models import Document

logger = logging.getLogger(__name__)


class PostgresProcessor(BaseProcessor):
    NAME = 'postgres'

    @events.logged(events.PROCESSING, 'raw.postgres')
    def process_raw(self, raw_doc):
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        modified_doc = copy.deepcopy(raw_doc.attributes)
        if modified_doc.get('versions'):
            modified_doc['versions'] = map(str, modified_doc['versions'])

        document.raw = modified_doc

        document.save()

    @events.logged(events.PROCESSING, 'normalized.postgres')
    def process_normalized(self, raw_doc, normalized):
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        document.normalized = normalized.attributes
        document.providerUpdatedDateTime = normalized['providerUpdatedDateTime']

        document.save()

    def _get_by_source_id(self, model, source, docID):
        try:
            return Document.objects.filter(source=source, docID=docID)[0]
        except IndexError:
            return None
