from __future__ import absolute_import

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "webview.settings")

import django
import logging
from api.webview.models import Document

from scrapi import events
from scrapi.processing.base import BaseProcessor

django.setup()

logger = logging.getLogger(__name__)


class PostgresProcessor(BaseProcessor):
    NAME = 'postgres'

    @events.logged(events.PROCESSING, 'raw.postgres')
    def process_raw(self, raw_doc):
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        document.raw = raw_doc.attributes

        document.save()

    @events.logged(events.PROCESSING, 'normalized.postgres')
    def process_normalized(self, raw_doc, normalized):
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        document.normalized = normalized.attributes
        document.providerUpdatedDateTime = normalized['providerUpdatedDateTime']

        document.save()

    def _get_by_source_id(self, model, source, docID):
        return Document.objects.filter(source=source, docID=docID)
