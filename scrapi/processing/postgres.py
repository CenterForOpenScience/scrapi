from __future__ import absolute_import

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import copy
import logging

from api.webview.models import HarvesterResponse

from scrapi import events
from scrapi.processing.base import BaseProcessor, BaseHarvesterResponse


logger = logging.getLogger(__name__)


class PostgresProcessor(BaseProcessor):
    NAME = 'postgres'

    @property
    def HarvesterResponseModel(self):
        return HarvesterResponseModel

    @events.logged(events.PROCESSING, 'raw.postgres')
    def process_raw(self, raw_doc):
        from api.webview.models import Document

        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        modified_doc = copy.deepcopy(raw_doc.attributes)
        if modified_doc.get('versions'):
            modified_doc['versions'] = map(str, modified_doc['versions'])

        document.raw = modified_doc

        document.save()

    @events.logged(events.PROCESSING, 'normalized.postgres')
    def process_normalized(self, raw_doc, normalized):
        from api.webview.models import Document

        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        document.normalized = normalized.attributes
        document.providerUpdatedDateTime = normalized['providerUpdatedDateTime']

        document.save()

    def _get_by_source_id(self, model, source, docID):
        from api.webview.models import Document

        try:
            return Document.objects.filter(source=source, docID=docID)[0]
        except IndexError:
            return None


class HarvesterResponseModel(BaseHarvesterResponse):

    response = None

    def __init__(self, *args, **kwargs):
        if kwargs:
            self.response = HarvesterResponse(*args, **kwargs)
        else:
            self.response = args[0]

    @property
    def method(self):
        return str(self.response.method)

    @property
    def url(self):
        return str(self.response.url)

    @property
    def ok(self):
        return str(self.response.ok)

    @property
    def content(self):
        return str(self.response.content)

    @property
    def encoding(self):
        return str(self.response.encoding)

    @property
    def headers_str(self):
        return str(self.response.headers_str)

    @property
    def status_code(self):
        return str(self.response.status_code)

    @property
    def time_made(self):
        return str(self.response.time_made)

    def save(self, *args, **kwargs):
        self.response.save()
        return self

    def update(self, **kwargs):
        return self.response.update(**kwargs)

    @classmethod
    def get(cls, url=None, method=None):
        return cls(HarvesterResponse.objects.get(url=url, method=method))
