from __future__ import absolute_import

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import copy
import logging

from api.webview.models import HarvesterResponse, Document

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
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        modified_doc = copy.deepcopy(raw_doc.attributes)
        if modified_doc.get('versions'):
            modified_doc['versions'] = list(map(str, modified_doc['versions']))

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


class HarvesterResponseModel(BaseHarvesterResponse):

    response = None

    def __init__(self, *args, **kwargs):
        if kwargs:
            self.response = HarvesterResponse(key=kwargs['method'].lower() + kwargs['url'].lower(), *args, **kwargs)
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
        return bool(self.response.ok)

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
        return int(self.response.status_code)

    @property
    def time_made(self):
        return str(self.response.time_made)

    def save(self, *args, **kwargs):
        self.response.save()
        return self

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self.response, k, v)
        return self.save()

    @classmethod
    def get(cls, url=None, method=None):
        key = method.lower() + url.lower()
        try:
            return cls(HarvesterResponse.objects.get(key=key))
        except HarvesterResponse.DoesNotExist:
            raise cls.DoesNotExist
