from __future__ import absolute_import

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import copy
import logging

import django

from api.webview.models import HarvesterResponse, Document

from scrapi import events
from scrapi.util import json_without_bytes
from scrapi.linter import RawDocument, NormalizedDocument
from scrapi.processing import DocumentTuple
from scrapi.processing.base import BaseProcessor, BaseHarvesterResponse, BaseDatabaseManager

django.setup()
logger = logging.getLogger(__name__)


class DatabaseManager(BaseDatabaseManager):
    '''All database management is performed by django'''

    def setup(self):
        return True

    def tear_down(self):
        pass

    def clear(self, force=False):
        pass

    def celery_setup(self, *args, **kwargs):
        pass


class PostgresProcessor(BaseProcessor):
    NAME = 'postgres'

    manager = DatabaseManager()

    def documents(self, *sources):
        sources = sources
        q = Document.objects.all()
        querysets = (q.filter(source=source) for source in sources) if sources else [q]
        for query in querysets:
            for doc in query:
                try:
                    raw = RawDocument(doc.raw, clean=False, validate=False)
                except AttributeError as e:
                    logger.info('{}  -- Malformed rawdoc in database, skipping'.format(e))
                    raw = None
                    continue
                normalized = NormalizedDocument(doc.normalized, validate=False, clean=False) if doc.normalized else None
                yield DocumentTuple(raw, normalized)

    def get(self, source, docID):
        try:
            document = Document.objects.get(source=source, docID=docID)
        except Document.DoesNotExist:
            return None
        raw = RawDocument(document.raw, clean=False, validate=False)
        normalized = NormalizedDocument(document.normalized, validate=False, clean=False) if document.normalized else None

        return DocumentTuple(raw, normalized)

    def delete(self, source, docID):
        doc = Document.objects.get(source=source, docID=docID)
        doc.delete()

    def create(self, attributes):
        attributes = json_without_bytes(attributes)
        Document.objects.create(
            source=attributes['source'],
            docID=attributes['docID'],
            providerUpdatedDateTime=None,
            raw=attributes,
            normalized=None
        ).save()

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
            key = kwargs['method'].lower() + kwargs['url'].lower()
            self.response = HarvesterResponse(key=key, *args, **kwargs)
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
        if isinstance(self.response.content, memoryview):
            return self.response.content.tobytes()
        if isinstance(self.response.content, bytes):
            return self.response.content
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
