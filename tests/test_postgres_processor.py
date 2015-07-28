# import pytest

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import django
from django.test import TestCase
from scrapi.processing.postgres import PostgresProcessor, Document

from . import utils
from scrapi.linter.document import RawDocument, NormalizedDocument

django.setup()

test_db = PostgresProcessor()

RAW = RawDocument(utils.POSTGRES_RAW_DOC)
NORMALIZED = NormalizedDocument(utils.RECORD)


class DocumentTestCase(TestCase):

    def test_raw_processing(self):
        test_db.process_raw(RAW)
        queryset = Document(docID='someID', source=RAW['source'])
        assert queryset.docID == RAW.attributes['docID']

    def test_normalized_processing(self):
        test_db.process_normalized(RAW, NORMALIZED)
        queryset = Document(docID=RAW['docID'], source=RAW['source'])
        assert(queryset.source == NORMALIZED['shareProperties']['source'])
