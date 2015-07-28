import pytest

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

import django
from django.test import TestCase
from scrapi.processing.postgres import PostgresProcessor, Document

from . import utils
from scrapi.linter.document import RawDocument

django.setup()

test_db = PostgresProcessor()

# NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = RawDocument(utils.POSTGRES_RAW_DOC)


class DocumentTestCase(TestCase):

    @pytest.mark.django_db
    def test_raw_processing(self):
        test_db.process_raw(RAW)
        queryset = Document(docID='someID', source=RAW['source'])
        assert queryset.docID == RAW.attributes['docID']
