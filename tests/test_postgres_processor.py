import pytest

import os
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "api.api.settings")

from . import utils
from scrapi.linter.document import NormalizedDocument, RawDocument
from scrapi.processing.postgres import PostgresProcessor, Document

test_db = PostgresProcessor()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = RawDocument(utils.RAW_DOC)


@pytest.mark.postgres
def test_process_raw():
    test_db.process_raw(RAW)
    queryset = Document(docID='someID', source=RAW['source'])
    assert queryset.docID == RAW.attributes['docID']
