import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from scrapi.linter.document import NormalizedDocument, RawDocument

from scrapi.processing.postgres import PostgresProcessor, Document

from . import utils

test_db = PostgresProcessor()

engine = create_engine('postgresql://localhost/test', echo=True)
session = sessionmaker(bind=engine)()

NORMALIZED = NormalizedDocument(utils.RECORD)
RAW = RawDocument(utils.RAW_DOC)


@pytest.mark.postgres
def test_process_raw():
    test_db.process_raw(RAW)
    queryset = Document(docID='someID', source=RAW['source'])
    assert queryset.docID == RAW.attributes['docID']
