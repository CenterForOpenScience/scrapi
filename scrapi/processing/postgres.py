from __future__ import absolute_import

import logging

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, String, DateTime

from sqlalchemy.dialects.postgresql import JSON, ARRAY, BYTEA

from scrapi import events
from scrapi.processing.base import BaseProcessor

logger = logging.getLogger(__name__)

engine = create_engine('postgresql://localhost/scrapi', echo=True)
session = sessionmaker(bind=engine)()
Base = declarative_base(bind=engine)


class PostgresProcessor(BaseProcessor):
    NAME = 'postgres'

    def __init__(self, *args, **kwargs):
        Base.metadata.create_all()
        session.commit()
        super(PostgresProcessor, self).__init__(*args, **kwargs)

    @events.logged(events.PROCESSING, 'raw.postgres')
    def process_raw(self, raw_doc):
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        session.add(document)
        session.commit()

    @events.logged(events.PROCESSING, 'normalized.postgres')
    def process_normalized(self, raw_doc, normalized):
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        document.title = normalized['title'],
        document.providerUpdatedDateTime = normalized['providerUpdatedDateTime'],
        document.uris = normalized['uris'],
        document.description = normalized.get('description'),
        document.tags = normalized.get('tags'),
        document.subjects = normalized.get('subjects'),
        document.otherProperties = normalized.get('otherProperties'),
        document.shareProperties = normalized.get('shareProperties')

        session.add(document)
        session.commit()

    def _get_by_source_id(self, model, source, docID):
        return session.query(model).filter_by(source=source, docID=docID).first()


class Document(Base):
    __tablename__ = 'documents'

    source = Column(String, primary_key=True)
    docID = Column(String, primary_key=True)

    raw = Column(BYTEA)
    normalized = Column(JSON)
    uris = Column(JSON)
    contributors = Column(ARRAY)
    description = Column(String)
    providerUpdatedDateTime = Column(DateTime)
    tags = Column(ARRAY)
    subjects = Column(ARRAY)

    otherProperties = Column(JSON)
    shareProperties = Column(JSON)
