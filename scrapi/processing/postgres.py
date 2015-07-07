from __future__ import absolute_import

import logging
from datetime import datetime as dt

from sqlalchemy import create_engine, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import Column, Boolean, String, Integer, LargeBinary, DateTime, ForeignKey, Table

from sqlalchemy.dialects.postgresql import JSON, ARRAY

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

        session.add(document)
        session.commit()

    def _get_by_source_id(self, model, source, docID):
        return session.query(model).filter_by(source=source, docID=docID).first()


class Document(Base):
    __tablename__ = 'documents'

    source = Column(String, primary_key=True)
    docID = Column(String, primary_key=True)

    raws = relationship('Raw', backref='document')
    normalized = relationship('Normalized', backref='document')

    logs = relationship('Log', backref='document')
