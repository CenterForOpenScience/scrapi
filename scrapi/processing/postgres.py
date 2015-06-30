from __future__ import absolute_import

import logging
from datetime import datetime as dt

from sqlalchemy import create_engine, ForeignKeyConstraint
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy import Column, Boolean, String, Integer, LargeBinary, DateTime, ForeignKey, Table

from sqlalchemy.dialects.postgresql import JSON, ARRAY

from scrapi import events
from scrapi.processing.base import BaseProcessor, HarvesterResponseModel

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

        raw = Raw(
            source=source,
            docID=docID,
            doc=raw_doc['doc'],
            filetype=raw_doc['filetype']
        )

        log = Log(**raw_doc['timestamps'])

        document.logs.append(log)
        document.raws.append(raw)

        session.add(document)
        session.add(raw)
        session.add(log)
        session.commit()

    @events.logged(events.PROCESSING, 'normalized.postgres')
    def process_normalized(self, raw_doc, normalized):
        source, docID = raw_doc['source'], raw_doc['docID']
        document = self._get_by_source_id(Document, source, docID) or Document(source=source, docID=docID)

        norm = Normalized()

        norm.source = source,
        norm.docID = docID,
        norm.title = normalized['title'],
        norm.providerUpdatedDateTime = normalized['providerUpdatedDateTime'],
        norm.uris = normalized['uris'],
        norm.description = normalized.get('description'),
        norm.tags = normalized.get('tags'),
        norm.subjects = normalized.get('subjects'),
        norm.languages = normalized.get('languages'),
        norm.freeToRead = normalized.get('freeToRead'),
        norm.licenses = normalized.get('licenses'),
        norm.publisher = normalized.get('publisher'),
        norm.sponsorships = normalized.get('sponsorships'),
        norm.version = normalized.get('version'),
        norm.otherProperties = normalized.get('otherProperties'),
        norm.shareProperties = normalized.get('shareProperties')

        log = Log(**raw_doc['timestamps'])

        document.normalized.append(norm)
        document.logs.append(log)

        # TODO: Too much duplication
        contributors = map(lambda c: Contributor(**c), normalized['contributors'])
        norm.contributors = contributors

        map(session.add, contributors)
        session.add(document)
        session.add(norm)
        session.add(log)
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


class Raw(Base):
    __tablename__ = 'raws'

    id = Column(Integer, primary_key=True)

    source = Column(String)
    docID = Column(String)

    doc = Column(LargeBinary)
    filetype = Column(String)

    __table_args__ = (
        ForeignKeyConstraint(
            [source, docID],
            [Document.source, Document.docID]
        ), {}
    )


contributor_association_table = Table(
    'contributor_association', Base.metadata,
    Column('normalized_id', Integer, ForeignKey('normalized.id'), primary_key=True),
    Column('contributor_id', Integer, ForeignKey('contributors.id'), primary_key=True),
)


class Normalized(Base):
    __tablename__ = 'normalized'

    id = Column(Integer, primary_key=True)

    source = Column(String)
    docID = Column(String)

    # This is the schema provided by SHARE
    contributors = relationship(
        'Contributor',
        secondary=contributor_association_table,
        backref='normalized'
    )

    title = Column(String)
    providerUpdatedDateTime = Column(DateTime)
    description = Column(String)
    uris = Column(JSON)
    tags = Column(ARRAY(String))
    subjects = Column(ARRAY(String))
    languages = Column(ARRAY(String))
    freeToRead = Column(JSON)
    licenses = Column(JSON)
    publisher = Column(JSON)
    sponsorships = Column(JSON)
    version = Column(JSON)
    otherProperties = Column(JSON)
    shareProperties = Column(JSON)

    __table_args__ = (
        ForeignKeyConstraint(
            [source, docID],
            [Document.source, Document.docID]
        ), {}
    )


class Contributor(Base):
    __tablename__ = 'contributors'

    # TODO: Need a sane primary key for contributors
    # This just creates a lot of duplication when reprocessing
    id = Column(Integer, primary_key=True)

    name = Column(String)
    givenName = Column(String)
    additionalName = Column(String)
    familyName = Column(String)

    email = Column(String)
    affiliation = Column(String)  # Or do we want an institutions table?

    sameAs = Column(ARRAY(String))


class Organization(Base):
    '''A model describing an organization
    '''  # TODO: This is not currently disambiguated in our schema
    __tablename__ = 'organizations'

    # TODO: Good identifier for an organization?
    # Name is much more likely to be unique
    id = Column(Integer, primary_key=True)

    name = Column(String)
    email = Column(String)
    sameAs = Column(ARRAY(String))


class Log(Base):
    __tablename__ = 'logs'

    id = Column(Integer, primary_key=True)

    document_source = Column(String)
    document_docID = Column(String)

    harvestStarted = Column(DateTime)
    harvestFinished = Column(DateTime)
    harvestTaskCreated = Column(DateTime)

    normalizeStarted = Column(DateTime)
    normalizeFinished = Column(DateTime)
    normalizeTaskCreated = Column(DateTime)

    __table_args__ = (
        ForeignKeyConstraint(
            [document_source, document_docID],
            [Document.source, Document.docID]
        ), {}
    )


class HarvesterResponse(HarvesterResponseModel, Base):
    """A parody of requests.response but stored in cassandra
    Should reflect all methods of a response object
    Contains an additional field time_made, self-explanitory
    """
    __tablename__ = 'responses'

    def __init__(self, *args, **kwargs):
        super(HarvesterResponse, self).__init__(*args, **kwargs)
        session.add(self)
        session.commit()

    method = Column(String, primary_key=True)
    url = Column(String, primary_key=True)

    # Raw request data
    ok = Column(Boolean)
    content = Column(LargeBinary)
    encoding = Column(String)
    headers_str = Column(String)
    status_code = Column(String)
    time_made = Column(DateTime, default=dt.now)

    @classmethod
    def get(self, method=None, url=None):
        try:
            ret = session.query(self).get((method, url))
        except Exception:
            ret = None
        if not ret:
            raise self.DoesNotExist
        return ret
