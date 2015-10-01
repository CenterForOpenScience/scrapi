from __future__ import absolute_import

import six
import json
import logging
from uuid import uuid4
from datetime import datetime

from dateutil.parser import parse

from cassandra.cqlengine import connection
from cassandra.cqlengine import management
from cassandra.cluster import NoHostAvailable
from cassandra.cqlengine import columns, models

from scrapi import events
from scrapi import settings
from scrapi.util import try_n_times
from scrapi.util import copy_to_unicode
from scrapi.linter import RawDocument, NormalizedDocument
from scrapi.processing import DocumentTuple
from scrapi.processing.base import BaseHarvesterResponse, BaseProcessor, BaseDatabaseManager


logger = logging.getLogger(__name__)
logging.getLogger('cqlengine.cql').setLevel(logging.WARN)


class DatabaseManager(BaseDatabaseManager):
    _models = set()

    def __init__(self, uri=None, keyspace=None):
        self._setup = False

        self.uri = uri or settings.CASSANDRA_URI
        self.keyspace = keyspace or settings.CASSANDRA_KEYSPACE
        self._models = set(map(self.register_model, self._models))

    def setup(self, force=False, sync=True):
        if self._setup and not force:
            return True

        try:
            connection.setup(self.uri, self.keyspace)
            if sync:
                management.create_keyspace(self.keyspace, replication_factor=1, strategy_class='SimpleStrategy')
                for model in self._models:
                    model.__keyspace__ = self.keyspace
                    management.sync_table(model)
        except NoHostAvailable:
            logger.error('Could not connect to Cassandra, expect errors.')
            return False

        # Note: return values are for test skipping
        self._setup = True
        return True

    def tear_down(self):
        if not self._setup:
            logger.warning('Attempting to tear down a database that was never setup')

        if connection.cluster is not None:
            connection.cluster.shutdown()
        if connection.session is not None:
            connection.session.shutdown()

        self._setup = False

    def clear(self, force=False):
        assert force, 'clear_keyspace must be called with force'
        assert self.keyspace != settings.CASSANDRA_KEYSPACE, 'Cannot erase the keyspace in settings'

        management.delete_keyspace(self.keyspace)
        self.tear_down()
        return self.setup()

    def register_model(self, model):
        self._models.add(model)
        model.__keyspace__ = self.keyspace
        if self._setup:
            management.sync_table(model)
        return model

    def celery_setup(self, *args, **kwargs):
        self.tear_down()
        self.setup()

    @classmethod
    def registered_model(cls, model):
        cls._models.add(model)
        return model


class CassandraProcessor(BaseProcessor):
    '''
    Cassandra processor for scrapi. Handles versioning and storing documents in Cassandra
    '''
    NAME = 'cassandra'
    _manager = None

    @property
    def manager(self):
        self._manager = self._manager or DatabaseManager()
        return self._manager

    @property
    def HarvesterResponseModel(self):
        return HarvesterResponse

    @events.logged(events.PROCESSING, 'normalized.cassandra')
    def process_normalized(self, raw_doc, normalized):
        self.send_to_database(
            source=copy_to_unicode(raw_doc['source']),
            docID=copy_to_unicode(raw_doc['docID']),
            contributors=copy_to_unicode(json.dumps(normalized['contributors'])),
            description=copy_to_unicode(normalized.get('description')),
            uris=copy_to_unicode(json.dumps(normalized['uris'])),
            providerUpdatedDateTime=parse(normalized['providerUpdatedDateTime']).replace(tzinfo=None),
            freeToRead=copy_to_unicode(json.dumps(normalized.get('freeToRead', {}))),
            languages=normalized.get('language'),
            licenses=copy_to_unicode(json.dumps(normalized.get('licenseRef', []))),
            publisher=copy_to_unicode(json.dumps(normalized.get('publisher', {}))),
            sponsorships=copy_to_unicode(json.dumps(normalized.get('sponsorship', []))),
            title=copy_to_unicode(normalized['title']),
            version=copy_to_unicode(json.dumps(normalized.get('version'), {})),
            otherProperties=copy_to_unicode(json.dumps(normalized.get('otherProperties', {}))),
            shareProperties=copy_to_unicode(json.dumps(normalized['shareProperties']))
        ).save()

    @events.logged(events.PROCESSING, 'raw.cassandra')
    def process_raw(self, raw_doc):
        self.send_to_database(
            source=raw_doc['source'],
            docID=raw_doc['docID'],
            filetype=raw_doc['filetype'],
            doc=six.text_type(raw_doc['doc']).encode('utf-8'),
            timestamps=raw_doc.get('timestamps', {})
        ).save()

    def send_to_database(self, docID, source, **kwargs):
        documents = DocumentModel.objects(docID=docID, source=source)
        if documents:
            document = documents[0]
            if self.different(dict(document), dict(docID=docID, source=source, **kwargs)):
                # Create new version, get UUID of new version, update
                versions = document.versions + kwargs.pop('versions', [])
                version = VersionModel(key=uuid4(), **dict(document))
                version.save()
                versions.append(version.key)
                return document.update(versions=versions, **kwargs)
            else:
                raise events.Skip("No changes detected for document with ID {0} and source {1}.".format(docID, source))
        else:
            # create document
            return DocumentModel.create(docID=docID, source=source, **kwargs)

    def different(self, old, new):
        try:
            return not all([new[key] == old[key] or (not new[key] and not old[key]) for key in new.keys() if key != 'timestamps'])
        except Exception:
            return True  # If the document fails to load/compare for some reason, accept a new version

    def documents(self, *sources):
        sources = sources
        q = DocumentModel.objects.timeout(500).allow_filtering().all().limit(1000)
        querysets = (q.filter(source=source) for source in sources) if sources else [q]
        for query in querysets:
            page = try_n_times(5, list, query)
            while len(page) > 0:
                for doc in page:
                    yield DocumentTuple(self.to_raw(doc), self.to_normalized(doc))
                page = try_n_times(5, self.next_page, query, page)

    def next_page(self, query, page):
        return list(query.filter(docID__gt=page[-1].docID))

    def to_raw(self, doc):
        return RawDocument({
            'doc': doc.doc,
            'docID': doc.docID,
            'source': doc.source,
            'filetype': doc.filetype,
            'timestamps': doc.timestamps,
            'versions': doc.versions
        }, validate=False, clean=False)

    def to_normalized(self, doc):
        # make the new dict actually contain real items
        normed = {}
        do_not_include = ['docID', 'doc', 'filetype', 'timestamps', 'source']
        for key, value in dict(doc).items():
            if value and key not in do_not_include:
                try:
                    normed[key] = json.loads(value)
                except (ValueError, TypeError):
                    normed[key] = value

        if normed.get('versions'):
            normed['versions'] = list(map(str, normed['versions']))

        # No datetime means the document wasn't normalized (probably wasn't on the approved list)
        # TODO - fix odd circular import that makes us import this here
        from scrapi.base.helpers import datetime_formatter
        if normed.get('providerUpdatedDateTime'):
            normed['providerUpdatedDateTime'] = datetime_formatter(normed['providerUpdatedDateTime'].isoformat())
        else:
            return None

        return NormalizedDocument(normed, validate=False, clean=False)

    def get(self, source, docID):

        documents = DocumentModel.objects(source=source, docID=docID)

        try:
            doc = documents[0]
        except IndexError:
            return None
        raw = self.to_raw(doc)
        normalized = self.to_normalized(doc)

        return DocumentTuple(raw, normalized)

    def delete(self, source, docID):
        document = DocumentModel.objects(source=source, docID=docID)
        document.timeout(5).delete()

    def create(self, attributes):
        DocumentModel.create(**attributes).save()


@DatabaseManager.registered_model
class DocumentModel(models.Model):
    '''
    Defines the schema for a metadata document in cassandra

    The schema contains denormalized raw document, denormalized
    normalized (so sorry for the terminology clash) document, and
    a list of version IDs that refer to previous versions of this
    metadata.
    '''
    __table_name__ = 'documents_source_partitioned'

    # Raw
    source = columns.Text(primary_key=True, partition_key=True)
    docID = columns.Text(primary_key=True, index=True, clustering_order='ASC')

    doc = columns.Bytes()
    filetype = columns.Text()
    timestamps = columns.Map(columns.Text, columns.Text)

    # Normalized
    uris = columns.Text()
    title = columns.Text()
    contributors = columns.Text()  # TODO
    providerUpdatedDateTime = columns.DateTime()

    description = columns.Text()
    freeToRead = columns.Text()  # TODO
    languages = columns.List(columns.Text())
    licenses = columns.Text()  # TODO
    publisher = columns.Text()  # TODO
    subjects = columns.List(columns.Text())
    tags = columns.List(columns.Text())
    sponsorships = columns.Text()  # TODO
    version = columns.Text()  # TODO
    otherProperties = columns.Text()  # TODO
    shareProperties = columns.Text()  # TODO

    # Additional metadata
    versions = columns.List(columns.UUID)


@DatabaseManager.registered_model
class DocumentModelOld(models.Model):
    '''
    Defines the schema for a metadata document in cassandra

    The schema contains denormalized raw document, denormalized
    normalized (so sorry for the terminology clash) document, and
    a list of version IDs that refer to previous versions of this
    metadata.
    '''
    __table_name__ = 'documents'

    # Raw
    docID = columns.Text(primary_key=True)
    source = columns.Text(primary_key=True, clustering_order="DESC")

    doc = columns.Bytes()
    filetype = columns.Text()
    timestamps = columns.Map(columns.Text, columns.Text)

    # Normalized
    uris = columns.Text()
    title = columns.Text()
    contributors = columns.Text()  # TODO
    providerUpdatedDateTime = columns.DateTime()

    description = columns.Text()
    freeToRead = columns.Text()  # TODO
    languages = columns.List(columns.Text())
    licenses = columns.Text()  # TODO
    publisher = columns.Text()  # TODO
    subjects = columns.List(columns.Text())
    tags = columns.List(columns.Text())
    sponsorships = columns.Text()  # TODO
    version = columns.Text()  # TODO
    otherProperties = columns.Text()  # TODO
    shareProperties = columns.Text()  # TODO

    # Additional metadata
    versions = columns.List(columns.UUID)


@DatabaseManager.registered_model
class VersionModel(models.Model):
    '''
    Defines the schema for a version of a metadata document in Cassandra

    See the DocumentModel class. This schema is very similar, except it is
    keyed on a UUID that is generated by us, rather than it's own metadata
    '''

    __table_name__ = 'versions'

    key = columns.UUID(primary_key=True, required=True)

    # Raw
    doc = columns.Bytes()
    docID = columns.Text()
    filetype = columns.Text()
    source = columns.Text()
    timestamps = columns.Map(columns.Text, columns.Text)

    # Normalized
    uris = columns.Text()
    title = columns.Text()
    contributors = columns.Text()  # TODO
    providerUpdatedDateTime = columns.DateTime()

    description = columns.Text()
    freeToRead = columns.Text()  # TODO
    languages = columns.List(columns.Text())
    licenses = columns.Text()  # TODO
    publisher = columns.Text()  # TODO
    subjects = columns.List(columns.Text())
    tags = columns.List(columns.Text())
    sponsorships = columns.Text()  # TODO
    version = columns.Text()  # TODO
    otherProperties = columns.Text()  # TODO
    shareProperties = columns.Text()  # TODO

    # Additional metadata
    versions = columns.List(columns.UUID)


@DatabaseManager.registered_model
class HarvesterResponse(models.Model, BaseHarvesterResponse):
    __table_name__ = 'responses'

    method = columns.Text(primary_key=True)
    url = columns.Text(primary_key=True, required=True)

    # Raw request data
    ok = columns.Boolean()
    content = columns.Bytes()
    encoding = columns.Text()
    headers_str = columns.Text()
    status_code = columns.Integer()
    time_made = columns.DateTime(default=datetime.now)
