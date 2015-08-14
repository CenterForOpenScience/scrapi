from __future__ import absolute_import

import json
import logging
from uuid import uuid4

from dateutil.parser import parse

from cassandra.cqlengine import columns, models

from scrapi import events
from scrapi import database  # noqa
from scrapi.util import copy_to_unicode
from scrapi.processing.base import BaseProcessor


logger = logging.getLogger(__name__)
logging.getLogger('cqlengine.cql').setLevel(logging.WARN)


class CassandraProcessor(BaseProcessor):
    '''
    Cassandra processor for scrapi. Handles versioning and storing documents in Cassandra
    '''
    NAME = 'cassandra'

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
            doc=raw_doc['doc'].encode('utf-8'),
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
                raise events.Skip("No changees detected for document with ID {0} and source {1}.".format(docID, source))
        else:
            # create document
            return DocumentModel.create(docID=docID, source=source, **kwargs)

    def different(self, old, new):
        try:
            return not all([new[key] == old[key] or (not new[key] and not old[key]) for key in new.keys() if key != 'timestamps'])
        except Exception:
            return True  # If the document fails to load/compare for some reason, accept a new version


@database.register_model
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


@database.register_model
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


@database.register_model
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
