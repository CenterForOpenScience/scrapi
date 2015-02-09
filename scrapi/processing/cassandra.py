import json
from uuid import uuid4

from cqlengine import columns, Model, connection
from cqlengine.management import sync_table, create_keyspace

from scrapi.processing.base import BaseProcessor
from scrapi.settings import CASSANDRA_URI, CASSANDRA_KEYSPACE

connection.setup(CASSANDRA_URI, CASSANDRA_KEYSPACE)
create_keyspace(CASSANDRA_KEYSPACE, replication_factor=1, strategy_class='SimpleStrategy')



class CassandraProcessor(BaseProcessor):
    NAME = 'cassandra'

    def __init__(self):
        sync_table(DocumentModel)
        sync_table(VersionModel)

    def process_normalized(self, raw_doc, normalized):
        self.send_to_database(
            docID=normalized.get("id")['serviceID'],
            source=normalized.get('source'),
            url=normalized.get('id')['url'],
            contributors=json.dumps(normalized.get('contributors')),
            id=normalized.get('id'),
            title=normalized.get('title'),
            tags=normalized.get('tags'),
            dateUpdated=normalized.get('dateUpdated'),
            properties=json.dumps(normalized.get('properties'))
        ).save()

    def process_raw(self, raw_doc):
        self.send_to_database(**raw_doc.attributes).save()

    def send_to_database(self, docID, source, **kwargs):
        documents = DocumentModel.objects(docID=docID, source=source)
        if documents:
            document = documents[0]
            # Create new version, get UUID of new version, update
            versions = document.versions
            if document.url:
                version = VersionModel(key=uuid4(), **dict(document))
                version.save()
                versions.append(version.key)
            return document.update(versions=versions, **kwargs)
        else:
            # create document
            return DocumentModel.create(docID=docID, source=source, **kwargs)

class DocumentModel(Model):
    __table_name__ = 'documents'
    __keyspace__ = CASSANDRA_KEYSPACE

    # Raw
    docID = columns.Text(primary_key=True)
    doc = columns.Bytes()
    source = columns.Text(primary_key=True, index=True, clustering_order="DESC")
    filetype = columns.Text()
    timestamps = columns.Map(columns.Text, columns.Text)

    # Normalized
    url = columns.Text()
    contributors = columns.Text()  # TODO
    id = columns.Map(columns.Text, columns.Text)
    title = columns.Text()
    description = columns.Text()
    tags = columns.List(columns.Text())
    dateUpdated = columns.Text()
    properties = columns.Text()  # TODO

    # Additional metadata
    versions = columns.List(columns.UUID)


class VersionModel(Model):
    __table_name__ = 'versions'
    __keyspace__ = CASSANDRA_KEYSPACE

    key = columns.UUID(primary_key=True, required=True)

    # Raw
    docID = columns.Text()
    doc = columns.Bytes()
    source = columns.Text(index=True)
    filetype = columns.Text()
    timestamps = columns.Map(columns.Text, columns.Text)

    # Normalized
    url = columns.Text()
    contributors = columns.Text()  # TODO: When supported, this should be a user-defined type
    id = columns.Map(columns.Text, columns.Text)
    title = columns.Text()
    description = columns.Text()
    tags = columns.List(columns.Text())
    dateUpdated = columns.Text()
    properties = columns.Text()  # TODO

    # Additional metadata
    versions = columns.List(columns.UUID)
