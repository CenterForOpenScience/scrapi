import json

from cqlengine import columns, Model, connection
from cqlengine.management import sync_table, create_keyspace

from scrapi.processing.base import BaseProcessor

connection.setup(['127.0.0.1'], 'cqlengine')
create_keyspace("cqlengine", replication_factor=1, strategy_class="SimpleStrategy")

class CassandraProcessor(BaseProcessor):
    NAME = 'cassandra'

    def __init__(self):
        sync_table(DocumentModel)

    def process_normalized(self, raw_doc, normalized):
        nm = DocumentModel.objects(docID=normalized.get("id")['serviceID'], source=normalized.get('source')).update(
            url=normalized.get('id')['url'],
            contributors=json.dumps(normalized.get('contributors')),
            id=normalized.get('id'),
            title=normalized.get('title'),
            tags=normalized.get('tags'),
            dateUpdated=normalized.get('dateUpdated'),
            properties=json.dumps(normalized.get('properties'))
        )

    def process_raw(self, raw_doc):
        rm = DocumentModel.create(**raw_doc.attributes)

class DocumentModel(Model):
    __table_name__ = 'documents'
    __keyspace__ = 'cqlengine'


    # Raw
    docID = columns.Text(primary_key=True)
    doc = columns.Bytes()
    source = columns.Text(primary_key=True, index=True, clustering_order="DESC")
    filetype = columns.Text()
    timestamps = columns.Map(columns.Text, columns.Text)

    # Normalized
    url = columns.Text()
    contributors = columns.Text() #TODO
    id = columns.Map(columns.Text, columns.Text)
    title = columns.Text()
    description = columns.Text()
    tags = columns.List(columns.Text())
    dateUpdated = columns.Text()
    properties = columns.Text() #TODO
