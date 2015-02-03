import json

from cqlengine import columns, Model, connection
from cqlengine.management import sync_table, create_keyspace

from scrapi.processing.base import BaseProcessor

connection.setup(['127.0.0.1'], 'cqlengine')
create_keyspace("cqlengine", replication_factor=1, strategy_class="SimpleStrategy")

class CassandraProcessor(BaseProcessor):
    NAME = 'cassandra'

    def __init__(self):
        sync_table(RawModel)
        sync_table(NormalizedModel)

    def process_normalized(self, raw_doc, normalized):
        nm = NormalizedModel.create(
            url=normalized.get('id')['url'],
            contributors=json.dumps(normalized.get('contributors')),
            id=normalized.get('id'),
            title=normalized.get('title'),
            source=normalized.get('source'),
            tags=normalized.get('tags'),
            dateUpdated=normalized.get('dateUpdated'),
            properties=json.dumps(normalized.get('properties'))
        )

    def process_raw(self, raw_doc):
        rm = RawModel.create(**raw_doc.attributes)


class RawModel(Model):
    __table_name__ = 'raw'
    __keyspace__ = 'cqlengine'

    docID = columns.Text(primary_key=True)
    doc = columns.Bytes()
    source = columns.Text(index=True)
    filetype = columns.Text()
    timestamps = columns.Map(columns.Text, columns.Text)


class NormalizedModel(Model):
    __table_name__ = 'normalized'
    __keyspace__ = 'cqlengine'

    url = columns.Text(primary_key=True)

    contributors = columns.Text()
    id = columns.Map(columns.Text, columns.Text)
    title = columns.Text(index=True)
    source = columns.Text(index=True)
    description = columns.Text()
    tags = columns.List(columns.Text())
    dateUpdated = columns.Text()
    properties=columns.Text()
