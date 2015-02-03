from cqlengine import columns, Model, connection
from cqlengine.management import sync_table, create_keyspace

from scrapi.processing.base import BaseProcessor

connection.setup(['127.0.0.1'], 'cqlengine')

class CassandraProcessor(BaseProcessor):
    NAME = 'cassandra'

    def __init__(self):
        create_keyspace("cqlengine", replication_factor=1, strategy_class="SimpleStrategy")
        sync_table(RawModel)


    def process_normalized(self, raw_doc, normalized):
        raise NotImplementedError

    def process_raw(self, raw_doc):
        rm = RawModel.create(**raw_doc.attributes)


class RawModel(Model):
    __table_name__ = 'raw_doc'
    __keyspace__ = 'cqlengine'
    docID = columns.Text(required=True, primary_key=True)
    doc = columns.Bytes(required=True)
    source = columns.Text(required=True)
    filetype = columns.Text(required=True)
    timestamps = columns.Map(columns.Text, columns.Text)
