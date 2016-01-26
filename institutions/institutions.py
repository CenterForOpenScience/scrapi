from elasticsearch import Elasticsearch
from elasticsearch_dsl import DocType, String, Boolean, Integer
from elasticsearch_dsl.connections import connections

from scrapi.settings import ELASTIC_URI, ELASTIC_INST_INDEX, ELASTIC_TIMEOUT

connections.create_connection(hosts=[ELASTIC_URI])


def setup():
    Institution.init()


def remove():
    es = Elasticsearch(ELASTIC_URI, request_timeout=ELASTIC_TIMEOUT)
    es.indices.delete(ELASTIC_INST_INDEX)


class Institution(DocType):
    name = String()
    established = String()
    location = {
        'street_address': String(),
        'city': String(),
        'state': String(),
        'country': String(),
        'ext_code': Integer()
    }
    web_url = String()
    id_ = String()
    public = Boolean()
    for_profit = Boolean()
    offers_degree = Boolean()
    other_names = String()

    def save(self, **kwargs):
        self.meta.id = self.id_
        return super(Institution, self).save(**kwargs)

    class Meta:
        index = ELASTIC_INST_INDEX
