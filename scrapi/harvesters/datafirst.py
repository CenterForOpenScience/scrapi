import csv
from datetime import timedelta, date

from lxml import etree

from scrapi import settings
from scrapi import requests
from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument, NormalizedDocument
from scrapi.base.helpers import (
    compose,
    CONSTANT,
    single_result,
    date_formatter,
    default_name_parser,
)

single_string = compose(lambda x: x.title().strip(), single_result)


def parse_contributor(author):
    orgs = {'statistics', 'south', 'africa', 'research', 'organization'}
    parts = set(author.lower().split(' '))
    if parts.intersection(orgs):
        return {'name': author}
    return default_name_parser([author])


class DataFirstHarvester(XMLHarvester):

    short_name = 'datafirst'
    long_name = 'DataFirst, University of Cape Town'
    url = 'http://www.datafirst.uct.ac.za/'
    file_format = 'xml'

    namespaces = {'icpsr': 'http://www.icpsr.umich.edu/DDI'}

    BASE_URL = 'http://www.datafirst.uct.ac.za/dataportal/index.php/catalog/export/csv?ps=500&sort_by=proddate&sort_order=desc&view=s'
    BASE_DATA_URL = 'http://www.datafirst.uct.ac.za/dataportal/index.php/catalog/ddi/{}'
    BASE_PROJECT_URL = 'http://www.datafirst.uct.ac.za/dataportal/index.php/catalog/{}'

    schema = {
        'title': ('//icpsr:titl/node()', single_string),
        'contributors': ('//icpsr:AuthEnty/node()', lambda x: map(parse_contributor, x.strip())),
        'uris': CONSTANT({}),
        'providerUpdatedDateTime': ('//icpsr:prodDate/node()', compose(date_formatter, single_result)),
        'description': ('//icpsr:abstract/node()', single_string),
    }

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()
        response = requests.get(self.BASE_URL)
        lines = response.text.strip().split('\n')
        reader = csv.reader(map(lambda x: x.encode('utf-8'), lines[1:]))
        return [RawDocument({
            'doc': self.harvest_record(line[0]),
            'source': self.short_name,
            'docID': line[0],
            'filetype': self.file_format
        }) for line in reader]

    def harvest_record(self, service_id):
        return etree.tostring(etree.XML(requests.get(self.BASE_DATA_URL.format(service_id)).content))

    def normalize(self, raw_doc):
        transformed = self.transform(etree.XML(raw_doc['doc']), fail=settings.RAISE_IN_TRANSFORMER)
        transformed['shareProperties'] = {
            'source': self.short_name,
            'docID': raw_doc['docID']
        }
        transformed['uris']['canonicalUri'] = self.BASE_PROJECT_URL.format(raw_doc['docID'])
        transformed['uris']['descriptorUris'] = [self.BASE_PROJECT_URL.format(raw_doc['docID'])]

        return NormalizedDocument(transformed, clean=True)
