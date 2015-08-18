import csv
from datetime import timedelta, date

from scrapi import settings
from scrapi import requests
from scrapi.linter import RawDocument
from scrapi.base import XMLHarvester
from scrapi.base.helpers import single_result


class DataFirstHarvester(XMLHarvester):

    short_name = 'datafirst'
    long_name = 'DataFirst, University of Cape Town'
    url = 'http://www.datafirst.uct.ac.za/'
    file_format = 'csv'

    BASE_URL = 'http://www.datafirst.uct.ac.za/dataportal/index.php/catalog/export/csv?ps=500&sort_by=proddate&sort_order=desc&view=s'
    BASE_DATA_URL = 'http://www.datafirst.uct.ac.za/dataportal/index.php/catalog/ddi/{}'

    schema = {
        'title': ('//titl/node().text', single_result),
        'description': (),
        'uris': {
            'canonicalUri': (),

        },
        'contributors': ()
    }

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()
        response = requests.get(self.BASE_URL)
        lines = response.text.strip().split('\n')
        reader = csv.reader(lines[1:])
        return [RawDocument({
            'doc': self.harvest_record(line[0]),
            'source': self.short_name,
            'docID': line[0],
            'filetype': self.file_format
        }) for line in [reader.next()]]

    def harvest_record(self, service_id):
        return requests.get(self.BASE_DATA_URL.format(service_id)).content
