from __future__ import unicode_literals

from datetime import date, timedelta, datetime

from lxml import etree

from scrapi import requests
from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi.util import copy_to_unicode
from scrapi.base.schemas import DOESCHEMA


class DoepagesHarvester(XMLHarvester):
    short_name = 'doepages'
    long_name = 'Department of Energy Pages'
    url = 'http://www.osti.gov/pages/'

    schema = DOESCHEMA

    namespaces = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'dcq': 'http://purl.org/dc/terms/'
    }

    def harvest(self, start_date=None, end_date=None):

        start_date = datetime.strptime(start_date, '%Y-%m-%d').date().strftime('%m/%d/%Y') if start_date else date.today().strftime('%m/%d/%Y')
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date().strftime('%m/%d/%Y') if end_date else (date.today() - timedelta(1)).strftime('%m/%d/%Y')

        base_url = 'http://www.osti.gov/pages/pagesxml?nrows={0}&EntryDateFrom={1}&EntryDateTo={2}'
        url = base_url.format('1', start_date, end_date)
        initial_data = requests.get(url)
        record_encoding = initial_data.encoding
        initial_doc = etree.XML(initial_data.content)

        num_results = int(initial_doc.xpath('//records/@count', namespaces=self.namespaces)[0])

        url = base_url.format(num_results, start_date, end_date)

        data = requests.get(url)
        doc = etree.XML(data.content)

        records = doc.xpath('records/record')

        xml_list = []
        for record in records:
            doc_id = record.xpath('dc:ostiId/node()', namespaces=self.namespaces)[0]
            record = etree.tostring(record, encoding=record_encoding)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.short_name,
                'docID': copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list
