from __future__ import unicode_literals

from datetime import date, timedelta

from lxml import etree

from scrapi import requests
from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
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

    def harvest(self, days_back=1):
        today = date.today()
        start_date = today - timedelta(days_back)
        base_url = 'http://www.osti.gov/pages/pagesxml?nrows={0}&EntryDateFrom={1}&EntryDateTo={2}'
        url = base_url.format('1', start_date.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y'))
        initial_data = requests.get(url)
        record_encoding = initial_data.encoding
        initial_doc = etree.XML(initial_data.content)

        num_results = int(initial_doc.xpath('//records/@count', namespaces=self.namespaces)[0])

        url = base_url.format(num_results, start_date.strftime('%m/%d/%Y'), today.strftime('%m/%d/%Y'))
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
                'docID': self.copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list

    def copy_to_unicode(self, element, encoding='UTF-8'):
        element = ''.join(element)
        if isinstance(element, unicode):
            return element
        else:
            return unicode(element, encoding=encoding)
