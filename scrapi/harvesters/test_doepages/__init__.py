from __future__ import unicode_literals

from datetime import date, timedelta

import requests
from lxml import etree

from scrapi.linter import RawDocument
from scrapi.base import TransformerHarvester
from scrapi.base.transformer import XML_to_JSON


class DoepagesHarvester(TransformerHarvester):

    NAME = 'test_doepages'
    NAMESPACES = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'dcq': 'http://purl.org/dc/terms/'
    }

    def harvest(self, days_back=1):
        start_date = date.today() - timedelta(days_back)
        base_url = 'http://www.osti.gov/pages/pagesxml?nrows={0}&EntryDateFrom={1}'
        url = base_url.format('1', start_date.strftime('%m/%d/%Y'))
        initial_data = requests.get(url)
        print(initial_data.url)
        record_encoding = initial_data.encoding
        try:
            initial_doc = etree.XML(initial_data.content)
        except etree.XMLSyntaxError as e:
            print("error in namespaces: {}".format(e))
            return []

        num_results = int(initial_doc.xpath('//records/@count', namespaces=self.NAMESPACES)[0])

        url = base_url.format(num_results, start_date.strftime('%m/%d/%Y'))
        data = requests.get(url)
        doc = etree.XML(data.content)

        records = doc.xpath('records/record')

        xml_list = []
        for record in records:
            doc_id = record.xpath('dc:ostiId/node()', namespaces=self.NAMESPACES)[0]
            record = etree.tostring(record, encoding=record_encoding)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.NAME,
                'docID': self.copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list

    def copy_to_unicode(self, element):
        encoding = 'UTF-8'
        element = ''.join(element)
        if isinstance(element, unicode):
            return element
        else:
            return unicode(element, encoding=encoding)

h = DoepagesHarvester(XML_to_JSON)

h.register_transformations([
    ('//dc:title/node()', 'title', h.NAMESPACES),
    ('//dc:description/node()', 'description', h.NAMESPACES)
])

harvest = h.harvest
normalize = h.normalize
