from __future__ import unicode_literals

from datetime import date, timedelta

import requests
from lxml import etree

from scrapi.linter import RawDocument
from scrapi.base import XMLHarvester
from scrapi.base.schemas import BASEXMLSCHEMA, update_schema


class DoepagesHarvester(XMLHarvester):

    def harvest(self, days_back=1):
        start_date = date.today() - timedelta(days_back)
        base_url = 'http://www.osti.gov/pages/pagesxml?nrows={0}&EntryDateFrom={1}'
        url = base_url.format('1', start_date.strftime('%m/%d/%Y'))
        initial_data = requests.get(url)
        record_encoding = initial_data.encoding
        initial_doc = etree.XML(initial_data.content)

        num_results = int(initial_doc.xpath('//records/@count', namespaces=self.namespaces)[0])

        url = base_url.format(num_results, start_date.strftime('%m/%d/%Y'))
        data = requests.get(url)
        doc = etree.XML(data.content)

        records = doc.xpath('records/record')

        xml_list = []
        for record in records:
            doc_id = record.xpath('dc:ostiId/node()', namespaces=self.namespaces)[0]
            record = etree.tostring(record, encoding=record_encoding)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.name,
                'docID': self.copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list

    def copy_to_unicode(self, element, encoding="UTF-8"):
        element = ''.join(element)
        if isinstance(element, unicode):
            return element
        else:
            return unicode(element, encoding=encoding)

    @property
    def name(self):
        return 'doepages'

    @property
    def namespaces(self):
        return {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcq': 'http://purl.org/dc/terms/'
        }

    @property
    def schema(self):
        return update_schema(
            BASEXMLSCHEMA,
            {
                "properties": {
                    "language": '//dc:language/node()',
                    "type": '//dc:type/node()',
                    "typeQualifier": '//dc:typeQualifier/node()',
                    "language": '//dc:language/node()',
                    "format": '//dc:format/node()',
                    "identifierOther": '//dc:identifierOther/node()',
                    "rights": '//dc:rights/node()',
                    "identifierDOEcontract": '//dcq:identifierDOEcontract/node()',
                    "relation": '//dc:relation/node()',
                    "coverage": '//dc:coverage/node()',
                    "identifier-purl": '//dc:identifier-purl/node()',
                    "identifier": '//dc:identifier/node()',
                    "identifierReport": '//dc:identifierReport/node()',
                    "publisherInfo": {
                        "publisher": '//dcq:publisher/node()',
                        "publisherCountry": '//dcq:publisherCountry/node()',
                        "publisherSponsor": '//dcq:publisherSponsor/node()',
                        "publisherAvailability": '//dcq:publisherAvailability/node()',
                        "publisherResearch": '//dcq:publisherResearch/node()',
                        "date": '//dc:date/node()'
                    }
                }
            }
        )

h = DoepagesHarvester()

harvest = h.harvest
normalize = h.normalize
