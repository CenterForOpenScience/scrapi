from __future__ import unicode_literals

from datetime import date, timedelta

from lxml import etree

from scrapi import requests
from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi.util import copy_to_unicode
from scrapi.base.schemas import BASEXMLSCHEMA
from scrapi.base.helpers import updated_schema, build_properties


class DoepagesHarvester(XMLHarvester):
    short_name = 'doepages'
    long_name = 'Department of Energy Pages'
    url = 'http://www.osti.gov/pages/'

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
                'docID': copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list

    @property
    def namespaces(self):
        return {
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcq': 'http://purl.org/dc/terms/'
        }

    @property
    def schema(self):
        return updated_schema(
            BASEXMLSCHEMA,
            {
                'otherProperties': build_properties(
                    ('language', '//dc:language/node()'),
                    ('type', '//dc:type/node()'),
                    ('typeQualifier', '//dc:typeQualifier/node()'),
                    ('language', '//dc:language/node()'),
                    ('format', '//dc:format/node()'),
                    ('identifierOther', '//dc:identifierOther/node()'),
                    ('rights', '//dc:rights/node()'),
                    ('identifierDOEcontract', '//dcq:identifierDOEcontract/node()'),
                    ('relation', '//dc:relation/node()'),
                    ('coverage', '//dc:coverage/node()'),
                    ('identifier-purl', '//dc:identifier-purl/node()'),
                    ('identifier', '//dc:identifier/node()'),
                    ('identifierReport', '//dc:identifierReport/node()'),
                    ('publisher', '//dcq:publisher/node()'),
                    ('publisherCountry', '//dcq:publisherCountry/node()'),
                    ('publisherSponsor', '//dcq:publisherSponsor/node()'),
                    ('publisherAvailability', '//dcq:publisherAvailability/node()'),
                    ('publisherResearch', '//dcq:publisherResearch/node()'),
                    ('date', '//dc:date/node()')
                )
            }
        )
