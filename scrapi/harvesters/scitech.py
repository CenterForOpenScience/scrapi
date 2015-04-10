"""
A harvester for the DoE's SciTech Connect Database. Makes use of SciTech's XML Querying Service. Parses the resulting XML for information and outputs the result as Json in a format that works with the OSF scrapi (scraper API).

Example API query: http://www.osti.gov/scitech/scitechxml?EntryDateFrom=02%2F02%2F2015&page=0
"""


from __future__ import unicode_literals

import datetime

from lxml import etree

from dateutil.parser import *

from scrapi import requests

from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
# from scrapi.base.helpers import updated_schema
from scrapi.base.schemas import BASEXMLSCHEMA

NAME = 'scitech'

DEFAULT_ENCODING = 'UTF-8'

record_encoding = None


class SciTechHarvester(XMLHarvester):
    file_format = 'xml'
    short_name = 'scitech'
    long_name = 'DoE\'s SciTech Connect Database'
    url = 'http://www.osti.gov/scitech'

    base_url = 'https://www.osti.gov/scitech/scitechxml'

    namespaces = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'dcq': 'http://purl.org/dc/terms/'
    }

    schema = BASEXMLSCHEMA
    # schema = updated_schema(BASEXMLSCHEMA, {
    #     'otherProperties': {
    #         'language': '//dc:language/node()',
    #         'type': '//dc:type/node()',
    #         'typeQualifier': '//dc:typeQualifier/node()',
    #         'language': '//dc:language/node()',
    #         'format': '//dc:format/node()',
    #         'identifierOther': '//dc:identifierOther/node()',
    #         'rights': '//dc:rights/node()',
    #         'identifierDOEcontract': '//dcq:identifierDOEcontract/node()',
    #         'relation': '//dc:relation/node()',
    #         'coverage': '//dc:coverage/node()',
    #         'identifier-purl': '//dc:identifier-purl/node()',
    #         'identifier': '//dc:identifier/node()',
    #         'identifierReport': '//dc:identifierReport/node()',
    #         'publisherInfo': {
    #             'publisher': '//dcq:publisher/node()',
    #             'publisherCountry': '//dcq:publisherCountry/node()',
    #             'publisherSponsor': '//dcq:publisherSponsor/node()',
    #             'publisherAvailability': '//dcq:publisherAvailability/node()',
    #             'publisherResearch': '//dcq:publisherResearch/node()',
    #             'date': '//dc:date/node()'
    #         }
    #     }
    # })

    def harvest(self, days_back=1):
        """A function for querying the SciTech Connect database for raw XML.
        The XML is chunked into smaller pieces, each representing data
        about an article/report. If there are multiple pages of results,
        this function iterates through all the pages."""

        return [
            RawDocument({
                'source': self.short_name,
                'filetype': self.file_format,
                'doc': etree.tostring(record),
                'docID': record.xpath('dc:ostiId/node()', namespaces=self.namespaces)[0].decode('utf-8'),
            })
            for record in self._fetch_records(days_back)
        ]

    def _fetch_records(self, days_back):
        page = 0
        morepages = True
        to_date = datetime.date.today().strftime('%m/%d/%Y')
        from_date = (datetime.date.today() - datetime.timedelta(days_back)).strftime('%m/%d/%Y')

        while morepages:
            resp = requests.get(self.base_url, params={
                'page': page,
                'EntryDateTo': to_date,
                'EntryDateFrom': from_date,
            })

            xml = etree.XML(resp.content)

            for record in xml.xpath('records/record'):
                yield record

            page += 1
            morepages = xml.xpath('//records/@morepages')[0] == 'true'
