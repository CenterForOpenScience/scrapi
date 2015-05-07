"""
A harvester for the DoE's SciTech Connect Database. Makes use of SciTech's XML Querying Service. Parses the resulting XML for information and outputs the result as Json in a format that works with the OSF scrapi (scraper API).

Example API query: http://www.osti.gov/scitech/scitechxml?EntryDateFrom=02%2F02%2F2015&page=0
"""


from __future__ import unicode_literals

from datetime import datetime, date, timedelta

from lxml import etree

from dateutil.parser import *

from scrapi import requests

from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi.base.schemas import DOESCHEMA

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

    schema = DOESCHEMA

    def harvest(self, start_date=None, end_date=None):
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
            for record in self._fetch_records(start_date, end_date)
        ]

    def _fetch_records(self, start_date, end_date):
        page = 0
        morepages = True

        start_date = datetime.strptime(start_date, '%Y-%m-%d').date().strftime('%m/%d/%Y') if start_date else date.today().strftime('%m/%d/%Y')
        end_date = datetime.strptime(end_date, '%Y-%m-%d').date().strftime('%m/%d/%Y') if end_date else (date.today() - timedelta(1)).strftime('%m/%d/%Y')

        while morepages:
            resp = requests.get(self.base_url, params={
                'page': page,
                'EntryDateTo': end_date,
                'EntryDateFrom': start_date,
            })

            xml = etree.XML(resp.content)

            for record in xml.xpath('records/record'):
                yield record

            page += 1
            morepages = xml.xpath('//records/@morepages')[0] == 'true'
