"""
A harvester for the DoE's SciTech Connect Database. Makes use of SciTech's XML Querying Service. Parses the resulting XML for information and outputs the result as Json in a format that works with the OSF scrapi (scraper API).

Example API query: http://www.osti.gov/scitech/scitechxml?EntryDateFrom=02%2F02%2F2015&page=0
"""

from __future__ import unicode_literals

from datetime import date, timedelta

import six
from lxml import etree

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi.base.schemas import DOESCHEMA
from scrapi.util import format_date_with_slashes

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
                'docID': six.u(record.xpath('dc:ostiId/node()', namespaces=self.namespaces)[0]),
            })
            for record in self._fetch_records(start_date, end_date)
        ]

    def _fetch_records(self, start_date, end_date):
        page = 0
        morepages = True

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        while morepages:
            resp = requests.get(self.base_url, params={
                'page': page,
                'EntryDateTo': format_date_with_slashes(end_date),
                'EntryDateFrom': format_date_with_slashes(start_date),
            })

            xml = etree.XML(resp.content)

            for record in xml.xpath('records/record'):
                yield record

            page += 1
            morepages = xml.xpath('//records/@morepages')[0] == 'true'
