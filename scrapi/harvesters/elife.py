"""
A harvester for metadata from eLife on Github for the SHARE project

Sample API call:
"""

from __future__ import unicode_literals

import logging
from datetime import date, timedelta

from lxml import etree

import github3

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import default_name_parser, build_properties, compose, single_result, datetime_formatter

logger = logging.getLogger(__name__)

#gather commits, each commit has a date associated with it, note max 100 per page and 3 pages pages
#https://api.github.com/repos/elifesciences/elife-articles/commits?page=1&per_page=100

#go to commit url

#go to files, scrape xml file names

#grab files from https://raw.githubusercontent.com/elifesciences/elife-articles/master/[xml file name]

#apply xml scraper
#<pub-date publication-format> </pub-date>

#for a specific article - you can reach the full journal article at http://dx.doi.org/10.7554/eLife.00181
#where e00181 is the elocation-id

#example: https://raw.githubusercontent.com/elifesciences/elife-articles/master/elife06011.xml

class ELifeHarvester(XMLHarvester):
    short_name = 'elife'
    long_name = 'eLife Sciences'
    url = 'https://github.com/elifesciences'
    DEFAULT_ENCODING = 'UTF-8'
    record_encoding = None

    namespaces = {}

    MAX_ROWS_PER_REQUEST = 999
    BASE_URL = 'https://api.github.com/repos/elifesciences/elife-articles/'
'''
    def fetch_rows(self, start_date, end_date):
        query = 'publication_date:[{}T00:00:00Z TO {}T00:00:00Z]'.format(start_date, end_date)

        resp = requests.get(self.BASE_URL, params={
            'q': query,
            'rows': '0',
            'api_key': PLOS_API_KEY,
        })

        total_rows = etree.XML(resp.content).xpath('//result/@numFound')
        total_rows = int(total_rows[0]) if total_rows else 0

        current_row = 0
        while current_row < total_rows:
            response = requests.get(self.BASE_URL, throttle=5, params={
                'q': query,
                'start': current_row,
                'api_key': PLOS_API_KEY,
                'rows': self.MAX_ROWS_PER_REQUEST,
            })

            for doc in etree.XML(response.content).xpath('//doc'):
                yield doc

            current_row += self.MAX_ROWS_PER_REQUEST

    def harvest(self, start_date=None, end_date=None):

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        if not PLOS_API_KEY:  # pragma: no cover
            return []

        return [
            RawDocument({
                'filetype': 'xml',
                'source': self.short_name,
                'doc': etree.tostring(row),
                'docID': row.xpath("str[@name='id']")[0].text,
            })
            for row in
            self.fetch_rows(start_date.isoformat(), end_date.isoformat())
            if row.xpath("arr[@name='abstract']")
            or row.xpath("str[@name='author_display']")
        ]

    schema = {
        'uris': {
            'canonicalUri': ('//str[@name="id"]/node()', compose('http://dx.doi.org/{}'.format, single_result)),
        },
        'contributors': ('//arr[@name="author_display"]/str/node()', default_name_parser),
        'providerUpdatedDateTime': ('//date[@name="publication_data"]/node()', compose(datetime_formatter, single_result)),
        'title': ('//str[@name="title_display"]/node()', single_result),
        'description': ('//arr[@name="abstract"]/str/node()', single_result),
        'publisher': {
            'name': ('//str[@name="journal"]/node()', single_result)
        },
        'otherProperties': build_properties(
            ('eissn', '//str[@name="eissn"]/node()'),
            ('articleType', '//str[@name="article_type"]/node()'),
            ('score', '//float[@name="score"]/node()')
        )
    }
'''