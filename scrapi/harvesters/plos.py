"""PLoS-API-harvester
=================
<p>To run "harvester.py" please follow the instructions:</p>
<ol>

<li>Create an account on <a href="http://register.plos.org/ambra-registration/register.action">PLOS API</a></li>

<li>Sign in <a href="http://alm.plos.org/">here</a> and click on your account name. Retrieve your API key.</li>

<li>Create a new file in the folder named "settings.py". In the file, put<br>
<code>API_KEY = (your API key)</code></li>

</ol>

Sample API query: http://api.plos.org/search?q=publication_date:[2015-01-30T00:00:00Z%20TO%202015-02-02T00:00:00Z]&api_key=ayourapikeyhere&rows=999&start=0
"""


from __future__ import unicode_literals

import logging
from datetime import date, timedelta

from lxml import etree

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import default_name_parser, build_properties, compose, single_result, date_formatter

logger = logging.getLogger(__name__)

try:
    from scrapi.settings import PLOS_API_KEY
except ImportError:
    PLOS_API_KEY = None
    logger.error('No PLOS_API_KEY found, PLoS will always return []')


class PlosHarvester(XMLHarvester):
    short_name = 'plos'
    long_name = 'Public Library of Science'
    url = 'http://www.plos.org/'

    namespaces = {}

    MAX_ROWS_PER_REQUEST = 999
    BASE_URL = 'http://api.plos.org/search'

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

        if not PLOS_API_KEY:
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
        'providerUpdatedDateTime': ('//date[@name="publication_data"]/node()', compose(date_formatter, single_result)),
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
