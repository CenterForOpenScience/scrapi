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
from datetime import datetime, timedelta

from lxml import etree
from dateutil.parser import *

from scrapi import requests
from scrapi.base import XMLHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import default_name_parser, build_properties, compose, single_result

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

    def build_query(self, days_back):
        to_date = datetime.utcnow()
        from_date = (datetime.utcnow() - timedelta(days=days_back))

        to_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0)
        from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)
        return 'publication_date:[{}Z TO {}Z]'.format(from_date.isoformat(), to_date.isoformat())

    def fetch_rows(self, days_back):
        query = self.build_query(days_back)

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

    def harvest(self, days_back=1):
        if not PLOS_API_KEY:
            return []

        return [
            RawDocument({
                'filetype': 'xml',
                'source': self.short_name,
                'doc': etree.tostring(row),
                'docID': row.xpath("str[@name='id']")[0].text.decode('utf-8'),
            })
            for row in
            self.fetch_rows(days_back)
            if row.xpath("arr[@name='abstract']")
            or row.xpath("str[@name='author_display']")
        ]

    schema = {
        'uris': {
            'canonicalUri': ('//str[@name="id"]/node()', compose('http://dx.doi.org/{}'.format, single_result)),
        },
        'contributors': ('//arr[@name="author_display"]/str/node()', default_name_parser),
        'providerUpdatedDateTime': ('//date[@name="publication_data"]/node()', compose(lambda x: parse(x).date().isoformat().decode('utf-8'), single_result)),
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
