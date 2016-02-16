"""
BioMed Central harvester of public projects for the SHARE Notification Service
Note: At the moment, this harvester only harvests basic data on each article, and does
not make a seperate request for additional metadata for each record.

Example API query: http://www.biomedcentral.com/search/results?terms=*&format=json&drpAddedInLast=1&itemsPerPage=250#2015-04-23
"""


from __future__ import unicode_literals

import json
import logging
from furl import furl
from datetime import date, timedelta

from scrapi import requests
from scrapi import settings
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, datetime_formatter, compose, default_name_parser

logger = logging.getLogger(__name__)


def process_urls(urls):
    all_uris = [url['value'] for url in urls]

    processed_uris = {
        'canonicalUri': all_uris[0],
        'objectUris': [],
        'providerUris': all_uris
    }

    for url in all_uris:
        if 'dx.doi.org' in url:
            processed_uris['objectUris'].append(url)

    return processed_uris


class BiomedCentralHarvester(JSONHarvester):
    short_name = 'biomedcentral'
    long_name = 'BioMed Central'
    url = 'http://www.springer.com/us/'
    count = 0

    base_url = 'http://api.springer.com/meta/v1/json'
    URL = furl(base_url)
    URL.args['api_key'] = settings.SPRINGER_API_KEY
    URL.args['p'] = 100

    @property
    def schema(self):
        return {
            'contributors': (
                '/creators',
                compose(
                    default_name_parser,
                    lambda authors: [author['creator'] for author in authors]
                )
            ),
            'uris': ('/url', process_urls),
            'title': '/title',
            'providerUpdatedDateTime': ('/publicationDate', datetime_formatter),
            'description': '/abstract',
            'freeToRead': {
                'startDate': ('/openaccess', '/publicationDate', lambda x, y: y if x == 'true' else None)
            },
            'publisher': {
                'name': '/publisher'
            },
            'subjects': ('/genre', lambda x: [x] if x else []),
            'otherProperties': build_properties(
                ('url', '/url'),
                ('doi', '/doi'),
                ('isbn', '/isbn'),
                ('printIsbn', '/printIsbn'),
                ('electronicIsbn', '/electronicIsbn'),
                ('volume', '/volume'),
                ('number', '/number'),
                ('startingPage', '/startingPage'),
                ('copyright', '/copyright'),
                ('identifier', '/identifier')
            )
        }

    def harvest(self, start_date=None, end_date=None):

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        delta = end_date - start_date

        date_strings = []
        for i in range(delta.days + 1):
            date_strings.append(start_date + timedelta(days=i))

        search_urls = []
        for adate in date_strings:
            self.URL.args['q'] = 'date:{}'.format(adate)

            search_urls.append(self.URL.url)

        records = self.get_records(search_urls)
        records_list = []
        for record in records:
            format_type = record['publisher']
            if format_type.lower() == "biomed central":
                records_list.append(RawDocument({
                    'doc': json.dumps(record),
                    'source': self.short_name,
                    'docID': record['identifier'],
                    'filetype': 'json'
                }))
        return records_list

    def get_records(self, search_urls):
        all_records_from_all_days = []
        for search_url in search_urls:
            records = requests.get(search_url).json()
            index = 1
            total_records = int(records['result'][0]['total'])

            all_records = []
            while len(all_records) < total_records:
                record_list = records['records']
                all_records += record_list
                index += 100
                records = requests.get(search_url + '&s={}'.format(str(index), throttle=10)).json()
            all_records_from_all_days = all_records_from_all_days + all_records

        return all_records_from_all_days
