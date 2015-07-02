"""
Harvard Dataverse harvester of public projects for the SHARE Notification Service

Example API query: https://dataverse.harvard.edu/api/search?q=*&type=dataset&per_page=10&key=api_key_here
"""

from __future__ import unicode_literals

import json
import logging
from dateutil.parser import parse
from datetime import date, timedelta
from datetime import datetime


from scrapi import requests
from scrapi import settings
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import default_name_parser, build_properties

logger = logging.getLogger(__name__)


try:
    from scrapi.settings import HARVARD_DATAVERSE_API_KEY
except ImportError:
    HARVARD_DATAVERSE_API_KEY = None
    logger.error('No HARVARD_DATAVERSE_API_KEY found, Harvard Dataverse will always return []')


class HarvardDataverseHarvester(JSONHarvester):
    short_name = 'harvarddataverse'
    long_name = 'harvarddataverse'
    url = 'https://dataverse.harvard.edu/'

    namespaces = {}

    MAX_ITEMS_PER_REQUEST = 20
    URL = 'https://dataverse.harvard.edu/api/search/?q=*'
    TYPE = 'dataset'

    schema = {
        'title': '/name',
        'description': '/description',
        'contributors': ('/authors', default_name_parser),
        'providerUpdatedDateTime': '/published_at',
        'uris': {
            'canonicalUri': '/url',
            'objectUris': [
                ('/image_url')
            ]
        },
        'otherProperties': build_properties(
            ('serviceID', '/global_id'),
            ('type', '/type'),
            ('links', '/url'),
            ('publishedDate', '/published_at')
        )
    }

    def harvest(self, start_date=None, end_date=None):
        if not start_date:
            start_date = datetime.datetime.now() - timedelta(2)
        if not end_date:
            end_date = datetime.datetime.now()

        search_url = '{}&type={}&per_page={}&key={}&sort=date&order=asc&fq=dateSort:[{}T00%5C:00%5C:00Z+TO+{}T00%5C:00%5C:00Z]'.format(
            self.URL,
            self.TYPE,
            self.MAX_ITEMS_PER_REQUEST,
            HARVARD_DATAVERSE_API_KEY,
            start_date,
            end_date
        )

        records = self.get_records(search_url)
        record_list = []
        for record in records:
            doc_id = record['global_id']

            record_list.append(
                RawDocument(
                    {
                        'doc': json.dumps(record),
                        'source': self.short_name,
                        'docID': doc_id,
                        'filetype': 'json'
                    }
                )
            )

        print('I harvested {} records'.format(len(record_list)))
        return record_list

    def get_records(self, search_url):
        records = requests.get(search_url)
        total_records = records.json()['data']['total_count']
        print('Gonna harvest {} records'.foramt(total_records))
        start = 0
        all_records = []

        while len(all_records) < total_records:
            records = requests.get(search_url + '&start={}'.format(str(start)))
            record_list = records.json()['data']['items']

            for record in record_list:
                all_records.append(record)

            start += self.MAX_ITEMS_PER_REQUEST

        return all_records
