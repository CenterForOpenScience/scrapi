"""
Harvard Dataverse harvester of public projects for the SHARE Notification Service

Example API query: https://dataverse.harvard.edu/api/search?q=*&type=dataset&per_page=10&key=api_key_here
"""

from __future__ import unicode_literals

import json
import logging
from datetime import date
from datetime import timedelta

import furl

from scrapi import requests
from scrapi import settings
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import default_name_parser, build_properties, date_formatter

logger = logging.getLogger(__name__)


try:
    from scrapi.settings import HARVARD_DATAVERSE_API_KEY
except ImportError:
    HARVARD_DATAVERSE_API_KEY = None
    logger.error('No HARVARD_DATAVERSE_API_KEY found, Harvard Dataverse will always return []')


class HarvardDataverseHarvester(JSONHarvester):
    short_name = 'harvarddataverse'
    long_name = 'Harvard Dataverse'
    url = 'https://dataverse.harvard.edu'

    namespaces = {}

    MAX_ITEMS_PER_REQUEST = 1000
    URL = 'https://dataverse.harvard.edu/api/search/?q=*'
    TYPE = 'dataset'

    schema = {
        'title': '/name',
        'description': '/description',
        'contributors': ('/authors', default_name_parser),
        'providerUpdatedDateTime': ('/published_at', date_formatter),
        'uris': {
            'canonicalUri': '/url',
            'objectUris': [
                ('/image_url')
            ]
        },
        'otherProperties': build_properties(
            ('serviceID', '/global_id'),
            ('type', '/type')
        )
    }

    def harvest(self, start_date=None, end_date=None):
        start_date = (start_date or date.today() - timedelta(settings.DAYS_BACK)).isoformat()
        end_date = (end_date or date.today()).isoformat()

        query = furl.furl(self.URL)
        query.args['type'] = self.TYPE
        query.args['per_page'] = self.MAX_ITEMS_PER_REQUEST
        query.args['key'] = HARVARD_DATAVERSE_API_KEY
        query.args['sort'] = 'date'
        query.args['order'] = 'asc'
        query.args['fq'] = 'dateSort:[{}T00:00:00Z TO {}T00:00:00Z]'.format(start_date, end_date)

        records = self.get_records(query.url)
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

        return record_list

    def get_records(self, search_url):
        records = requests.get(search_url)
        total_records = records.json()['data']['total_count']
        start = 0
        all_records = []

        while len(all_records) < total_records:
            records = requests.get(search_url + '&start={}'.format(str(start)))
            record_list = records.json()['data']['items']

            for record in record_list:
                all_records.append(record)

            start += self.MAX_ITEMS_PER_REQUEST

        return all_records
