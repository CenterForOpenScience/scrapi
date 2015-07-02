"""
Harvard Dataverse harvester of public projects for the SHARE Notification Service
Needs start and end date parameters in API call, waiting for Harvard
Dataverse to explain how that is done (not in their documentation). 

Example API query: https://dataverse.harvard.edu/api/search?q=*&type=dataset&per_page=10&key=7b96391a-0564-4a3d-aa42-b3065cceb841
"""

from __future__ import unicode_literals

import json
import logging
from dateutil.parser import parse
from datetime import date, timedelta


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
    URL = 'https://dataverse.harvard.edu/api/search/?q=bird'
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
            # ('definedType', '/defined_type'), # NOT APPLICABLE
            ('type', '/type'),
            ('links', '/url'),
            ('publishedDate', '/published_at')
        )
    }

    def harvest(self, start_date=None, end_date=None):

        if start_date != None:
            start_date = start_date - timedelta(1) if start_date else date.today() - timedelta(1 + settings.DAYS_BACK)
            end_date = end_date - timedelta(1) if end_date else date.today() - timedelta(1)

        search_url = '{0}&type={1}&per_page={2}&key={3}'.format(
            self.URL,
            self.TYPE,
            self.MAX_ITEMS_PER_REQUEST,
            HARVARD_DATAVERSE_API_KEY
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
