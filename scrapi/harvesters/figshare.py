"""
Figshare harvester of public projects for the SHARE Notification Service
Note: At the moment, this harvester only harvests basic data on each article, and does
not make a seperate request for additional metadata for each record.

Example API query: http://api.figshare.com/v1/articles/search?search_for=*&from_date=2015-2-1&end_date=2015-2-1
"""

from __future__ import unicode_literals

import json
import logging
from dateutil.parser import parse
from datetime import date, timedelta

from nameparser import HumanName

from scrapi import requests
from scrapi.base import JSONHarvester
from scrapi.base.schemas import CONSTANT
from scrapi.linter.document import RawDocument

logger = logging.getLogger(__name__)


def process_contributors(authors):

    contributor_list = []
    for person in authors:
        name = HumanName(person['author_name'])
        contributor = {
            'prefix': name.title,
            'given': name.first,
            'middle': name.middle,
            'family': name.last,
            'suffix': name.suffix,
            'email': '',
            'ORCID': '',
        }
        contributor_list.append(contributor)

    return contributor_list


class FigshareHarvester(JSONHarvester):
    short_name = 'figshare'
    long_name = 'figshare'
    url = 'http://figshare.com/'

    URL = 'http://api.figshare.com/v1/articles/search?search_for=*&from_date='

    schema = {
        'title': 'title',
        'description': 'description',
        'contributors': ('authors', process_contributors),
        'tags': CONSTANT([]),
        'dateUpdated': ('modified_date', lambda x: parse(x).isoformat().decode('utf-8')),
        'id': {
            'url': ('DOI', lambda x: x[0] if isinstance(x, list) else x),
            'serviceID': ('article_id', lambda x: str(x).decode('utf-8')),
            'doi': ('DOI', lambda x: x[0].replace('http://dx.doi.org/', '') if isinstance(x, list) else x.replace('http://dx.doi.org/', ''))
        },
        'properties': {
            'definedType': 'defined_type',
            'type': 'type',
            'links': 'links',
            'publishedDate': 'published_date'
        }
    }

    def harvest(self, days_back=0):
        start_date = date.today() - timedelta(days_back) - timedelta(1)
        end_date = date.today() - timedelta(1)
        search_url = '{0}{1}-{2}-{3}&end_date={4}-{5}-{6}'.format(
            self.URL,
            start_date.year,
            start_date.month,
            start_date.day,
            end_date.year,
            end_date.month,
            end_date.day
        )

        records = self.get_records(search_url)

        record_list = []
        for record in records:
            doc_id = record['article_id']

            record_list.append(
                RawDocument(
                    {
                        'doc': json.dumps(record),
                        'source': self.short_name,
                        'docID': unicode(doc_id),
                        'filetype': 'json'
                    }
                )
            )

        return record_list

    def get_records(self, search_url):
        records = requests.get(search_url)
        total_records = records.json()['items_found']
        page = 1

        all_records = []
        while len(all_records) < total_records:
            record_list = records.json()['items']

            for record in record_list:
                if len(all_records) < total_records:
                    all_records.append(record)

            page += 1
            records = requests.get(search_url + '&page={}'.format(str(page)), throttle=3)

        return all_records
