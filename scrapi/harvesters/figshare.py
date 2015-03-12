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
from scrapi.base import BaseHarvester
from scrapi.linter.document import RawDocument, NormalizedDocument

logger = logging.getLogger(__name__)


class FigshareHarvester(BaseHarvester):
    short_name = 'figshare'
    long_name = 'figshare'
    file_format = 'json'
    URL = 'http://api.figshare.com/v1/articles/search?search_for=*&from_date='

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

    def get_contributors(self, record):

        authors = record['authors']

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

    def get_ids(self, record):
        # Right now, only take the last DOI - others in properties
        doi = record['DOI']
        try:
            doi = doi.replace('http://dx.doi.org/', '')
        except AttributeError:
            for item in doi:
                item.replace('http://dx.doi.org/', '')
                doi = item

        return {
            'serviceID': unicode(record['article_id']),
            'url': record['url'],
            'doi': doi
        }

    def get_properties(self, record):
        return {
            'article_id': record['article_id'],
            'defined_type': record['defined_type'],
            'type': record['type'],
            'links': record['links'],
            'doi': record['DOI'],
            'publishedDate': record['published_date']
        }

    def normalize(self, raw_doc):
        doc = raw_doc.get('doc')
        record = json.loads(doc)

        normalized_dict = {
            'title': record['title'],
            'contributors': self.get_contributors(record),
            'properties': self.get_properties(record),
            'description': record['description'],
            'tags': [],
            'id': self.get_ids(record),
            'source': self.short_name,
            'dateUpdated': unicode(parse(record['modified_date']).isoformat())
        }

        return NormalizedDocument(normalized_dict)
