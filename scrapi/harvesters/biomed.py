"""
BioMed Central harvester of public projects for the SHARE Notification Service
Note: At the moment, this harvester only harvests basic data on each article, and does
not make a seperate request for additional metadata for each record.

Example API query: http://www.biomedcentral.com/webapi/1.0/latest_articles.json
"""

from __future__ import unicode_literals

import json
import logging
from dateutil.parser import parse

from nameparser import HumanName

from scrapi import requests
from scrapi.base import BaseHarvester
from scrapi.linter.document import RawDocument, NormalizedDocument

logger = logging.getLogger(__name__)


class biomedHarvester(BaseHarvester):
    short_name = 'biomed'
    long_name = 'BioMed Central'
    url = 'http://www.biomedcentral.com/'

    file_format = 'json'
    URL = 'http://www.biomedcentral.com/search/results?terms=*&format=json&drpAddedInLast={}&itemsPerPage=250'

    def harvest(self, days_back=1):
        search_url = self.URL.format(days_back)
        records = self.get_records(search_url)

        record_list = []
        for record in records:
            doc_id = record['arxId']

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
        page = 1

        all_records = []
        current_records = len(records.json()['entries'])
        while current_records > 0:
            record_list = records.json()['entries']

            for record in record_list:
                all_records.append(record)

            page += 1
            records = requests.get(search_url + '&page={}'.format(str(page)), throttle=10)
            current_records = len(records.json()['entries'])

        return all_records

    def get_contributors(self, record):

        authors = record['authorNames']
        authors = authors.replace('<span class="author-names">', '').replace('</span>', '')
        authors = authors.split(',')

        authors = [author.strip() for author in authors]

        if ' and ' in authors[-1]:
            split_name = authors[-1].split(' and ')
            authors.pop(-1)
            authors.extend(split_name)

        contributor_list = []
        for person in authors:
            name = HumanName(person)
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

        return {
            'serviceID': unicode(record['arxId']),
            'url': record['articleFullUrl'],
            'doi': record['doi']
        }

    def get_properties(self, record):
        return {
            'blurbTitle': record['blurbTitle'],
            'imageUrl': record['imageUrl'],
            'articleUrl': record['articleUrl'],
            'type': record['type'],
            'isOpenAccess': record['isOpenAccess'],
            'isFree': record['isFree'],
            'isHighlyAccessed': record['isHighlyAccessed'],
            'status': record['status'],
            'abstractPath': record['abstractPath'],
            'journal Id': record['journal Id'],
            'published Date': record['published Date']
        }

    def normalize(self, raw_doc):
        doc = raw_doc.get('doc')
        record = json.loads(doc)

        normalized_dict = {
            'title': record['bibliograhyTitle'],
            'contributors': self.get_contributors(record),
            'properties': self.get_properties(record),
            'description': record['blurbText'],
            'tags': [],
            'id': self.get_ids(record),
            'source': self.short_name,
            'dateUpdated': unicode(parse(record['published Date']).isoformat())
        }

        return NormalizedDocument(normalized_dict)
