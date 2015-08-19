"""
Open Science Framework harvester of public projects for the SHARE Notification Service

Example API query: https://osf.io/api/v1/search/
https://staging.osf.io/api/v1/search/?q=category:registration%20AND%20date_created:[2015-01-01%20TO%202015-03-10]&size=1000
https://osf.io/api/v1/search/?q=category:registration%20AND%20NOT%20title=test%20AND%20NOT%20title=%22Test%20Project%22
"""

from __future__ import unicode_literals

import json
import logging
from datetime import date, timedelta

from nameparser import HumanName

from scrapi import requests
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, date_formatter

logger = logging.getLogger(__name__)


def process_contributors(authors):

    contributor_list = []
    for person in authors:
        name = HumanName(person['fullname'])
        contributor = {
            'name': person['fullname'],
            'givenName': name.first,
            'additionalName': name.middle,
            'familyName': name.last,
        }
        contributor_list.append(contributor)

    return contributor_list


def process_null(entry):
    if entry is None:
        return ''
    else:
        return entry


def process_tags(entry):
    if isinstance(entry, list):
        return entry
    else:
        return [entry]


class OSFHarvester(JSONHarvester):
    short_name = 'osf'
    long_name = 'Open Science Framework'
    url = 'http://osf.io/api/v1/search/'
    count = 0

    # Only registrations that aren't just the word "test" or "test project"
    URL = 'https://osf.io/api/v1/search/?q=category:registration ' +\
          ' AND registered_date:[{} TO {}]' +\
          ' AND NOT title=test AND NOT title="Test Project"&size=1000'

    @property
    def schema(self):
        return {
            'contributors': ('/contributors', process_contributors),
            'title': ('/title', process_null),
            'providerUpdatedDateTime': ('/date_registered', date_formatter),
            'description': ('/description', process_null),
            'uris': {
                'canonicalUri': ('/url', lambda x: 'http://osf.io' + x),
            },
            'tags': ('/tags', process_tags),
            'otherProperties': build_properties(
                ('parent_title', '/parent_title'),
                ('category', '/category'),
                ('wiki_link', '/wiki_link'),
                ('is_component', '/is_component'),
                ('is_registration', '/is_registration'),
                ('parent_url', '/parent_url'),
                ('journal Id', '/journal Id')
            )
        }

    def harvest(self, start_date=None, end_date=None):
        # Always harvest a 2 day period starting 2 days back to honor time given
        # to contributors to cancel a public registration
        start_date = start_date or date.today() - timedelta(4)
        end_date = end_date or date.today() - timedelta(2)

        search_url = self.URL.format(start_date.isoformat(), end_date.isoformat())
        records = self.get_records(search_url)

        record_list = []
        for record in records:
            doc_id = record['url'].replace('/', '')

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

        try:
            total = int(records.json()['counts']['registration'])
        except KeyError:
            return []

        from_arg = 0
        all_records = []
        while len(all_records) < total:
            record_list = records.json()['results']

            for record in record_list:
                all_records.append(record)

            from_arg += 1000
            records = requests.get(search_url + '&from={}'.format(str(from_arg)), throttle=10)

        return all_records
