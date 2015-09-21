"""
BioMed Central harvester of public projects for the SHARE Notification Service
Note: At the moment, this harvester only harvests basic data on each article, and does
not make a seperate request for additional metadata for each record.

Example API query: http://www.biomedcentral.com/search/results?terms=*&format=json&drpAddedInLast=1&itemsPerPage=250#2015-04-23
"""

from __future__ import unicode_literals

import json
import logging
from datetime import date, timedelta, datetime

from nameparser import HumanName

from scrapi import requests
from scrapi import settings
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, datetime_formatter

logger = logging.getLogger(__name__)


def process_contributors(authors):

    authors = authors.strip().replace('<span class="author-names">', '').replace('</span>', '')
    authors = authors.split(',')

    if ' and ' in authors[-1] or ' <em>et al.</em>' in authors[-1]:
        split_name = authors.pop(-1).replace(' <em>et al.</em>', '').split(' and ')
        authors.extend(split_name)

    contributor_list = []
    for person in authors:
        name = HumanName(person)
        contributor = {
            'name': person,
            'givenName': name.first,
            'additionalName': name.middle,
            'familyName': name.last,
            'sameAs': [],
        }
        contributor_list.append(contributor)

    return contributor_list


class BiomedCentralHarvester(JSONHarvester):
    short_name = 'biomedcentral'
    long_name = 'BioMed Central'
    url = 'http://www.biomedcentral.com/'
    count = 0

    URL = 'http://www.biomedcentral.com/search/results?terms=*&format=json&drpAddedInLast={}&itemsPerPage=250'

    @property
    def schema(self):
        return {
            'contributors': ('/authorNames', process_contributors),
            'uris': {
                'canonicalUri': '/articleFullUrl',
                'objectUris': ('/doi', 'http://dx.doi.org/{}'.format),
                'providerUris': ('/articleFullUrl', '/abstractPath', lambda x, y: [x, y])
            },
            'title': ('/bibliographyTitle', '/blurbTitle', lambda x, y: x or y),
            'providerUpdatedDateTime': ('/published Date', datetime_formatter),
            'description': '/blurbText',
            'freeToRead': {
                'startDate': ('/is_free', '/published Date', lambda x, y: y if x else None)
            },
            'otherProperties': build_properties(
                ('imageURL', '/imageUrl', {'description': 'a image url'}),
                ('type', '/type'),
                ('isOpenAccess', '/isOpenAccess'),
                ('articleUrl', '/articleUrl'),
                ('articleFullUrl', '/articleFullUrl'),
                ('isFree', '/isFree'),
                ('isHighlyAccessed', '/isHighlyAccessed'),
                ('status', '/status'),
                ('abstractPath', '/abstractPath'),
                ('journal Id', '/journal Id'),
                ('article_host', '/article_host'),
                ('longCitation', '/longCitation'),
                ('is_subscription', '/is_subscription')
            )
        }

    def harvest(self, start_date=None, end_date=None):

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)

        # Biomed central can only have a start date
        end_date = date.today()
        date_number = end_date - start_date

        search_url = self.URL.format(date_number.days)
        records = self.get_records(search_url)

        record_list = []
        for record in records:
            doc_id = record['arxId']

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
        now = datetime.now()
        records = requests.get(search_url + "#{}".format(date.today()))
        page = 1

        all_records = []
        current_records = len(records.json()['entries'])
        while current_records > 0:
            record_list = records.json()['entries']

            for record in record_list:
                # Check to see if this is from the future, don't grab if so
                published_date = record.get('published Date')
                if datetime.strptime(published_date, '%Y-%m-%d') >= now:
                    continue
                all_records.append(record)

            page += 1
            records = requests.get(search_url + '&page={}#{}'.format(str(page), date.today()), throttle=10)
            current_records = len(records.json()['entries'])

        return all_records
