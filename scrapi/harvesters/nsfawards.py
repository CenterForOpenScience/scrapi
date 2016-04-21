"""
NSF Award harvester of public projects for the SHARE Notification Service

Example API query: http://api.nsf.gov/services/v1/awards.json
"""

from __future__ import unicode_literals

import json
import logging
from datetime import date, timedelta

import six

from scrapi import requests
from scrapi import settings
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, datetime_formatter

logger = logging.getLogger(__name__)


def process_NSF_contributors(firstname, lastname, awardeename):
    # return something that's in the SHARE schema
    return [
        {
            'name': '{} {}'.format(firstname, lastname),
            'givenName': firstname,
            'familyName': lastname,
        },
        {
            'name': awardeename
        }
    ]


def process_nsf_uris(awd_id):
    nsf_url = 'http://www.nsf.gov/awardsearch/showAward?AWD_ID={}'.format(awd_id)
    return {
        'canonicalUri': nsf_url,
        'providerUri': [nsf_url]
    }


def process_sponsorships(agency, awd_id, title):
    return [
        {
            'sponsor': {
                'sponsorName': agency
            },
            'award': {
                'awardIdentifier': 'http://www.nsf.gov/awardsearch/showAward?AWD_ID={}'.format(awd_id),
                'awardName': title
            }
        }
    ]


class NSFAwards(JSONHarvester):
    short_name = 'nsfawards'
    long_name = 'NSF Awards'
    url = 'http://www.nsf.gov/'

    URL = 'http://api.nsf.gov/services/v1/awards.json?dateStart='

    schema = {
        'title': '/title',
        'contributors': ('/piFirstName', '/piLastName', '/awardeeName', process_NSF_contributors),
        'providerUpdatedDateTime': ('/date', datetime_formatter),
        'uris': ('/id', process_nsf_uris),
        'sponsorships': ('/agency', '/id', '/title', process_sponsorships),
        'otherProperties': build_properties(
            ('awardeeCity', '/awardeeCity'),
            ('awardeeStateCode', '/awardeeStateCode'),
            ('fundsObligatedAmt', '/fundsObligatedAmt'),
            ('publicAccessMandate', '/publicAccessMandate'),
        )
    }

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date if start_date else date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date - timedelta(1) if end_date else date.today() - timedelta(1)

        search_url = '{0}{1}&dateEnd={2}'.format(
            self.URL,
            start_date.strftime('%m/%d/%Y'),
            end_date.strftime('%m/%d/%Y')
        )

        records = self.get_records(search_url)

        record_list = []
        for record in records:
            doc_id = record['id']

            record_list.append(
                RawDocument(
                    {
                        'doc': json.dumps(record),
                        'source': self.short_name,
                        'docID': six.text_type(doc_id),
                        'filetype': 'json'
                    }
                )
            )

        return record_list

    def get_records(self, search_url):
        records = requests.get(search_url).json()['response'].get('award')
        offset = 1

        all_records = []
        while len(records) == 25:
            for record in records:
                all_records.append(record)

            offset += 25
            records = requests.get(search_url + '&offset={}'.format(str(offset)), throttle=3).json()['response'].get('award')
        all_records.extend(records)

        return all_records
