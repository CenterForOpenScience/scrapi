"""
Stepic.org harvester of MOOC-online courses for SHARE Notification Service
Example API query: https://stepic.org:443/api/lessons/100
"""

from __future__ import unicode_literals

import json

import pycountry
from dateutil.parser import parse

from scrapi import requests
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument


def process_owner(owners_id):
    resp = requests.get("https://stepic.org/api/users/" + str(owners_id)).json()
    try:
        person = resp[u'users'][0]
    except KeyError:
        person = {u'first_name': '', u'last_name': ''}
    owner = {
        'name': " ".join([person[u'first_name'], person[u'last_name']]),
        'givenName': person[u'first_name'],
        'additionalName': '',
        'familyName': person[u'last_name'],
        'email': '',
        'sameAs': [],
    }
    return [owner]


class StepicHarvester(JSONHarvester):
    short_name = 'stepic'
    long_name = 'Stepic.org Online Education Platform'
    url = 'http://www.stepic.org'
    count = 0

    URL = 'https://stepic.org/api/lessons'

    @property
    def schema(self):
        return {
            'contributors': ('/owner', process_owner),
            'uris': {
                'canonicalUri': ('/id', lambda x: self.url + '/' + str(x))
            },
            'title': '/title',
            'providerUpdatedDateTime': ('/update_date', lambda x: parse(x).isoformat()),
            'description': '/title',
            'languages': ('/language', lambda x: [pycountry.languages.get(alpha2=x).terminology])
        }

    def harvest(self, start_date=None, end_date=None):
        # TODO - stepic has no means of querying by date, we should add handling for the
        # start and end date once it does.

        search_url = self.URL
        records = self.get_records(search_url)

        record_list = []
        for record in records:
            doc_id = record['id']
            record_list.append(
                RawDocument(
                    {
                        'doc': json.dumps(record),
                        'source': self.short_name,
                        'docID': ('stepic_doc' + str(doc_id)).decode('utf-8'),
                        'filetype': 'json'
                    }
                )
            )
        return record_list

    def get_records(self, search_url):
        all_lessons = []
        resp = requests.get(self.URL + '?page=last').json()
        last_lesson_id = resp['lessons'][-1]['id']
        for pk in range(last_lesson_id + 1):
            lesson = requests.get(search_url + "/" + str(pk), expected=[200, 403, 404])
            if lesson.status_code == 200:
                lesson_list = lesson.json()['lessons'][0]
                all_lessons.append(lesson_list)
        return all_lessons
