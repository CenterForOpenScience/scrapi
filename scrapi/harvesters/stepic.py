# """
# BioMed Central harvester of public projects for the SHARE Notification Service
# Note: At the moment, this harvester only harvests basic data on each article, and does
# not make a seperate request for additional metadata for each record.
#
# Example API query: http://www.biomedcentral.com/webapi/1.0/latest_articles.json
# """

"""
Stepic.org harvester of MOOC-online courses for SHARE Notification Service
Example API query: https://stepic.org:443/api/lessons/100
"""


from __future__ import unicode_literals

import json
import logging
from dateutil.parser import parse
import pycountry

from scrapi import requests
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument

logger = logging.getLogger(__name__)


def process_owner(owners_id):
    logger.info(owners_id)
    resp = requests.get("https://stepic.org:443/api/users/" + str(owners_id)).json()
    try:
        person = resp[u'users'][0]
    except KeyError:
        person = {u'first_name': 'None', u'last_name': 'None'}
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
    url = 'http://www.stepic.org/lesson'
    count = 0

    URL = 'https://stepic.org:443/api/lessons'

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
            'languages': ('/language', lambda x: [pycountry.languages.get(alpha2=x).terminology, ])
        }

    def harvest(self, days_back=1):

        search_url = self.URL
        records = self.get_records(search_url)

        record_list = []
        for record in records:
            doc_id = record['id']
            logger.info(doc_id)
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
        pk = 1
        lesson = requests.get(search_url + "/" + str(pk))
        resp = requests.get(self.URL + '?page=last').json()
        last_lesson_id = resp['lessons'][-1]['id']
        while pk < last_lesson_id:
            if lesson.status_code == 200:
                lesson_list = lesson.json()['lessons'][0]
                all_lessons.append(lesson_list)
            pk += 1
            lesson = requests.get(search_url + "/" + str(pk))
        return all_lessons
