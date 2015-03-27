"""
A CrossRef harvester for the SHARE project

Example API request: http://api.crossref.org/v1/works?filter=from-pub-date:2015-02-02,until-pub-date:2015-02-02&rows=1000
"""


## Harvester for the CrossRef metadata service
from __future__ import unicode_literals

import json
import logging

from datetime import date, timedelta

from nameparser import HumanName
from dateutil.parser import parse

from scrapi import requests
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument

logger = logging.getLogger(__name__)


def process_contributor(author, orcid):
    name = HumanName(author)
    return {
        'prefix': name.title,
        'given': name.first,
        'middle': name.middle,
        'family': name.last,
        'suffix': name.suffix,
        'email': '',
        'ORCID': orcid
    }


def process_date(date):
    date = ' '.join([str(part) for part in date[0]])
    date = parse(date)
    date = date.isoformat()
    date = date.decode('utf-8')
    return date


class CrossRefHarvester(JSONHarvester):
    short_name = 'crossref'
    long_name = 'CrossRef'
    url = 'http://www.crossref.org'

    DEFAULT_ENCODING = 'UTF-8'

    record_encoding = None

    @property
    def schema(self):
        return {
            'title': ('title', lambda x: x[0] if x else ''),
            'description': ('subtitle', lambda x: x[0] if (isinstance(x, list) and x) else x or ''),
            'dateUpdated': (self.nested('issued', 'date-parts'), process_date),  # lambda x: parse(' '.join([part for part in x[0]]).isoformat().decode('utf-8'))),
            'id': {
                'serviceID': 'DOI',
                'doi': 'DOI',
                'url': 'URL'
            },
            'contributors': ('author', lambda x: [
                process_contributor(*[
                    '{} {}'.format(entry.get('given'), entry.get('family')),
                    entry.get('ORCID')
                ]) for entry in x
            ]),
            'tags': ('subject', 'container-title', lambda x, y: [tag.lower() for tag in (x or []) + (y or [])]),
            'properties': {
                'journalTitle': 'container-title',
                'volume': 'volume',
                'issue': 'issue',
                'publisher': 'publisher',
                'type': 'type',
                'ISSN': 'ISSN',
                'ISBN': 'ISBN',
                'member': 'member',
                'score': 'score',
                'issued': 'issued',
                'deposited': 'deposited',
                'indexed': 'indexed',
                'page': 'page',
                'issue': 'issue',
                'volume': 'volume',
                'referenceCount': 'reference-count',
                'updatePolicy': 'update-policy',
                'depositedTimestamp': self.nested('deposited', 'timestamp')
            }
        }

    def copy_to_unicode(self, element):
        encoding = self.record_encoding or self.DEFAULT_ENCODING
        element = ''.join(element)
        if isinstance(element, unicode):
            return element
        else:
            return unicode(element, encoding=encoding)

    def harvest(self, days_back=0):
        start_date = date.today() - timedelta(days_back)
        base_url = 'http://api.crossref.org/v1/works?filter=from-pub-date:{},until-pub-date:{}&rows={{}}&offset={{}}'.format(str(start_date), str(date.today()))
        total = requests.get(base_url.format('0', '0')).json()['message']['total-results']
        logger.info('{} documents to be harvested'.format(total))

        doc_list = []
        for i in xrange(0, total, 1000):
            records = requests.get(base_url.format(1000, i)).json()['message']['items']
            logger.info('Harvested {} documents'.format(i + len(records)))

            for record in records:
                doc_id = record['DOI']
                doc_list.append(RawDocument({
                    'doc': json.dumps(record),
                    'source': self.short_name,
                    'docID': doc_id,
                    'filetype': 'json'
                }))

        return doc_list
