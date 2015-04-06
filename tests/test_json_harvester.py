from __future__ import unicode_literals

import json

from nameparser import HumanName
from dateutil.parser import parse

from scrapi.base import JSONHarvester
from scrapi.linter import RawDocument

expected = {
    "description": "This is a  test",
    "contributors": [
        {
            "given": "Testy",
            "suffix": "",
            "family": "Testerson",
            "middle": "",
            "prefix": "",
            "ORCID": None,
            "email": ""
        },
        {
            "given": "Test",
            "suffix": "Jr",
            "family": "Testerson",
            "middle": "",
            "prefix": "",
            "ORCID": None,
            "email": ""
        }
    ],
    "title": "Test",
    "tags": [
        "testing",
        "json tests"
    ],
    "id": {
        "url": "http://example.com",
        "serviceID": "10.10123/232ff",
        "doi": "10.10123/232ff"
    },
    "source": "crossref",
    "dateUpdated": "2015-02-02T00:00:00",
    "properties": {
        "referenceCount": "7",
        "updatePolicy": "No",
        "depositedTimestamp": "right now",
        "Empty": "",
        "Empty2": ""
    }
}

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


class TestHarvester(JSONHarvester):
    short_name = 'crossref'
    long_name = 'CrossRef'
    url = 'http://www.crossref.org'

    DEFAULT_ENCODING = 'UTF-8'

    record_encoding = None

    @property
    def schema(self):
        return {
            'title': ('/title', lambda x: x[0] if x else ''),
            'description': ('/subtitle', lambda x: x[0] if (isinstance(x, list) and x) else x or ''),
            'dateUpdated': ('/issued/date-parts', lambda x: parse(' '.join([part for part in x[0]])).isoformat().decode('utf-8')),
            'id': {
                'serviceID': '/DOI',
                'doi': '/DOI',
                'url': '/URL'
            },
            'contributors': ('/author', lambda x: [
                process_contributor(*[
                    '{} {}'.format(entry.get('given'), entry.get('family')),
                    entry.get('ORCID')
                ]) for entry in x
            ]),
            'tags': ('/subject', '/container-title', lambda x, y: [tag.lower() for tag in (x or []) + (y or [])]),
            'properties': {
                'referenceCount': '/reference-count',
                'updatePolicy': '/update-policy',
                'depositedTimestamp': '/deposited/timestamp',
                'Empty': '/trash/not-here',
                'Empty2': '/'
            }
        }

    def harvest(self, days_back=1):
        return [RawDocument({
            'doc': str(json.dumps({
                'title': ['Test', 'subtitle'],
                'subtitle': 'This is a  test',
                'issued': {
                    'date-parts': [['2015', '2', '2']]
                },
                'DOI': '10.10123/232ff',
                'URL': 'http://example.com',
                'author': [{
                    'family': 'Testerson',
                    'given': 'Testy'
                }, {
                    'family': 'Testerson Jr',
                    'given': 'Test'
                }],
                'subject': ['Testing'],
                'container-title': ['JSON tests'],
                'reference-count': '7',
                'update-policy': 'No',
                'deposited': {
                    'timestamp': 'right now'
                },
                'trash': ''
            })),
            'source': 'TEST',
            'filetype': 'json',
            'docID': '1'
        })]


class TestJSONHarvester(object):

    def setup_method(self, method):
        self.harvester = TestHarvester()

    def test_normalize(self):
        result = [
            self.harvester.normalize(record) for record in self.harvester.harvest()
        ][0]

        for key in result.attributes.keys():
            assert result[key] == expected[key]
