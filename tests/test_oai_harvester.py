from __future__ import unicode_literals

import httpretty

from scrapi.base import OAIHarvester
from scrapi.linter import RawDocument

from .utils import TEST_OAI_DOC


class TestHarvester(OAIHarvester):
    base_url = ''
    long_name = 'Test'
    short_name = 'test'
    url = 'test'
    property_list = ['type', 'source', 'publisher', 'format', 'date']

    @httpretty.activate
    def harvest(self, start_date='2015-03-14', end_date='2015-03-16'):

        request_url = 'http://validAI.edu/?from={}&to={}'.format(start_date, end_date)

        httpretty.register_uri(httpretty.GET, request_url,
                               body=TEST_OAI_DOC,
                               content_type="application/XML")

        records = self.get_records(request_url, start_date, end_date)

        return [RawDocument({
            'doc': str(TEST_OAI_DOC),
            'source': 'crossref',
            'filetype': 'XML',
            'docID': "1"
        }) for record in records]


class TestOAIHarvester(object):

    def setup_method(self, method):
        self.harvester = TestHarvester()

    def test_normalize(self):
        results = [
            self.harvester.normalize(record) for record in self.harvester.harvest()
        ]

        for res in results:
            assert res['title'] == 'Test'
