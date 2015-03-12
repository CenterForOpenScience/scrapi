from __future__ import unicode_literals

from scrapi.base import OAIHarvester
from scrapi.linter import RawDocument

from .utils import TEST_OAI_DOC

class TestHarvester(OAIHarvester):

    def harvest(self, days_back=1):
        return [RawDocument({
            'doc': str(TEST_OAI_DOC),
            'source': 'TEST',
            'filetype': 'XML',
            'docID': "1"
        }) for _ in xrange(days_back)]


class TestOAIHarvester(object):

    def setup_method(self, method):
        self.harvester = TestHarvester(
            name='Test',
            base_url='',
            property_list=['type', 'source', 'publisher', 'format', 'date'],
        )

    def test_normalize(self):
        results = [
            self.harvester.normalize(record) for record in self.harvester.harvest()
        ]

        for res in results:
            assert res['title'] == 'Test'
