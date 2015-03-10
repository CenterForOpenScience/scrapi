from __future__ import unicode_literals

import functools

from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument

from .utils import get_leaves
from .utils import TEST_SCHEMA, TEST_NAMESPACES, TEST_XML_DOC


class TestHarvester(XMLHarvester):

    def harvest(self, days_back=1):
        return [RawDocument({
            'doc': str(TEST_XML_DOC),
            'source': 'TEST',
            'filetype': 'XML',
            'docID': "1"
        }) for _ in xrange(days_back)]


class TestTransformer(object):

    def setup_method(self, method):
        self.harvester = TestHarvester("TEST", TEST_SCHEMA, TEST_NAMESPACES)

    def test_normalize(self):
        results = [
            self.harvester.normalize(record) for record in self.harvester.harvest(days_back=10)
        ]

        for result in results:
            assert result['properties']['title1'] == 'Test'
            assert result['properties']['title2'] == 'test'
            assert result['properties']['title3'] == 'Testtest'

            for (k, v) in get_leaves(result.attributes):
                assert type(v) != functools.partial
