from __future__ import unicode_literals

import functools

from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi.base.schemas import update_schema

from .utils import get_leaves
from .utils import TEST_SCHEMA, TEST_NAMESPACES, TEST_XML_DOC


class TestHarvester(XMLHarvester):

    SCHEMA = TEST_SCHEMA

    def harvest(self, days_back=1):
        return [RawDocument({
            'doc': str(TEST_XML_DOC),
            'source': 'TEST',
            'filetype': 'XML',
            'docID': "1"
        }) for _ in xrange(days_back)]


    @property
    def name(self):
        return 'TEST'

    @property
    def namespaces(self):
        return TEST_NAMESPACES

    @property
    def schema(self):
        return self.SCHEMA


class TestTransformer(object):

    def setup_method(self, method):
        self.harvester = TestHarvester()

    def test_arg_kwargs(self):
        def process_title(title, title1="test"):
            return title + title1
        def process_title2(title1="test"):
            return title

        args = ("//dc:title/node()", )
        kwargs = {"title1": "//dc:title/node()"}

        self.harvester.SCHEMA = update_schema(
            TEST_SCHEMA,
            {
                'title': ((args, kwargs), process_title),
                'properties': {
                    'title2': ((args,), process_title),
                    'title3': ((kwargs, ), process_title2),
                }
            }
        )

        results = [self.harvester.normalize(record) for record in self.harvester.harvest(days_back=1)]

        for result in results:
            assert result['title'] == "TestTest"
            assert result['properties']['title2'] == 'Testtest'
            assert result['properties']['title3'] == 'Test'

    def test_arg_kwargs_fail(self):
        self.harvester.SCHEMA = update_schema(
            TEST_SCHEMA,
            {"title": (("test", ), lambda x: x)}
        )
        try:
            results = [self.harvester.normalize(record) for record in self.harvester.harvest()]
        except ValueError:
            assert True
        else:
            assert False

    def test_normalize(self):
        results = [
            self.harvester.normalize(record) for record in self.harvester.harvest(days_back=10)
        ]

        for result in results:
            assert result['title'] == "Title overwritten"
            assert result['properties']['title1'] == 'Test'
            assert result['properties']['title2'] == 'test'
            assert result['properties']['title3'] == 'Testtest'

            for (k, v) in get_leaves(result.attributes):
                assert type(v) != functools.partial
