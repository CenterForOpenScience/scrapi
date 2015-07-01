from __future__ import unicode_literals

import functools

import mock
import pytest
from lxml.etree import XPathEvalError
from jsonschema.exceptions import ValidationError
from six.moves import xrange

from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi import base
from scrapi.base.helpers import updated_schema, pack, build_properties, CONSTANT

from .utils import TEST_SCHEMA, TEST_NAMESPACES, TEST_XML_DOC


class TestHarvester(XMLHarvester):
    long_name = 'TEST'
    short_name = 'test'
    url = 'TEST'
    schema = TEST_SCHEMA
    namespaces = TEST_NAMESPACES

    def harvest(self, days_back=1):
        return [RawDocument({
            'doc': TEST_XML_DOC,
            'source': 'test',
            'filetype': 'XML',
            'docID': "1"
        }) for _ in xrange(days_back)]


class TestTransformer(object):

    def setup_method(self, method):
        self.harvester = TestHarvester()

    def test_arg_kwargs(self):
        def process_title(title, title1="test"):
            return title[0] + (title1[0] if isinstance(title1, list) else title1)

        def process_title2(title1="test"):
            return title1[0] if isinstance(title1, list) else title1

        args = ("//dc:title/node()", )
        kwargs = {"title1": "//dc:title/node()"}

        self.harvester.schema = updated_schema(
            TEST_SCHEMA,
            {
                'title': (pack(*args, **kwargs), process_title),
                'otherProperties': build_properties(
                    ('title2', (pack(*args), process_title)),
                    ('title3', (pack(**kwargs), process_title2)),
                    ('title4', (pack('//dc:title/node()', title1='//dc:title/node()'), process_title))
                )
            }
        )


        results = [self.harvester.normalize(record) for record in self.harvester.harvest(days_back=1)]

        for result in results:
            assert result['title'] == "TestTest"
            assert result['otherProperties'][0]['properties']['title2'] == 'Testtest'
            assert result['otherProperties'][1]['properties']['title3'] == 'Test'
            assert result['otherProperties'][2]['properties']['title4'] == "TestTest"

    def test_normalize(self):
        results = [
            self.harvester.normalize(record) for record in self.harvester.harvest(days_back=1)
        ]

        for result in results:
            assert result['title'] == "Title overwritten"
            assert result['otherProperties'][0]['properties']['title1'] == 'Test'
            assert result['otherProperties'][1]['properties']['title2'] == 'test'
            assert result['otherProperties'][2]['properties']['title3'] == 'Testtest'

    def test_constants(self):
        self.harvester.schema = updated_schema(
            TEST_SCHEMA, {
                'tags': (CONSTANT(['X']), lambda x: x),
                'otherProperties': [{
                    'name': CONSTANT('test'),
                    'properties':{
                        'test':  CONSTANT('test')
                    },
                    'uri': CONSTANT('http://example.com'),
                    'description': CONSTANT('A test field')
                }]
            }
        )
        results = [
            self.harvester.normalize(record) for record in self.harvester.harvest(days_back=1)
        ]

        for result in results:
            assert result['otherProperties'][0]['properties']['test'] == 'test'
            assert result['tags'] == ['X']

    def test_failing_transformation_with_raises(self):
        base.settings.RAISE_IN_TRANSFORMER = True

        self.harvester.schema = updated_schema(TEST_SCHEMA, {'title': 'A completely 1n\/@lid expre55ion'})

        with pytest.raises(XPathEvalError) as e:
            x = [self.harvester.normalize(record) for record in self.harvester.harvest()]

    def test_failing_transformation_wont_raise(self):
        base.transformer.logger.setLevel(50)
        base.settings.RAISE_IN_TRANSFORMER = False

        self.harvester.schema = updated_schema(TEST_SCHEMA, {'title': 'A completely 1n\/@lid expre55ion'})

        with pytest.raises(ValidationError) as e:
            x = [self.harvester.normalize(record) for record in self.harvester.harvest()]
