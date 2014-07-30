import unittest
import os
import sys
import shutil
import json
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))
from scrapi_tools.document import RawDocument, NormalizedDocument
from scrapi_tools.exceptions import MissingAttributeError
from api import process_docs
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestAPI(unittest.TestCase):

    def tearDown(self):
        try:
            shutil.rmtree('archive/TEST')
        except OSError as e:
            # Who cares
            if not e.errno == 2:
                raise e

    def test_process_raw(self):
        raw_file = RawDocument({
            'doc': json.dumps({'Hello':  'world'}),
            'source': "TEST",
            'doc_id': 37,
            'filetype': "json"
        })

        assert process_docs.process_raw(raw_file, 'test-version')

        found = False
        for dirname, dirnames, filenames in os.walk('archive/TEST/{0}'.format(raw_file.get('doc_id'))):
            if os.path.isfile(dirname + '/raw.json'):
                found = True
        assert found

    def test_process_legal(self):
        raw_doc = RawDocument({
            'doc': json.dumps({'Hello': 'world'}),
            'source': 'TEST',
            'doc_id': 37,
            'filetype': 'json'
        })
        ts = str(process_docs.process_raw(raw_doc, 'test-version'))
        timestamp = None
        for dirname, dirnames, filenames in os.walk('archive/TEST/{0}'.format(raw_doc.get('doc_id'))):
            if os.path.isfile(dirname + '/raw.json'):
                timestamp = dirname.split('/')[-1]
        assert timestamp == ts

        doc = NormalizedDocument({
            'title': "TEST PROJECT",
            'contributors': ['Me, Myself', 'And I'],
            'properties': {
                'description': 'science stuff',
                'email': 'email stuff'
            },
            'meta': {},
            'id': raw_doc.get('doc_id'),
            'source': raw_doc.get('source'),
            'timestamp': str(timestamp)
        })

        assert process_docs.process(doc, timestamp)

        found = False
        for dirname, dirnames, filenames in os.walk('archive/TEST/{0}'.format(raw_doc.get('doc_id'))):
            if os.path.isfile(dirname + '/normalized.json'):
                found = True
        assert found

    def test_process_illegal(self):
        with self.assertRaises(MissingAttributeError):
            RawDocument({
                'doc': json.dumps({'Hello': 'world'}),
                'source': 'TEST',
                'filetype': 'json'
            })
