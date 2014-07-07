import unittest
import os
import sys
import shutil
import json
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))

from api import process_docs
import logging

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestAPI(unittest.TestCase):

    def tearDown(self):
        shutil.rmtree('archive/TEST')

    def test_process_raw(self):
        doc = json.dumps({'Hello':  'world'})
        source = "TEST"
        doc_id = 37
        filetype = "json"

        assert process_docs.process_raw(doc, source, doc_id, filetype)

        found = False
        for dirname, dirnames, filenames in os.walk('archive/TEST/{0}'.format(doc_id)):
            if os.path.isfile(dirname + '/raw.json'):
                found = True
        assert found

    def test_process(self):
        source = "TEST"
        doc_id = 37
        filetype = 'json'
        doc = {
            'title': "TEST PROJECT",
            'contributors': ['Me, Myself', 'And I'],
            'properties': {
                'description': 'science stuff',
                'email': 'email stuff'
            },
            'meta': {},
            'id': doc_id,
            'source': source
        }
        process_docs.process_raw(json.dumps(doc), source, doc_id, filetype)
        timestamp = None
        for dirname, dirnames, filenames in os.walk('archive/TEST/{0}'.format(doc_id)):
            if os.path.isfile(dirname + '/raw.json'):
                timestamp = dirname.split('/')[-1]
        assert timestamp

        assert process_docs.process(doc, timestamp)
