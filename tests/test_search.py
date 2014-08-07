import unittest
import os
import sys
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))

from website import search
import logging
import datetime
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class TestSearch(unittest.TestCase):

    def setUp(self):
        search.delete_all('test')
        source = "test"
        doc_id = 38
        doc = {
            'title': "TEST PROJECT",
            'contributors': ['Me, Myself', 'And I'],
            'properties': {
                'description': 'science stuff',
                'email': 'email stuff'
            },
            'meta': {},
            'id': doc_id,
            'source': source,
            'iso_timestamp': datetime.datetime.now().isoformat()
        }

        search.update(source, doc, 'article', doc_id)

    def tearDown(self):
        search.delete_all('test')

    def test_search(self):
        results, count = search.search('test', 'test')
        assert len(results) == count
        assert len(results) == 1

    def test_delete_doc(self):
        search.delete_doc('test', 'article', '38')
        results, count = search.search('test', 'test')
        assert len(results) == count
        assert len(results) == 0
