import unittest
import os
import sys
import shutil
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
        doc = "Hello world"
        source = "TEST"
        doc_id = 37
        filetype = "test"

        assert process_docs.process_raw(doc, source, doc_id, filetype)

        found = False

        for dirname, dirnames, filenames in os.walk('archive/TEST/{0}'.format(doc_id)):
            if os.path.isfile(dirname + '/raw.test'):
                found = True
        assert found
