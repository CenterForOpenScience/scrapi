import unittest
import os
import sys
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))

from worker_manager import celerytasks
import shutil 
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
from scrapi_tools.document import RawDocument, NormalizedDocument
from scrapi_tools.manager import _Registry

MANIFEST = 'tests/test.yml'
DIRECTORY = 'TEST'

class TestCelerytasks(unittest.TestCase):

    def tearDown(self):
        try:
            shutil.rmtree('archive/.test')
        except OSError as e:
            # Who cares
            if not e.errno == 2:
                raise e

    def test_consume_RawDoc(self):
        result_list = celerytasks._consume(DIRECTORY)
        for result in result_list[0]:
            assert isinstance(result, RawDocument)

    # TODO: test consume returning a registry object (_Registry)
    def test_consume_Registry(self):
        pass

    # TODO test 3rd part of the tuple
    def test_consume_consumer_version(self):
        pass

    def test_normalize(self):
        pass

    def test_run_consumer_list(self):
        consumer_list = celerytasks.run_consumer(MANIFEST)
        assert isinstance(consumer_list, list)


## tests! 

# run_consumer.py
    # _consume.py
    # _normalize.py

# check_archive

# request_normalized
