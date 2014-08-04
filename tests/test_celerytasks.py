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
from datetime import datetime
import yaml

MANIFEST = 'tests/test.yml'
DIRECTORY = 'TEST'
TIMESTAMP = datetime.now()

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
        results = celerytasks._consume(DIRECTORY)
        assert isinstance(results[1], _Registry)

    # TODO test 3rd part of the tuple
    def test_consume_consumer_version(self):
        results = celerytasks._consume(DIRECTORY)
        assert isinstance(results[2], str)

    def test_normalize_NormalizedDocument(self):
        output = celerytasks._consume(DIRECTORY)
        results = output[0]
        registry = output[1]
        with open(MANIFEST) as f:
            manifest = yaml.load(f)
        for result in results:
            test_doc = celerytasks._normalize(result, TIMESTAMP, registry, manifest)
            assert isinstance(test_doc, dict)

    def test_run_consumer_list(self):
        consumer_list = celerytasks.run_consumer(MANIFEST)
        assert isinstance(consumer_list, list)


## tests! 

# run_consumer.py
    # _consume.py
    # _normalize.py

# check_archive

# request_normalized
