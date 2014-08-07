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
from datetime import date
import yaml

MANIFEST = 'tests/test.yml'
DIRECTORY = 'TEST'
TIMESTAMP = date.today()


class TestCelerytasks(unittest.TestCase):

    def tearDown(self):
        try:
            shutil.rmtree('archive/TEST')
        except OSError as e:
            # Who cares
            if not e.errno == 2:
                raise e

    def test_consume_RawDoc(self):
        result_list = celerytasks._consume(DIRECTORY)
        for result in result_list[0]:
            assert isinstance(result, RawDocument)

    # Test consume returning a registry object (_Registry)
    def test_consume_Registry(self):
        results = celerytasks._consume(DIRECTORY)
        assert isinstance(results[1], _Registry)

    # make sure registry is a dict
    def test_consume_registry_consumers_isdict(self):
        results = celerytasks._consume(DIRECTORY)
        registry = results[1]
        registry_dicts = registry.consumers[DIRECTORY]
        assert isinstance(registry_dicts, dict)

    # Make sure registry dict has 2 correct modules
    def test_consume_registry_consumers_haskeys(self):
        results = celerytasks._consume(DIRECTORY)
        registry = results[1]
        registry_dicts = registry.consumers[DIRECTORY]
        assert registry_dicts.keys() == ['normalize', 'consume']

    # Make sure registry dict has a callable normalize function
    def test_consume_registry_hasnormalizefunc(self):
        results = celerytasks._consume(DIRECTORY)
        registry = results[1]
        registry_dicts = registry.consumers[DIRECTORY]
        assert callable(registry_dicts['normalize'])

    # Make sure registry dict has a callable normalize function
    def test_consume_registry_hasconsumefunc(self):
        results = celerytasks._consume(DIRECTORY)
        registry = results[1]
        registry_dicts = registry.consumers[DIRECTORY]
        assert callable(registry_dicts['consume'])

    # test 3rd part of the tuple (returns a str)
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
            assert isinstance(test_doc, NormalizedDocument)

    def test_run_consumer_list(self):
        consumer_list = celerytasks.run_consumer(MANIFEST)
        assert isinstance(consumer_list, list)


# tests! -- for later

# check_archive

# request_normalized
