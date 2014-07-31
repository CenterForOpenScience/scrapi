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


class TestCelerytasks(unittest.TestCase):

    def tearDown(self):
        try:
            shutil.rmtree('archive/.test')
        except OSError as e:
            # Who cares
            if not e.errno == 2:
                raise e

    def test_consume(self):
        pass

    def test_normalize(self):
        pass


## tests! 

## TODO: make a test consumer file that returns dummy data
# name it consumer.py in a .test directory in consumers

# run_consumer.py
    # _consume
    # _normalize

# check_archive

# request_normalized

# maybe use mock
# make it look like things have run or that urls have been accessed 