# This file will take an input (JSON) document, and find collision in the database
import sys
import os
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))
from website import search


def detect(doc):
    results = search.search('scrapi', doc['id'])
    if len(results) != 0:
        return True
