# This file will take an input (JSON) document, and find collision in the database
import sys
import os
sys.path.insert(1, os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
))
from website import search
from pyelasticsearch.exceptions import ElasticHttpNotFoundError


def detect(doc):
    try:
        results = search.search('scrapi', doc['id'])
    except ElasticHttpNotFoundError:
        return False

    if len(results) != 0:
        return True
    else:
        return False
