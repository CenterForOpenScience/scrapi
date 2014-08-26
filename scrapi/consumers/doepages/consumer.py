## Consumer for DOE Pages for SHARE

import requests
from lxml import etree
from datetime import date, timedelta

from scrapi_tools import lint
from scrapi_tools.document import RawDocument, NormalizedDocument


NAME = 'doepages'

NAMESPACES = {'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#', 
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcq': 'http://purl.org/dc/terms/'}

def consume():
    pass


def normalize():
    pass

if __name__ == '__main__':
    print(lint(consume, normalize))
