# -*- coding: utf-8 -*-
"""
    PUSH API for scrapi prototype
"""
from __future__ import unicode_literals

import copy
import json
import logging
import requests
import datetime

# from scrapi import settings
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument


logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def tutorial():
    return {
        "title": "string representing title of the resource",
        "contributors": "a list of dictionaries containing prefix, middle, family, suffix, and ORCID of contributors.",
        "id": "a dictionary of unique IDs given to the resource based on the particular publication we’re accessing. Should include an entry for a URL that links right to the original resource, a DOI, and a service specific ID",
        "url": "A url pointing to the resources\'' real location",
        "doi": "The digital object identifier of the resource, if it has one",
        "serviceID": "A service specific identifier for the resource",
        "description": "an abstract or general description of the resource",
        "tags": "a list of tags or keywords identified in the resource itself, normalized to be all lower case",
        "source": "string identifying where the resource came from",
        "timestamp": "string indicating when the resource was accessed by scrAPI using the format YYYY-MM-DD h : m : s in iso format",
        "dateCreated": "string indicating when the resource was first created or published using the format YYYY-MM-DD in iso format",
        "dateUpdated": "string indicating when the resource was last updated in the home repository using the format YYYY-MM-DD in iso format",
    }

def process_api_input(input_data):
    ''' Takes a list of documents as raw input from API route
    returns a list of linted normalzied documents
    '''
    raw_documents = consume(input_data)
    
    return lint_results(input_data)


def consume(input_data):
    ''' takes a list of input from the api route,
    returns a list of raw documents 
    '''
    event_list = json.loads(input_data)

    raw_doc_list = []
    for event in event_list:
        raw_doc_list.append(RawDocument({
            'doc': json.dumps(event),
            'source': event.get('source'),
            'docID': event.get('id')['serviceID'],
            'filetype': 'json'
        }))

    return raw_doc_list


def normalize(raw_doc):
    raw = raw_doc.get('doc')
    normalized_dict = json.loads(raw)

    return NormalizedDocument(normalized_dict)


def lint_results(input_data):
    print(lint(lambda: consume(input_data), normalize))

