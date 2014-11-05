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

from scrapi import tasks
from scrapi import events

from scrapi.util import timestamp

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
    # this is a list of scrapi rawDocuments
    raw_documents = consume(input_data)
    print(lint_results(input_data))

    # consumed is a tuple with scrAPI rawDocuments and timestamps
    consumed = task_consume(raw_documents)

    consumed_docs, timestamps = consumed

    storage = {'is_push': True}

    for raw in consumed_docs:
        raw['timestamps'] = timestamps
        tasks.process_raw(raw)
        normalized = normalize(raw)

        #TODO - what are the kwargs here?
        tasks.process_normalized(normalized, raw, storage=storage)

    # import pdb; pdb.set_trace()

def task_consume(raw_documents):
    ''' takes in the raw_doc_list and emulates the 
    normal scrapi consume task, adding appropriate
    timestamps and returning a tuple consisting
    of the raw doc list and the dict of timestamps
    '''

    # TODO - better way to get this? 
    raw_dict = json.loads(raw_documents[0].get('doc'))
    source = raw_dict['source']

    timestamps = {
        'consumeTaskCreated': timestamp(),
        'consumeStarted': timestamp(),
        'consumeFinished': timestamp()
    }

    # TODO - handle consumer_name
    logger.info('API Input from "{}" has finished consumption'.format(source))
    events.dispatch(events.CONSUMER_RUN, events.COMPLETED, consumer=source, number=len(raw_documents))

    return raw_documents, timestamps


def task_normalize(raw_doc):
    ''' emulates the normalize function in the celery
    tasks, adds timestamps to the raw doc and returns
    a single normalized document with the correct
    timestamps
    '''

    raw_doc['timestamps']['normalizeStarted'] = timestamp()

    normalizeStarted = timestamp()

    normalized = normalize(raw_doc)

    normalized['timestamps'] = raw_doc['timestamps']
    normalized['timestamps']['normalizeFinished'] = timestamp()

    return normalzied


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
    # import pdb; pdb.set_trace()
    source = normalized_dict['source']
    events.dispatch(events.PROCESSING, events.CREATED,
                        consumer=source, docID=raw_doc['docID'])

    return NormalizedDocument(normalized_dict)


def lint_results(input_data):
    print(lint(lambda: consume(input_data), normalize))

