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
from base64 import b64encode

from scrapi import tasks
from scrapi import events
from scrapi import settings

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

from scrapi.util import timestamp

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


TUTORIAL = {
    "title": "string representing title of the resource",
    "contributors": "a list of dictionaries containing prefix, middle, family, suffix, and ORCID of contributors.",
    "id": "a dictionary of unique IDs given to the resource based on the particular publication we’re accessing. Should include an entry for a URL that links right to the original resource, a DOI, and a service specific ID",
    "url": "A url pointing to the resources\' real location",
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
    events = input_data['events']
    # this is a list of scrapi rawDocuments
    raw_documents = consume(events)
    lint(lambda: consume(events), normalize)

    consumed_docs, timestamps = task_consume(raw_documents)

    storage = {'is_push': True}

    for raw in consumed_docs:
        raw['timestamps'] = timestamps
        tasks.process_raw(raw, storage=storage)
        normalized = task_normalize(raw)

        tasks.process_normalized(normalized, raw, storage=storage)


def consume(event_list):
    ''' takes a list of input from the api route,
    returns a list of raw documents 
    '''

    return [
        RawDocument({
            'doc': json.dumps(event),
            'source': event.get('source'),
            'docID': event.get('id')['serviceID'],
            'filetype': 'json'
        })
        for event in event_list
    ]


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
    events.dispatch(events.CONSUMER_RUN, events.COMPLETED,
                    consumer=source, number=len(raw_documents))

    return raw_documents, timestamps


def task_normalize(raw_doc):
    ''' emulates the normalize function in the celery
    tasks, adds timestamps to the raw doc and returns
    a single normalized document with the correct
    timestamps and raw field with link to archive
    '''

    raw_doc['timestamps']['normalizeStarted'] = timestamp()

    normalized = normalize(raw_doc)

    normalized['timestamps'] = raw_doc['timestamps']
    normalized['timestamps']['normalizeFinished'] = timestamp()

    normalized['raw'] = '{url}/{archive}{source}/{doc_id}/{consumeFinished}/raw.json'.format(
        url=settings.SCRAPI_URL,
        archive=settings.ARCHIVE_DIRECTORY,
        source=normalized['source'],
        doc_id=b64encode(raw_doc['docID']),
        consumeFinished=normalized['timestamps']['consumeFinished']
    )

    return normalized


def normalize(raw_doc):
    normalized_dict = json.loads(raw_doc['doc'])
    source = normalized_dict['source']
    events.dispatch(events.PROCESSING, events.CREATED,
                    consumer=source, docID=raw_doc['docID'])

    return NormalizedDocument(normalized_dict)
