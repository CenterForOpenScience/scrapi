# -*- coding: utf-8 -*-
"""
    PUSH API for scrapi prototype
"""
from __future__ import unicode_literals

import json
import logging
from base64 import b64encode

from scrapi import tasks
from scrapi import events
from scrapi import settings
from scrapi.util import timestamp
from scrapi.linter.document import RawDocument, NormalizedDocument

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


def process_api_input(events):
    ''' Takes a list of documents as raw input from API route
    returns a list of linted normalzied documents
    '''

    # this is a list of scrapi rawDocuments
    raw_documents = harvest(events)

    harvested_docs, timestamps = task_harvest(raw_documents)

    storage = {'is_push': True}

    for raw in harvested_docs:
        raw['timestamps'] = timestamps
        tasks.process_raw.delay(raw, storage=storage)
        normalized = task_normalize(raw)

        tasks.process_normalized.delay(normalized, raw, storage=storage)


def harvest(event_list):
    ''' takes a list of input from the api route,
    returns a list of raw documents
    '''

    return [
        RawDocument({
            'filetype': 'json',
            'doc': json.dumps(event),
            'source': event['source'],
            'docID': event['id']['serviceID']
        })
        for event in event_list
    ]


def task_harvest(raw_documents):
    ''' takes in the raw_doc_list and emulates the
    normal scrapi harvest task, adding appropriate
    timestamps and returning a tuple consisting
    of the raw doc list and the dict of timestamps
    '''

    source = raw_documents[0]['source']

    timestamps = {
        'harvestTaskCreated': timestamp(),
        'harvestStarted': timestamp(),
        'harvestFinished': timestamp()
    }

    # TODO - handle harvester_name
    logger.info('API Input from "{}" has finished harvesting'.format(source))
    events.dispatch(events.HARVESTER_RUN, events.COMPLETED,
                    harvester=source, number=len(raw_documents))

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

    normalized['dateCollected'] = normalized['timestamps']['harvestFinished']

    return normalized


def normalize(raw_doc):
    normalized_dict = json.loads(raw_doc['doc'])
    source = normalized_dict['source']
    events.dispatch(events.PROCESSING, events.CREATED,
                    harvester=source, docID=raw_doc['docID'])

    return NormalizedDocument(normalized_dict)
