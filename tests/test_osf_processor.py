from __future__ import unicode_literals

import re
import json
import mock
import pytest
import httpretty
from copy import deepcopy

from hashlib import md5

from scrapi import settings

from scrapi.processing import osf

from scrapi.linter.document import RawDocument, NormalizedDocument


RAW_DOC = {
    'doc': '{}',
    'docID': 'someID',
    'timestamps': {
        'consumeFinished': '2012-11-30T17:05:48+00:00',
        'consumeStarted': '2012-11-30T17:05:48+00:00',
        'consumeTaskCreated': '2012-11-30T17:05:48+00:00'
    }
}

RECORD = {
    'title': 'Using Table Stable Carbon in Gold and STAR Isotopes',
    'contributors': [
        {
            'prefix': 'The One And Only',
            'given': 'DEVON',
            'middle': 'Get The Tables',
            'family': 'DUDLEY',
            'suffix': 'Thirsty Boy',
            'email': 'dudley.boyz@email.uni.edu',
            'ORCID': 'BubbaRayDudley'
        }
    ],
    'id': {
        'url': 'http://www.plosone.org/article',
        'doi': '10.1371/doi.DOI!',
        'serviceID': 'AWESOME'
    },
    'properties': {
        'figures': ['http://www.plosone.org/article/image.png'],
        'type': 'text',
                'yep': 'A property'
    },
    'description': 'This study seeks to understand how humans impact\
            the dietary patterns of eight free-ranging vervet monkey\
            (Chlorocebus pygerythrus) groups in South Africa using stable\
            isotope analysis.',
    'tags': [
        'behavior',
        'genetics'
    ],
    'source': 'example_pusher',
    'dateCreated': '2012-11-30T17:05:48+00:00',
    'dateUpdated': '2015-02-23T17:05:48+00:01',
    '_id': 'yes! yes! yes!',
    'count': 0
}

NORMALIZED = NormalizedDocument(RECORD)


@httpretty.activate
@mock.patch('scrapi.processing.osf.collision.already_processed')
def test_has_properties(mock_already_processed):

    normed = deepcopy(NORMALIZED)

    httpretty.register_uri(httpretty.POST, re.compile('{}/.*'.format(settings.OSF_PREFIX)), body=json.dumps(RECORD))
    httpretty.register_uri(httpretty.PUT, re.compile('{}/.*'.format(settings.OSF_PREFIX)))

    mock_already_processed.return_value = False,  md5().hexdigest()

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(RAW_DOC, normed)

    assert(normed['collisionCategory'])
    assert(normed['_id'])


@mock.patch('scrapi.processing.osf.collision.already_processed')
def test_found_returns(mock_already_processed):
    normed = deepcopy(NORMALIZED)

    mock_already_processed.return_value = True,  md5().hexdigest()

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(RAW_DOC, normed)

    assert 'meta' not in normed.attributes.keys()


@httpretty.activate
@mock.patch('scrapi.processing.osf.collision.already_processed')
@mock.patch('scrapi.processing.osf.collision.detect_collisions')
def test_contrib_deleted_if_resource(mock_detect_collisions, mock_already_processed):
    normed = deepcopy(NORMALIZED)
    resource = deepcopy(RECORD)

    httpretty.register_uri(httpretty.POST, re.compile('{}/.*'.format(settings.OSF_PREFIX)), body=json.dumps(RECORD))
    httpretty.register_uri(httpretty.PUT, re.compile('{}/.*'.format(settings.OSF_PREFIX)))

    mock_already_processed.return_value = False,  md5().hexdigest()

    mock_detect_collisions.return_value = resource

    resource['meta'] = {}

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(RAW_DOC, normed)

    assert 'contributors' not in resource.keys()


@httpretty.activate
@mock.patch('scrapi.processing.osf.collision.already_processed')
@mock.patch('scrapi.processing.osf.collision.detect_collisions')
def test_report_id_added_to_cmids(mock_detect_collisions, mock_already_processed):

    normed = deepcopy(NORMALIZED)
    resource = deepcopy(RECORD)

    httpretty.register_uri(httpretty.POST, re.compile('{}/.*'.format(settings.OSF_PREFIX)), body=json.dumps(RECORD))
    httpretty.register_uri(httpretty.PUT, re.compile('{}/.*'.format(settings.OSF_PREFIX)))

    mock_already_processed.return_value = False,  md5().hexdigest()

    mock_detect_collisions.return_value = resource

    resource['meta'] = {}
    resource['attached'] = {}
    resource['attached']['cmids'] = ['some stuff']

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(RAW_DOC, normed)

    assert len(resource['attached']['cmids']) > 1
    assert normed['_id'] in resource['attached']['cmids']
