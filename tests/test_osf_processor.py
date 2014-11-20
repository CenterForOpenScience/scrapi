from __future__ import unicode_literals

import re
import json
import mock
import pytest
import httpretty

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

NORM_DOC = {
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


@httpretty.activate
@mock.patch('scrapi.processing.osf.collision.already_processed')
def test_has_properties(mock_already_processed):

    httpretty.register_uri(httpretty.POST, re.compile('.*'), body=json.dumps(NORM_DOC))
    httpretty.register_uri(httpretty.PUT, re.compile('.*'))

    mock_already_processed.return_value = False,  md5().hexdigest()

    normalized = NormalizedDocument(NORM_DOC)

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(RAW_DOC, normalized)

    assert(normalized['collisionCategory'])
    assert(normalized['_id'])
