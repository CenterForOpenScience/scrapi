from __future__ import unicode_literals

import re
import json
import mock
import httpretty
from copy import deepcopy

from hashlib import md5

import utils

from scrapi import settings

from scrapi.processing import osf

from scrapi.linter.document import NormalizedDocument


NORMALIZED = NormalizedDocument(utils.RECORD)


@httpretty.activate
@mock.patch('scrapi.processing.osf.collision.already_processed')
def test_has_properties(mock_already_processed):

    normed = deepcopy(NORMALIZED)

    httpretty.register_uri(httpretty.POST, re.compile('{}/.*'.format(settings.OSF_PREFIX)), body=json.dumps(utils.RECORD))
    httpretty.register_uri(httpretty.PUT, re.compile('{}/.*'.format(settings.OSF_PREFIX)))

    mock_already_processed.return_value = False, md5().hexdigest()

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(utils.RAW_DOC, normed)

    assert(normed['collisionCategory'])
    assert(normed['_id'])


@mock.patch('scrapi.processing.osf.collision.already_processed')
def test_found_returns(mock_already_processed):
    normed = deepcopy(NORMALIZED)

    mock_already_processed.return_value = True, md5().hexdigest()

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(utils.RAW_DOC, normed)

    assert 'meta' not in normed.attributes.keys()


@httpretty.activate
@mock.patch('scrapi.processing.osf.collision.already_processed')
@mock.patch('scrapi.processing.osf.collision.detect_collisions')
def test_contrib_deleted_if_resource(mock_detect_collisions, mock_already_processed):
    normed = deepcopy(NORMALIZED)
    resource = deepcopy(utils.RECORD)

    httpretty.register_uri(httpretty.PUT, re.compile('{}/.*'.format(settings.OSF_PREFIX)))
    httpretty.register_uri(httpretty.POST, re.compile('{}/.*'.format(settings.OSF_PREFIX)), body=json.dumps(utils.RECORD))

    mock_already_processed.return_value = False, md5().hexdigest()

    mock_detect_collisions.return_value = resource

    resource['meta'] = {}

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(utils.RAW_DOC, normed)

    assert 'contributors' not in resource.keys()


@httpretty.activate
@mock.patch('scrapi.processing.osf.collision.already_processed')
@mock.patch('scrapi.processing.osf.collision.detect_collisions')
def test_report_id_added_to_cmids(mock_detect_collisions, mock_already_processed):

    normed = deepcopy(NORMALIZED)
    resource = deepcopy(utils.RECORD)

    httpretty.register_uri(httpretty.POST, re.compile('{}/.*'.format(settings.OSF_PREFIX)), body=json.dumps(utils.RECORD))
    httpretty.register_uri(httpretty.PUT, re.compile('{}/.*'.format(settings.OSF_PREFIX)))

    mock_already_processed.return_value = False, md5().hexdigest()

    mock_detect_collisions.return_value = resource

    resource['meta'] = {}
    resource['attached'] = {}
    resource['attached']['cmids'] = ['some stuff']

    osf_processor = osf.OSFProcessor()
    osf_processor.process_normalized(utils.RAW_DOC, normed)

    assert len(resource['attached']['cmids']) > 1
    assert normed['_id'] in resource['attached']['cmids']
