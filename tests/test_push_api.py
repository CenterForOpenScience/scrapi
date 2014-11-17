from __future__ import unicode_literals

import json
import mock
import pytest
from datetime import datetime

from scrapi import events
from scrapi.linter.document import RawDocument, NormalizedDocument

from website import process_metadata


API_INPUT = {
    'source': 'Wrestling Digest',
    'events': [
        {
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
                'yep':'A property'
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
        }
    ]
}

RAW_DOC = {
    'doc': json.dumps(API_INPUT['events'][0]),
    'docID': 'someID',
    'timestamps': {
        'consumeFinished': '2012-11-30T17:05:48+00:00',
        'consumeStarted': '2012-11-30T17:05:48+00:00',
        'consumeTaskCreated': '2012-11-30T17:05:48+00:00'
    }
}

TIMESTAMPS = {
    'consumeTaskCreated': '2012-11-30T17:05:48+00:00',
    'consumeStarted': '2012-11-30T17:05:48+00:00',
    'consumeFinished': '2012-11-30T17:05:48+00:00'
}


@pytest.fixture
def dispatch(monkeypatch):
    event_mock = mock.MagicMock()
    monkeypatch.setattr('scrapi.events.dispatch', event_mock)
    return event_mock


def test_consume_returns_list():

    result = process_metadata.consume(API_INPUT['events'])
    assert isinstance(result, list)


def test_task_consume_returns_tuple():

    result = process_metadata.task_consume(API_INPUT['events'])
    assert isinstance(result, tuple)


def test_task_consume_returns_timestamps():

    task_consume_tuple = process_metadata.task_consume(API_INPUT['events'])

    timestamps = task_consume_tuple[1]

    assert isinstance(timestamps, dict)
    assert sorted(timestamps.keys()) == [
        'consumeFinished', 'consumeStarted', 'consumeTaskCreated']

    for key, value in timestamps.iteritems():
        datetime_obj = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S.%f')

        assert isinstance(datetime_obj, datetime)


def test_task_consume_returns_rawdocs():
    task_consume_tuple = process_metadata.task_consume(API_INPUT['events'])
    raw_docs = task_consume_tuple[0]

    assert isinstance(raw_docs, list)

    for item in raw_docs:
        assert isinstance(item, dict)


def test_task_consume_calls(dispatch):
    process_metadata.task_consume(API_INPUT['events'])
    assert dispatch.called


def test_normalize_calls(dispatch):
    process_metadata.normalize(RAW_DOC)
    assert dispatch.called


def test_normalize_returns_normalized_document():
    normalized = process_metadata.normalize(RAW_DOC)
    assert isinstance(normalized, NormalizedDocument)


def test_task_normalize():
    normed = process_metadata.task_normalize(RAW_DOC)
    assert isinstance(normed, NormalizedDocument)


def test_tutorial_is_dict():
    tut = process_metadata.TUTORIAL
    assert isinstance(tut, dict)


def test_process_api_input_calls(monkeypatch):
    mock_consume = mock.MagicMock()
    mock_task_consume = mock.MagicMock()

    mock_task_consume.return_value = ([RAW_DOC], TIMESTAMPS)

    monkeypatch.setattr('website.process_metadata.consume', mock_consume)
    monkeypatch.setattr(
        'website.process_metadata.task_consume', mock_task_consume)

    process_metadata.process_api_input(API_INPUT['events'])

    assert mock_consume.called
    mock_consume.assert_called_once_with(API_INPUT['events'])

    assert mock_task_consume.called
