from __future__ import unicode_literals

import mock
import pytest

from website import process_metadata


@pytest.fixture
def dispatch(monkeypatch):
    event_mock = mock.MagicMock()
    monkeypatch.setattr('scrapi.events.dispatch', event_mock)
    return event_mock


def test_consume_returns_list():

    events = [{'source': 'dudleyz', 'id': {'serviceID': 'getthetables'}}]
    result = process_metadata.consume(events)
    assert isinstance(result, list)


def test_task_consume_returns_tuple():

    events = [{'source': 'cmpunk', 'id': {'serviceID': 'bestintheworld'}}]
    result = process_metadata.task_consume(events)

    assert isinstance(result, tuple)


def test_task_consume_calls(monkeypatch, dispatch):

    mock_consume = mock.MagicMock()
    monkeypatch.setattr('website.process_metadata.consume', mock_consume)

    events = [{'source': 'dudleyz', 'id': {'serviceID': 'getthetables'}}]
    process_metadata.task_consume(events)
    assert dispatch.called

