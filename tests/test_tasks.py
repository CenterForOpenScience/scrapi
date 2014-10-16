import mock
import pytest

from scrapi import tasks


@pytest.fixture
def dispatch(monkeypatch):
    event_mock = mock.MagicMock()
    monkeypatch.setattr('scrapi.events.dispatch', event_mock)
    return event_mock


@pytest.mark.usesfixtures('consumer')
def test_run_consumer(monkeypatch, dispatch, consumer):
    tasks.run_consumer('test')
    assert dispatch.called
