import mock
import pytest

from scrapi import tasks
from scrapi import events


@pytest.fixture
def dispatch(monkeypatch):
    event_mock = mock.MagicMock()
    monkeypatch.setattr('scrapi.events.dispatch', event_mock)
    return event_mock


def test_run_consumer_calls(monkeypatch, dispatch):
    mock_consume = mock.MagicMock()
    mock_begin_norm = mock.MagicMock()

    monkeypatch.setattr('scrapi.tasks.consume', mock_consume)
    monkeypatch.setattr('scrapi.tasks.begin_normalization', mock_begin_norm)

    tasks.run_consumer('test')

    assert dispatch.called
    assert mock_consume.si.called
    assert mock_begin_norm.s.called

    mock_begin_norm.s.assert_called_once_with('test')
    mock_consume.si.assert_called_once_with('test', 'TIME', days_back=1)


def test_run_consumer_daysback(monkeypatch, dispatch):
    mock_consume = mock.MagicMock()
    mock_begin_norm = mock.MagicMock()

    monkeypatch.setattr('scrapi.tasks.consume', mock_consume)
    monkeypatch.setattr('scrapi.tasks.begin_normalization', mock_begin_norm)

    tasks.run_consumer('test', days_back=10)

    assert dispatch.called
    assert mock_consume.si.called
    assert mock_begin_norm.s.called

    mock_begin_norm.s.assert_called_once_with('test')
    mock_consume.si.assert_called_once_with('test', 'TIME', days_back=10)


@pytest.mark.usefixtures('consumer')
def test_consume_runs_consume(dispatch, consumer):
    tasks.consume('test', 'TIME')

    assert consumer.consume.called


@pytest.mark.usefixtures('consumer')
def test_consume_days_back(dispatch, consumer):
    _, timestamps = tasks.consume('test', 'TIME', days_back=10)

    keys = ['consumeFinished', 'consumeTaskCreated', 'consumeStarted']

    for key in keys:
        assert key in timestamps.keys()

    assert consumer.consume.called
    consumer.consume.assert_called_once_with(days_back=10)


@pytest.mark.usefixtures('consumer')
def test_consume_raises(dispatch, consumer):
    consumer.consume.side_effect = KeyError('testing')

    with pytest.raises(KeyError) as e:
        tasks.consume('test', 'TIME')

    assert e.value.message == 'testing'
    assert dispatch.called
    dispatch.assert_called_with(events.CONSUMER_RUN, events.FAILED,
            consumer='test', exception=str(e.value))
