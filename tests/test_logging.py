import mock
import pytest

from scrapi import events
from scrapi import settings

settings.USE_FLUENTD = True


@pytest.fixture
def mock_event(monkeypatch):
    mock_event = mock.Mock()
    monkeypatch.setattr(events.event, 'Event', mock_event)
    return mock_event


@pytest.fixture
def mock_dispatch(monkeypatch):
    mock_dispatch = mock.Mock()
    monkeypatch.setattr(events, 'dispatch', mock_dispatch)
    return mock_dispatch


def test_dispatch(mock_event):
    events.dispatch('event', 'passed')
    mock_event.assert_called_once_with(
        'event', {'event': 'event', 'status': 'passed'})


def test_dispatch_index(mock_event):
    events.dispatch('event', 'passed', _index='foo')
    mock_event.assert_called_once_with(
        'event.foo', {'event': 'event', 'status': 'passed'})


def test_dispatch_kwargs(mock_event):
    events.dispatch('event', 'passed', ash='ketchem')
    mock_event.assert_called_once_with(
        'event', {'event': 'event', 'status': 'passed', 'ash': 'ketchem'})


def test_dispatch_index_kwargs(mock_event):
    events.dispatch('event', 'passed', _index='pallet', ash='ketchem')
    mock_event.assert_called_once_with(
        'event.pallet', {'event': 'event', 'status': 'passed', 'ash': 'ketchem'})


def test_logged_decorator(mock_dispatch):
    @events.logged('testing')
    def logged_func(test):
        return test

    logged_func('foo')
    assert mock_dispatch.call_count == 2
    mock_dispatch.assert_has_calls([
        mock.call('testing', events.STARTED, _index=None, test='foo'),
        mock.call('testing', events.COMPLETED, _index=None, test='foo')
    ])


def test_logged_decorator_exceptions(mock_dispatch):
    @events.logged('testing')
    def logged_func(test):
        raise ValueError('test')

    with pytest.raises(ValueError) as e:
        logged_func('foo')

    assert e.value.message == 'test'
    assert mock_dispatch.call_count == 2
    mock_dispatch.assert_has_calls([
        mock.call('testing', events.STARTED, _index=None, test='foo'),
        mock.call('testing', events.FAILED, _index=None, test='foo', exception=e.value)
    ])


def test_logged_decorator_skipped(mock_dispatch):
    exception = events.Skip('For Reasons')

    @events.logged('testing')
    def logged_func(test):
        raise exception

    assert logged_func('baz') is None
    mock_dispatch.assert_has_calls([
        mock.call('testing', events.STARTED, _index=None, test='baz'),
        mock.call('testing', events.SKIPPED, _index=None, test='baz', reason='For Reasons')
    ])


def test_logged_decorator_stargs(mock_dispatch):
    @events.logged('testing')
    def logged_func(test, *args):
        return 'share'

    assert logged_func('baz', 1, 2, 3) == 'share'
    mock_dispatch.assert_has_calls([
        mock.call('testing', events.STARTED, _index=None, test='baz', args=[1, 2, 3]),
        mock.call('testing', events.COMPLETED, _index=None, test='baz', args=[1, 2, 3])
    ])


def test_logged_decorator_kwargs(mock_dispatch):
    @events.logged('testing')
    def logged_func(test, pika='chu', **kwargs):
        return 'share'

    assert logged_func('baz', tota='dile') == 'share'
    mock_dispatch.assert_has_calls([
        mock.call('testing', events.STARTED, _index=None, test='baz', pika='chu', kwargs={'tota': 'dile'}),
        mock.call('testing', events.COMPLETED, _index=None, test='baz', pika='chu', kwargs={'tota': 'dile'}),
    ])

settings.USE_FLUENTD = False
