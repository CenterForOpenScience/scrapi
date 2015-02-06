import mock
import pytest

from scrapi import tasks
from scrapi import events
from scrapi.linter import RawDocument


BLACKHOLE = lambda *_, **__: None


@pytest.fixture
def dispatch(monkeypatch):
    event_mock = mock.MagicMock()
    monkeypatch.setattr('scrapi.events.dispatch', event_mock)
    return event_mock


@pytest.fixture
def raw_doc():
    return RawDocument({
        'doc': 'bar',
        'docID': u'foo',
        'source': u'test',
        'filetype': u'xml',
    })


@pytest.fixture
def raw_docs():
    return [
        RawDocument({
            'doc': str(x),
            'docID': unicode(x),
            'source': u'test',
            'filetype': u'xml',
        })
        for x in xrange(11)
    ]


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
    dispatch.assert_called_with(
        events.CONSUMER_RUN,
        events.FAILED,
        days_back=1,
        consumer='test',
        job_created='TIME',
        exception=repr(e.value),
    )


def test_begin_normalize_starts(raw_docs, monkeypatch, dispatch):
    mock_norm = mock.MagicMock()
    mock_praw = mock.MagicMock()
    mock_pnorm = mock.MagicMock()

    monkeypatch.setattr('scrapi.tasks.normalize', mock_norm)
    monkeypatch.setattr('scrapi.tasks.process_raw', mock_praw)
    monkeypatch.setattr('scrapi.tasks.process_normalized', mock_pnorm)

    timestamps = {}
    tasks.begin_normalization((raw_docs, timestamps), 'test')

    assert dispatch.call_count == 22
    assert mock_norm.si.call_count == 11
    assert mock_pnorm.s.call_count == 11
    assert mock_praw.delay.call_count == 11

    for x in raw_docs:
        mock_pnorm.s.assert_any_call(x)
        mock_praw.delay.assert_any_call(x)
        mock_norm.si.assert_any_call(x, 'test')


def test_begin_normalize_logging(raw_docs, monkeypatch, dispatch):
    monkeypatch.setattr('scrapi.tasks.normalize.si', mock.MagicMock())
    monkeypatch.setattr('scrapi.tasks.process_raw.delay', BLACKHOLE)
    monkeypatch.setattr('scrapi.tasks.process_normalized.s', BLACKHOLE)

    timestamps = {}

    tasks.begin_normalization((raw_docs, timestamps), 'test')

    assert dispatch.call_count == 22

    for x in raw_docs:
        dispatch.assert_any_call(events.NORMALIZATION,
                events.CREATED, consumer='test', **x.attributes)
        dispatch.assert_any_call(events.PROCESSING,
                events.CREATED, consumer='test', **x.attributes)


def test_process_raw_calls(raw_doc, monkeypatch):
    pmock = mock.Mock()

    monkeypatch.setattr('scrapi.tasks.processing.process_raw', pmock)

    tasks.process_raw(raw_doc)

    pmock.assert_called_once_with(raw_doc, {})


def test_process_raw_logging(raw_doc, dispatch, monkeypatch):
    monkeypatch.setattr('scrapi.tasks.processing.process_raw', BLACKHOLE)

    tasks.process_raw(raw_doc)

    calls = [
        mock.call(events.PROCESSING, events.STARTED, _index='raw', **raw_doc.attributes),
        mock.call(events.PROCESSING, events.COMPLETED, _index='raw', **raw_doc.attributes)
    ]

    dispatch.assert_has_calls(calls)


def test_process_norm_calls(raw_doc, monkeypatch):
    pmock = mock.Mock()

    monkeypatch.setattr('scrapi.tasks.processing.process_normalized', pmock)

    tasks.process_normalized(raw_doc, raw_doc)

    pmock.assert_called_once_with(raw_doc, raw_doc, {})


def test_process_norm_logging(raw_doc, dispatch, monkeypatch):
    monkeypatch.setattr('scrapi.tasks.processing.process_normalized', BLACKHOLE)

    tasks.process_normalized(raw_doc, raw_doc)

    calls = [
        mock.call(events.PROCESSING, events.STARTED, _index='normalized', **raw_doc.attributes),
        mock.call(events.PROCESSING, events.COMPLETED, _index='normalized', **raw_doc.attributes)
    ]

    dispatch.assert_has_calls(calls)
