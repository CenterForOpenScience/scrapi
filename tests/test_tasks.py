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


def test_run_harvester_calls(monkeypatch, dispatch):
    mock_harvest = mock.MagicMock()
    mock_begin_norm = mock.MagicMock()

    monkeypatch.setattr('scrapi.tasks.harvest', mock_harvest)
    monkeypatch.setattr('scrapi.tasks.begin_normalization', mock_begin_norm)

    tasks.run_harvester('test')

    assert dispatch.called
    assert mock_harvest.si.called
    assert mock_begin_norm.s.called

    mock_begin_norm.s.assert_called_once_with('test')
    mock_harvest.si.assert_called_once_with('test', 'TIME', days_back=1)


def test_run_harvester_daysback(monkeypatch, dispatch):
    mock_harvest = mock.MagicMock()
    mock_begin_norm = mock.MagicMock()

    monkeypatch.setattr('scrapi.tasks.harvest', mock_harvest)
    monkeypatch.setattr('scrapi.tasks.begin_normalization', mock_begin_norm)

    tasks.run_harvester('test', days_back=10)

    assert dispatch.called
    assert mock_harvest.si.called
    assert mock_begin_norm.s.called

    mock_begin_norm.s.assert_called_once_with('test')
    mock_harvest.si.assert_called_once_with('test', 'TIME', days_back=10)


@pytest.mark.usefixtures('harvester')
def test_harvest_runs_harvest(dispatch, harvester):
    tasks.harvest('test', 'TIME')

    assert harvester.harvest.called


@pytest.mark.usefixtures('harvester')
def test_harvest_days_back(dispatch, harvester):
    _, timestamps = tasks.harvest('test', 'TIME', days_back=10)

    keys = ['harvestFinished', 'harvestTaskCreated', 'harvestStarted']

    for key in keys:
        assert key in timestamps.keys()

    assert harvester.harvest.called
    harvester.harvest.assert_called_once_with(days_back=10)


@pytest.mark.usefixtures('harvester')
def test_harvest_raises(dispatch, harvester):
    harvester.harvest.side_effect = KeyError('testing')

    with pytest.raises(KeyError) as e:
        tasks.harvest('test', 'TIME')

    assert e.value.message == 'testing'
    assert dispatch.called
    dispatch.assert_called_with(
        events.HARVESTER_RUN,
        events.FAILED,
        days_back=1,
        harvester='test',
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
                events.CREATED, harvester='test', **x.attributes)
        dispatch.assert_any_call(events.PROCESSING,
                events.CREATED, harvester='test', **x.attributes)


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
