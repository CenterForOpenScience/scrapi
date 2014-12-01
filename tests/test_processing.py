import mock
import pytest

from scrapi import events
from scrapi import settings

settings.DEBUG = False
settings.RAW_PROCESSING = ['storage', 'osf', 'foo', 'bar']
settings.NORMALIZED_PROCESSING = ['storage', 'osf', 'foo', 'bar']

from scrapi import processing

BLACKHOLE = lambda *_, **__: None


@pytest.fixture(autouse=True)
def no_events(monkeypatch):
    monkeypatch.setattr('scrapi.processing.events.dispatch', BLACKHOLE)


@pytest.fixture
def get_processor(monkeypatch):
    mock_get_proc = mock.MagicMock()
    monkeypatch.setattr('scrapi.processing.get_processor', mock_get_proc)
    return mock_get_proc


def test_normalized_calls_all(get_processor):
    processing.process_normalized(mock.MagicMock(), mock.MagicMock(), {})

    for processor in settings.NORMALIZED_PROCESSING:
        get_processor.assert_any_call(processor)


def test_raw_calls_all(get_processor):
    processing.process_raw(mock.MagicMock(), {})

    for processor in settings.RAW_PROCESSING:
        get_processor.assert_any_call(processor)


def test_normalized_catches(monkeypatch, get_processor):
    settings.NORMALIZED_PROCESSING = ['osf']
    mock_event = mock.Mock()
    raw_mock = mock.MagicMock()
    get_processor.side_effect = KeyError('You raise me uuuuup')

    monkeypatch.setattr('scrapi.processing._normalized_event', mock_event)

    processing.process_normalized(raw_mock, raw_mock, {})

    mock_event.assert_called_with(
        events.FAILED, 'osf', raw_mock, exception=repr(get_processor.side_effect))


def test_raw_catches(monkeypatch, get_processor):
    mock_event = mock.Mock()
    raw_mock = mock.MagicMock()

    get_processor.side_effect = KeyError('You raise me uuuuup')

    monkeypatch.setattr('scrapi.processing._raw_event', mock_event)

    processing.process_raw(raw_mock, {})

    mock_event.assert_any_call(
        events.FAILED, 'osf', raw_mock, exception=repr(get_processor.side_effect))


def test_normalized_calls_all_throwing(get_processor):
    get_processor.side_effect = lambda x: Exception(
        'Reasons') if x == 'storage' else mock.Mock()

    processing.process_normalized(mock.MagicMock(), mock.MagicMock(), {})

    for processor in settings.NORMALIZED_PROCESSING:
        get_processor.assert_any_call(processor)


def test_normalized_calls_all_throwing(get_processor):
    get_processor.side_effect = lambda x: Exception(
        'Reasons') if x == 'storage' else mock.Mock()

    processing.process_raw(mock.MagicMock(), {})

    for processor in settings.RAW_PROCESSING:
        get_processor.assert_any_call(processor)


def test_raises_on_bad_processor():
    with pytest.raises(NotImplementedError):
        processing.get_processor("Baby, You're never there.")
