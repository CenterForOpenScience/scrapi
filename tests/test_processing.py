import mock
import pytest

from scrapi import settings

# settings.DEBUG = False
# settings.RAW_PROCESSING = ['storage', 'osf', 'foo', 'bar']
# settings.NORMALIZED_PROCESSING = ['storage', 'osf', 'foo', 'bar']

from scrapi import processing

BLACKHOLE = lambda *_, **__: None


def patch_processing():
    settings.DEBUG = False
    settings.RAW_PROCESSING = ['storage', 'osf', 'foo', 'bar']
    settings.NORMALIZED_PROCESSING = ['storage', 'osf', 'foo', 'bar']


def unpatch_processing():
    settings.DEBUG = True
    settings.RAW_PROCESSING = ['cassandra']
    settings.NORMALIZED_PROCESSING = ['cassandra', 'elasticsearch']


@pytest.fixture
def get_processor(monkeypatch):
    mock_get_proc = mock.MagicMock()
    monkeypatch.setattr('scrapi.processing.get_processor', mock_get_proc)
    return mock_get_proc


def raise_exception(x):
    if x == 'storage':
        raise Exception('Reasons')
    return mock.Mock()


def test_normalized_calls_all(get_processor):
    processing.process_normalized(mock.MagicMock(), mock.MagicMock(), {})

    for processor in settings.NORMALIZED_PROCESSING:
        get_processor.assert_any_call(processor)


def test_raw_calls_all(get_processor):
    processing.process_raw(mock.MagicMock(), {})

    for processor in settings.RAW_PROCESSING:
        get_processor.assert_any_call(processor)


def test_normalized_calls_all_throwing(get_processor):
    patch_processing()
    get_processor.side_effect = raise_exception

    with pytest.raises(Exception):
        processing.process_normalized(mock.MagicMock(), mock.MagicMock(), {})
    unpatch_processing()


def test_raw_calls_all_throwing(get_processor):
    patch_processing()
    get_processor.side_effect = raise_exception

    with pytest.raises(Exception):
        processing.process_raw(mock.MagicMock(), {})
    unpatch_processing()


def test_raises_on_bad_processor():
    with pytest.raises(NotImplementedError):
        processing.get_processor("Baby, You're never there.")
