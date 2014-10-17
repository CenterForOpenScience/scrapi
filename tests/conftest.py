import mock
import pytest

from scrapi import settings
settings.DEBUG = True
settings.CELERY_ALWAYS_EAGER = True
settings.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True


@pytest.fixture(autouse=True)
def consumer(monkeypatch):
    import_mock = mock.MagicMock()
    consumer_mock = mock.MagicMock()

    import_mock.return_value = consumer_mock

    monkeypatch.setattr('scrapi.tasks.import_consumer', import_mock)
    monkeypatch.setattr('scrapi.util.import_consumer', import_mock)

    return consumer_mock


@pytest.fixture(autouse=True)
def timestamp_patch(monkeypatch):
    monkeypatch.setattr('scrapi.util.timestamp', lambda: 'TIME')
    monkeypatch.setattr('scrapi.tasks.timestamp', lambda: 'TIME')
