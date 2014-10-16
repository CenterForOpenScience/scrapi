import mock
import pytest

from scrapi import settings
settings.DEBUG = True
settings.CELERY_ALWAYS_EAGER = True
settings.CELERY_EAGER_PROPAGATES_EXCEPTIONS = True


@pytest.fixture(autouse=True)
def consumer(monkeypatch):
    consumer_mock = mock.MagicMock()
    monkeypatch.setattr('scrapi.tasks.import_consumer', consumer_mock)
    monkeypatch.setattr('scrapi.util.import_consumer', consumer_mock)
    return consumer_mock
