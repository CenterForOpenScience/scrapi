import mock
import pytest

from scrapi import requests


@pytest.fixture(autouse=True)
def mock_requests(monkeypatch):
    mock_req = mock.Mock()
    monkeypatch.setattr(requests, 'requests', mock_req)
    return mock_req


@pytest.fixture(autouse=True)
def mock_record_transactions(monkeypatch):
    monkeypatch.setattr(requests.settings, 'RECORD_HTTP_TRANSACTIONS', True)


def test_record_or_load_response_respects_record_false(mock_requests, monkeypatch):
    mock_rec_or_load = mock.Mock()
    monkeypatch.setattr(requests.settings, 'RECORD_HTTP_TRANSACTIONS', False)
    monkeypatch.setattr(requests, 'record_or_load_response', mock_rec_or_load)

    requests.get('foo')

    assert not mock_rec_or_load.called
    assert mock_requests.request.called_once_with('get', 'foo')


def test_record_or_load_response_respects_record_true(mock_requests, monkeypatch):
    mock_rec_or_load = mock.Mock()
    monkeypatch.setattr(requests, 'record_or_load_response', mock_rec_or_load)

    requests.get('foo')

    assert mock_rec_or_load.called_once_with('get', 'foo')
    assert mock_requests.request.called_once_with('get', 'foo')


@pytest.mark.cassandra
def test_record_or_load_loads(mock_requests, monkeypatch):
    requests.HarvesterResponse(method='get', url='dinosaurs.sexy', content='rawr').save()

    resp = requests.get('dinosaurs.sexy')

    assert resp.content == 'rawr'
    assert not mock_requests.request.called
    assert isinstance(resp, requests.HarvesterResponse)


@pytest.mark.cassandra
def test_record_or_load_records(mock_requests, monkeypatch):
    mock_requests.request.return_value = mock.Mock(encoding='utf-8', content='rawr', status_code=200, headers={'tota': 'dyle'})

    resp = requests.get('dinosaurs.sexy')

    model = requests.HarvesterResponse.get(method='get', url='dinosaurs.sexy')

    assert model.method == 'get'
    assert model.content == 'rawr'
    assert model.encoding == 'utf-8'
    assert model.status_code == 200
    assert model.url == 'dinosaurs.sexy'
    assert model.headers == {'tota': 'dyle'}
    assert model.headers_str == '{"tota": "dyle"}'
    assert isinstance(resp, requests.HarvesterResponse)


def test_get_calls_get(monkeypatch):
    mock_request = mock.Mock()
    monkeypatch.setattr(requests, 'record_or_load_response', mock_request)

    requests.get('test', tota='dyle')

    assert mock_request.called_once_with('get', 'test', tota='dyle')


def test_put_calls_put(monkeypatch):
    mock_request = mock.Mock()
    monkeypatch.setattr(requests, 'record_or_load_response', mock_request)

    requests.put('test', tota='dyle')

    assert mock_request.called_once_with('put', 'test', tota='dyle')


def test_post_calls_post(monkeypatch):
    mock_request = mock.Mock()
    monkeypatch.setattr(requests, 'record_or_load_response', mock_request)

    requests.post('test', tota='dyle')

    assert mock_request.called_once_with('post', 'test', tota='dyle')


def test_delete_calls_delete(monkeypatch):
    mock_request = mock.Mock()
    monkeypatch.setattr(requests, 'record_or_load_response', mock_request)

    requests.delete('test', tota='dyle')

    assert mock_request.called_once_with('delete', 'test', tota='dyle')
