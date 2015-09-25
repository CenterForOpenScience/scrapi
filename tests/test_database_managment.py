import mock

from scrapi import settings
from scrapi.processing import cassandra
from scrapi.processing.cassandra import DatabaseManager


class TestCassandraDatabaseManager(object):
    def test_setup_calls_setup(self, monkeypatch):
        mock_connect = mock.Mock()
        mock_sync_table = mock.Mock()
        mock_create_keyspace = mock.Mock()
        monkeypatch.setattr(cassandra.connection, 'setup', mock_connect)
        monkeypatch.setattr(cassandra.management, 'sync_table', mock_sync_table)
        monkeypatch.setattr(cassandra.management, 'create_keyspace', mock_create_keyspace)

        manager = DatabaseManager(keyspace='test')
        manager.setup()

        assert mock_connect.called_once_with(manager.uri, manager.keyspace)
        assert mock_create_keyspace.called_once_with(manager.keyspace, replication_factor=1, strategy_class='SimpleStrategy')

    def test_setup_runs_once(self, monkeypatch):
        mock_sync_table = mock.Mock()
        mock_connect = mock.Mock()
        mock_create_keyspace = mock.Mock()
        monkeypatch.setattr(cassandra.connection, 'setup', mock_connect)
        monkeypatch.setattr(cassandra.management, 'sync_table', mock_sync_table)
        monkeypatch.setattr(cassandra.management, 'create_keyspace', mock_create_keyspace)

        manager = DatabaseManager(keyspace='test')
        manager.setup()
        manager.setup()
        manager.setup()

        assert mock_connect.called_once_with(manager.uri, manager.keyspace)
        assert mock_create_keyspace.called_once_with(manager.keyspace, replication_factor=1, strategy_class='SimpleStrategy')

    def test_pulls_from_settings(self, monkeypatch):
        manager = DatabaseManager()

        assert manager.uri == settings.CASSANDRA_URI
        assert manager.keyspace == settings.CASSANDRA_KEYSPACE

    def test_respects_kwargs(self, monkeypatch):
        manager = DatabaseManager(uri='none', keyspace='test')

        assert manager.uri == 'none'
        assert manager.keyspace == 'test'

    def test_register_model(self, monkeypatch):
        mock_sync = mock.Mock()
        monkeypatch.setattr(cassandra.management, 'sync_table', mock_sync)

        manager = DatabaseManager()
        manager._setup = True
        manager._models = set()

        mock_model = mock.Mock()
        assert mock_model == manager.registered_model(mock_model)
        assert mock_model == manager.register_model(mock_model)

        assert manager._models == {mock_model}
        assert mock_sync.called_once_with(mock_model)
