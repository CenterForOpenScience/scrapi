import mock

from scrapi import database
from scrapi import settings


class TestDatabaseManager(object):
    def test_setup_calls_setup(self, monkeypatch):
        mock_connect = mock.Mock()
        mock_create_keyspace = mock.Mock()
        monkeypatch.setattr(database.connection, 'setup', mock_connect)
        monkeypatch.setattr(database.management, 'create_keyspace', mock_create_keyspace)

        manager = database.DatabaseManager()
        manager.setup()

        assert mock_connect.called_once_with(manager.uri, manager.keyspace)
        assert mock_create_keyspace.called_once_with(manager.keyspace, replication_factor=1, strategy_class='SimpleStrategy')

    def test_setup_runs_once(self, monkeypatch):
        mock_connect = mock.Mock()
        mock_create_keyspace = mock.Mock()
        monkeypatch.setattr(database.connection, 'setup', mock_connect)
        monkeypatch.setattr(database.management, 'create_keyspace', mock_create_keyspace)

        manager = database.DatabaseManager()
        manager.setup()
        manager.setup()
        manager.setup()

        assert mock_connect.called_once_with(manager.uri, manager.keyspace)
        assert mock_create_keyspace.called_once_with(manager.keyspace, replication_factor=1, strategy_class='SimpleStrategy')

    def test_pulls_from_settings(self, monkeypatch):
        manager = database.DatabaseManager()

        assert manager.uri == settings.CASSANDRA_URI
        assert manager.keyspace == settings.CASSANDRA_KEYSPACE

    def test_respects_kwargs(self, monkeypatch):
        manager = database.DatabaseManager(uri='none', keyspace='test')

        assert manager.uri == 'none'
        assert manager.keyspace == 'test'

    def test_register_model(self, monkeypatch):
        mock_sync = mock.Mock()
        monkeypatch.setattr(database.management, 'sync_table', mock_sync)

        manager = database.DatabaseManager()
        manager._setup = True

        mock_model = mock.Mock()
        assert mock_model == manager.register_model(mock_model)

        assert manager._models == [mock_model]
        assert mock_sync.called_once_with(mock_model)
