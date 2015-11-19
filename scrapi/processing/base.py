import six
import json
from abc import abstractproperty, abstractmethod

from requests.structures import CaseInsensitiveDict


class BaseProcessor(object):
    NAME = None

    def process_raw(self, raw_doc, **kwargs):
        pass  # pragma: no cover

    def process_normalized(self, raw_doc, normalized, **kwargs):
        pass  # pragma: no cover

    @abstractmethod
    def documents(self, *sources):
        '''
        an iterator that will return documents
        '''
        raise NotImplementedError

    @abstractmethod
    def get_versions(self, source, docID):
        raise NotImplementedError

    def different(self, old, new):
        try:
            return not all([new[key] == old[key] or (not new[key] and not old[key]) for key in new.keys() if key != 'timestamps'])
        except Exception:
            return True  # If the document fails to load/compare for some reason, accept a new version


class BaseDatabaseManager(object):
    '''A base class for database managers in the scrapi processing module

        Must handle setup, teardown, and multi-process initialization of database connections
        All errors should be logged, but not thrown
    '''

    @abstractmethod
    def setup(self):
        '''Sets up the database connection. Returns True if the database connection
            is successful, False otherwise
        '''
        raise NotImplementedError

    @abstractmethod
    def tear_down(self):
        '''Tears down the database connection.
        '''
        raise NotImplementedError

    @abstractmethod
    def clear(self, force=False):
        '''Deletes everything in a table/keyspace etc
            Should fail if called on the production database
            for testing purposes only
        '''
        raise NotImplementedError

    @abstractmethod
    def celery_setup(self, *args, **kwargs):
        '''Performs the necessary operations to allow a new process to connect to the database
        '''
        raise NotImplementedError


class BaseHarvesterResponse(object):
    """A parody of requests.response but stored in a database for caching
    Should reflect all methods of a response object
    Contains an additional field time_made, self-explanatory
    """

    class DoesNotExist(Exception):
        pass

    @abstractproperty
    def method(self):
        raise NotImplementedError

    @abstractproperty
    def url(self):
        raise NotImplementedError

    @abstractproperty
    def ok(self):
        raise NotImplementedError

    @abstractproperty
    def content(self):
        raise NotImplementedError

    @abstractproperty
    def encoding(self):
        raise NotImplementedError

    @abstractproperty
    def headers_str(self):
        raise NotImplementedError

    @abstractproperty
    def status_code(self):
        raise NotImplementedError

    @abstractproperty
    def time_made(self):
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def get(self, url=None, method=None):
        raise NotImplementedError

    @abstractmethod
    def save(self):
        raise NotImplementedError

    @abstractmethod
    def update(self, **kwargs):
        raise NotImplementedError

    def json(self):
        try:
            content = self.content.decode('utf-8')
        except AttributeError:  # python 3eeeee!
            content = self.content
        return json.loads(content)

    @property
    def headers(self):
        return CaseInsensitiveDict(json.loads(self.headers_str))

    @property
    def text(self):
        return six.u(self.content)
