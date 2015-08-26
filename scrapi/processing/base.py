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
