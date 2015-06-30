import abc
import json
from requests.structures import CaseInsensitiveDict


class BaseProcessor(object):
    NAME = None

    def process_raw(self, raw_doc, **kwargs):
        pass  # pragma: no cover

    def process_normalized(self, raw_doc, normalized, **kwargs):
        pass  # pragma: no cover


class HarvesterResponseModel(object):

    class DoesNotExist(Exception):
        pass  # pragma: no cover

    def json(self):
        return json.loads(self.content)

    @property
    def headers(self):
        return CaseInsensitiveDict(json.loads(self.headers_str))

    @property
    def text(self):
        return self.content.decode('utf-8')

    @classmethod
    @abc.abstractmethod
    def get(self, url=None, method=None):
        raise NotImplementedError
