from __future__ import unicode_literals

import abc
import logging
from copy import deepcopy

logger = logging.getLogger(__name__)


class BaseTransformer(object):

    __metaclass__ = abc.ABCMeta

    def transform(self, doc):
        return self._transform(self.schema, doc)

    def _transform(self, schema, doc):
        transformed = {}
        for key, value in schema.items():
            if isinstance(value, dict):
                transformed[key] = self._transform(value, doc)
            elif isinstance(value, list) or isinstance(value, tuple):
                transformed[key] = self._transform_iter(value, doc)
            elif isinstance(value, basestring):
                transformed[key] = self._transform_string(value, doc)
        return transformed

    def _transform_iter(self, l, doc):
        docs = []
        for value in l:
            if isinstance(value, basestring):
                docs.append(self._transform_string(value, doc))
            elif callable(value):
                return value(*[res for res in docs])

    @abc.abstractmethod
    def _transform_string(self, string, doc):
        raise NotImplementedError

    @abc.abstractproperty
    def name(self):
        raise NotImplementedError

    @abc.abstractproperty
    def schema(self):
        raise NotImplementedError


class XMLTransformer(BaseTransformer):

    __metaclass__ = abc.ABCMeta

    def _transform_string(self, string, doc):
        val = doc.xpath(string, namespaces=self.namespaces)
        return '' if not val else val[0] if len(val) == 1 else val

    @abc.abstractproperty
    def namespaces(self):
        raise NotImplementedError
