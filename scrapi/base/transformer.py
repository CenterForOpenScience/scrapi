from __future__ import unicode_literals

import abc
import logging
from copy import deepcopy
from functools import partial

logger = logging.getLogger(__name__)


class BaseTransformer(object):

    __metaclass__ = abc.ABCMeta

    def __init__(self, schema):
        self.schema = deepcopy(schema)

    def transform(self, doc):
        self.process_schema()
        return self._transform(self.schema, doc)

    def _transform(self, schema, doc):
        ret = {}
        for key, transformation in schema.items():
            if isinstance(transformation, dict):
                ret[key] = self._transform(transformation, doc)
            else:
                ret[key] = transformation(doc)

        return ret

    @abc.abstractmethod
    def process_schema(self):
        pass  # Compute lambda functions for schema


class XMLTransformer(BaseTransformer):

    def __init__(self, name, schema, namespaces):
        super(XMLTransformer, self).__init__(schema)
        self.namespaces = namespaces
        self.NAME = name

        self._processed = False

    def process_schema(self):
        if not self._processed:
            self.schema = self._process_schema(self.schema)

    def _process_schema(self, schema):
        for key, value in schema.items():
            if isinstance(value, dict):
                schema[key] = self._process_schema(value)
            elif isinstance(value, list):
                schema[key] = partial(self._process_list, value)
            elif isinstance(value, basestring):
                schema[key] = partial(self._process_string, value)
        return schema

    def _process_string(self, string, doc):
        val = doc.xpath(string, namespaces=self.namespaces)
        return '' if not val else val[0] if len(val) == 1 else val

    def _process_list(self, l, doc):
        fns = []
        for value in l:
            if isinstance(value, basestring):
                fns.append(partial(self._process_string, value))
            elif callable(value):
                return (value(*[fn(doc) for fn in fns]))
