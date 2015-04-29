from __future__ import unicode_literals

import abc
import logging

from jsonpointer import resolve_pointer, JsonPointerException

logger = logging.getLogger(__name__)


class BaseTransformer(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def _transform_string(self, string, doc):
        raise NotImplementedError

    @abc.abstractproperty
    def schema(self):
        raise NotImplementedError

    def transform(self, doc):
        return self._transform(self.schema, doc)

    def _transform(self, schema, doc):
        transformed = {}
        for key, value in schema.items():
            transformed[key] = self._transform_value(value, doc)
        return transformed

    def _transform_value(self, value, doc):
        if isinstance(value, dict):
            return self._transform(value, doc)
        elif isinstance(value, list):
            return self._transform_list(value, doc)
        elif isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], tuple):
            return self._transform_args_kwargs(value, doc)
        elif isinstance(value, tuple):
            return self._transform_tuple(value, doc)
        elif isinstance(value, basestring):
            return self._transform_string(value, doc)
        elif callable(value):
            return value(doc)

    def _transform_list(self, l, doc):
        return [
            self._transform_value(item, doc) for item in l
        ]

    def _transform_tuple(self, l, doc):

        fn, values = l[-1], l[:-1]
        args = []

        for value in values:
            if isinstance(value, basestring):
                args.append(self._transform_string(value, doc))
            elif callable(value):
                args.append(value(doc))
        return fn(*args)

    def _transform_args_kwargs(self, l, doc):
        fn = l[1]
        return fn(
            *self._transform_args(l[0], doc),
            **self._transform_kwargs(l[0], doc)
        )

    def _transform_args(self, t, doc):
        return [self._transform_string(arg, doc) for arg in t[0]]

    def _transform_kwargs(self, t, doc):
        return {
            k: self._transform_string(v, doc) for k, v in t[1].items()
        } if len(t) == 2 else {}


class XMLTransformer(BaseTransformer):

    __metaclass__ = abc.ABCMeta

    def _transform_string(self, string, doc):
        return doc.xpath(string, namespaces=self.namespaces)

    @abc.abstractproperty
    def namespaces(self):
        raise NotImplementedError


class JSONTransformer(BaseTransformer):

    __metaclass__ = abc.ABCMeta

    def _transform_string(self, val, doc):
        try:
            return resolve_pointer(doc, val)
        except JsonPointerException:
            return ''
