from __future__ import unicode_literals

import abc
import copy
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
            if isinstance(value, dict):
                transformed[key] = self._transform(value, doc)
            elif isinstance(value, list):
                transformed[key] = self._transform_list(value, doc)
            elif isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], tuple):
                transformed[key] = self._transform_args_kwargs(value, doc)
            elif isinstance(value, tuple):
                transformed[key] = self._transform_tuple(value, doc)
            elif isinstance(value, basestring):
                transformed[key] = self._transform_string(value, doc)
            elif callable(value):
                transformed[key] = value(doc)
        return transformed

    def _transform_list(self, l, doc):
        l = copy.deepcopy(l)
        for item in l:
            item.update({
                'properties': self._transform(item['properties'], doc)
            })
        return l

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
        val = doc.xpath(string, namespaces=self.namespaces)
        return val

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
