from __future__ import unicode_literals

import abc
import logging

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

        if isinstance(l[0], tuple) and len(l) == 2:
            return self._transform_args_kwargs(l, doc)

        for value in l:
            if isinstance(value, basestring):
                docs.append(self._transform_string(value, doc))
            elif callable(value):
                return value(*[res for res in docs])

    def _transform_args_kwargs(self, l, doc):
        fn = l[1]
        return fn(
            *self._transform_args(l[0], doc),
            **self._transform_kwargs(l[0], doc)
        )

    def _transform_args(self, t, doc):
        if not isinstance(t[0], list) and not isinstance(t[0], tuple):
            return []

        return [self._transform_string(arg, doc) for arg in t[0]]

    def _transform_kwargs(self, t, doc):
        kwargs = t[0] if isinstance(t[0], dict) else t[1] if len(t) == 2 else {}

        return {
            k: self._transform_string(v, doc) for k, v in kwargs.items()
        }

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
        return '' if not val else unicode(val[0]) if len(val) == 1 else [unicode(v) for v in val]

    @abc.abstractproperty
    def namespaces(self):
        raise NotImplementedError
