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
            return self._transform_arg_kwargs(l, doc)

        for value in l:
            if isinstance(value, basestring):
                docs.append(self._transform_string(value, doc))
            elif callable(value):
                return value(*[res for res in docs])

    def _transform_arg_kwargs(self, l, doc):
        if len(l[0]) == 1:
            if isinstance(l[0][0], dict):
                kwargs = l[0][0]
                args = []
            elif isinstance(l[0][0], tuple) or isinstance(l[0][0], list):
                args = l[0][0]
                kwargs = {}
            else:
                raise ValueError("((args, kwargs), callable) pattern not matched, {} does not define args or kwargs correctly".format(l))
        else:
            args = l[0][0]
            kwargs = l[0][1]
        fn = l[1]
        return fn(
            *[self._transform_string(arg, doc) for arg in args],
            **{key: self._transform_string(value, doc) for key, value in kwargs.items()}
        )

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
