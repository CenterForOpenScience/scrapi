from __future__ import unicode_literals

import abc
import logging

logger = logging.getLogger(__name__)


class BaseTransformer(object):

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def _transform_string(self, string, doc):
        raise NotImplementedError

    @abc.abstractproperty
    def short_name(self):
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
            elif isinstance(value, list) or isinstance(value, tuple):
                transformed[key] = self._transform_iterable(value, doc)
            elif isinstance(value, basestring):
                transformed[key] = self._transform_string(value, doc)
            elif callable(value):
                transformed[key] = value(doc)
        return transformed

    def _transform_iterable(self, l, doc):
        docs = []

        if isinstance(l[0], tuple) and len(l) == 2:
            return self._transform_args_kwargs(l, doc)

        fn = l[-1]

        for value in l[:-1]:
            if isinstance(value, basestring):
                docs.append(self._transform_string(value, doc))
            elif callable(value):
                docs.append(value(doc))
        return fn(*[res for res in docs])

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
        return '' if not val else unicode(val[0]) if len(val) == 1 else [unicode(v) for v in val]

    @abc.abstractproperty
    def namespaces(self):
        raise NotImplementedError


class JSONTransformer(BaseTransformer):

    __metaclass__ = abc.ABCMeta

    def _transform_string(self, val, doc):
        return doc.get(val, '')

    def _process_nested(self, strings, d):
        try:
            if len(strings) == 1:
                return d[strings[0]]
            elif len(strings) > 1:
                return self._process_nested(strings[1:], d[strings[0]])
            else:
                return ''
        except KeyError:
            return ''

    def nested(self, *args):
        return lambda doc: self._process_nested(args, doc)
