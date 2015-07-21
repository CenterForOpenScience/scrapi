from __future__ import unicode_literals

import six
import abc
import logging

from jsonpointer import resolve_pointer, JsonPointerException

logger = logging.getLogger(__name__)


@six.add_metaclass(abc.ABCMeta)
class BaseTransformer(object):

    @abc.abstractmethod
    def _transform_string(self, string, doc):
        raise NotImplementedError

    @abc.abstractproperty
    def schema(self):
        raise NotImplementedError

    def transform(self, doc, fail=False):
        return self._transform_dict(self.schema, doc, fail=fail)

    def _transform_dict(self, d, doc, fail=False):
        return {
            key: self._maybe_transform_value(value, doc, fail=fail)
            for key, value in d.items()
        }

    def _transform_list(self, l, doc, fail=False):
        return [
            self._maybe_transform_value(item, doc, fail=fail)
            for item in l
        ]

    def _maybe_transform_value(self, value, doc, fail=False):
        try:
            return self._transform_value(value, doc, fail=fail)
        except Exception as e:
            if fail:
                raise
            logger.exception(e)
            return None

    def _transform_value(self, value, doc, fail=False):
        if isinstance(value, dict):
            return self._transform_dict(value, doc, fail=fail)
        elif isinstance(value, list):
            return self._transform_list(value, doc, fail=fail)
        elif isinstance(value, tuple) and len(value) == 2 and isinstance(value[0], tuple):
            return self._transform_args_kwargs(value, doc)
        elif isinstance(value, tuple):
            return self._transform_tuple(value, doc)
        elif isinstance(value, six.string_types):
            return self._transform_string(value, doc)
        elif callable(value):
            return value(doc)

    def _transform_tuple(self, l, doc):

        fn, values = l[-1], l[:-1]
        args = []

        for value in values:
            if isinstance(value, six.string_types):
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


@six.add_metaclass(abc.ABCMeta)
class XMLTransformer(BaseTransformer):

    namespaces = {}

    def _transform_string(self, string, doc):
        return doc.xpath(string, namespaces=self.namespaces)


@six.add_metaclass(abc.ABCMeta)
class JSONTransformer(BaseTransformer):

    def _transform_string(self, val, doc):
        try:
            return resolve_pointer(doc, val)
        except JsonPointerException as e:
            # This is because of jsonpointer's exception structure
            if 'not found in' in e.args[0] or 'is not a valid list index' in e.args[0]:
                return None
            raise e
