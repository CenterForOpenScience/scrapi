import json

import jsonschema

from scrapi import registry
from scrapi.util import json_without_bytes


def strip_empty(schema, required):
    ''' Removes empty fields from the processed schema
    '''
    for k, v in schema.items():
        if k not in required:
            schema[k] = do_strip_empty(v)
            if not schema[k]:
                del schema[k]
    return schema


def do_strip_empty(value):
    ''' Filters empty values from container types
    '''
    return {
        dict: lambda d: dict([(k, do_strip_empty(v)) for k, v in d.items() if v]),
        list: lambda l: [do_strip_empty(v) for v in l if v],
        tuple: lambda l: [do_strip_empty(v) for v in l if v]
    }.get(type(value), lambda x: x or None)(value)


class BaseDocument(object):

    """
        For file objects. Automatically validates input to ensure
        compatibility with scrAPI.
    """

    schema = {}

    def __init__(self, attributes):
        # validate a version of the attributes that are safe to check
        # against the JSON schema
        attributes = json_without_bytes(strip_empty(attributes, self.schema.get('required', [])))
        jsonschema.validate(attributes, self.schema,
                            format_checker=jsonschema.FormatChecker())

        self.attributes = attributes

    def get(self, attribute, default=None):
        """
            Maintains compatibility with previous dictionary implementation of scrAPI
            :: str -> str
        """
        return self.attributes.get(attribute, default)

    def __getitem__(self, attr):
        return self.attributes[attr]

    def __setitem__(self, attr, val):
        self.attributes[attr] = val

    def __delitem__(self, attr):
        del self.attributes[attr]


class RawDocument(BaseDocument):

    @property
    def schema(self):
        return {
            '$schema': 'http://json-schema.org/draft-04/schema#',
            'type': 'object',
            'properties': {
                'doc': {
                    'type': 'string',
                    'description': 'The raw metadata'
                },
                'docID': {
                    'type': 'string',
                    'description': 'A service-unique identifier'
                },
                'source': {
                    'type': 'string',
                    'enum': [entry.short_name for entry in registry.values()],
                    'description': 'short_name for the source'
                },
                'filetype': {
                    'type': 'string',
                    'description': 'The format of the metadata (ie, xml, json)'
                }
            },
            'required': [
                'doc',
                'docID',
                'source',
                'filetype'
            ]
        }


class NormalizedDocument(BaseDocument):

    with open('normalized.json') as f:
        schema = json.load(f)
