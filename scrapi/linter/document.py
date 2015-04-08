import json

import jsonschema

from scrapi import registry


class BaseDocument(object):

    """
        For file objects. Automatically validates input to ensure
        compatibility with scrAPI.
    """

    schema = {}

    def __init__(self, attributes):
        jsonschema.validate(attributes, self.schema, format_checker=jsonschema.FormatChecker())

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
