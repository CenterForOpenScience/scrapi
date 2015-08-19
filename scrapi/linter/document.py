import json
import copy

import jsonschema

from scrapi import registry
from scrapi.util import json_without_bytes


def strip_empty(document, required=tuple()):
    ''' Removes empty fields from the processed schema
    '''
    new_doc = {}
    for k, v in document.items():
        if k in required:
            new_doc[k] = v
        else:
            new_val = do_strip_empty(v)
            if k == 'otherProperties':
                new_val = [property for property in new_val if property.get('properties')]
            if new_val:
                new_doc[k] = new_val
    return new_doc


def strip_list(l):
    return list(filter(lambda x: x, map(do_strip_empty, l)))


def do_strip_empty(value):
    ''' Filters empty values from container types
    '''
    return {
        dict: strip_empty,
        list: strip_list,
        tuple: strip_list
    }.get(type(value), lambda x: x)(value)


class BaseDocument(object):

    """
        For file objects. Automatically validates input to ensure
        compatibility with scrAPI.
    """

    schema = {}
    format_checker = jsonschema.FormatChecker()

    def __init__(self, attributes, validate=True, clean=False):
        ''' Initializes a document

            :param dict attributes: the dictionary representation of a document
            :param bool validate: If true, the object will be validated before creation
            :param bool clean: If true, optional fields that are null will be deleted
        '''
        # validate a version of the attributes that are safe to check
        # against the JSON schema

        # Allows validation in python3
        self.attributes = json_without_bytes(copy.deepcopy(attributes))
        if clean:
            self.attributes = strip_empty(self.attributes, required=self.schema.get('required', []))
        if validate:
            self.validate()

    def validate(self, schema=None):
        jsonschema.validate(self.attributes, schema or self.schema, format_checker=self.format_checker)

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
