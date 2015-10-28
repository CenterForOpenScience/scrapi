from __future__ import unicode_literals

import copy

import pytest
from jsonschema.exceptions import ValidationError

from scrapi.linter import NormalizedDocument


class TestValidation(object):

    def test_validate_with_clean(self):
        expected = {
            "description": "This is a  test",
            "contributors": [
               {
                    "name": "Test Testerson Jr",
                    "givenName": "Test",
                    "familyName": "Testerson",
                    "additionalName": "",
                    "sameAs": [],
                }
            ],
            'title': '',
            'subjects': ['Math'],
            'uris': {
                "canonicalUri": "http://example.com"
            },
            "providerUpdatedDateTime": "2015-02-02T00:00:00Z",
            "shareProperties": {
                "source": "test",
                "docID": "1"
            }
        }
        doc = NormalizedDocument(to_be_validated, clean=True)
        assert doc.attributes == expected


    def test_validate(self):
        expected = {
            "description": "This is a  test",
            "contributors": [
                {
                    "name": "Test Testerson Jr",
                    "givenName": "Test",
                    "familyName": "Testerson",
                    "additionalName": "",
                    "sameAs": []
                }
            ],
            'title': '',
            'tags': ['', '', ''],
            'subjects': ['', 'Math'],
            'uris': {
                "canonicalUri": "http://example.com"
            },
            "providerUpdatedDateTime": "2015-02-02T00:00:00Z",
            "shareProperties": {
                "source": "test",
                "docID": "1"
            },
            "otherProperties": [
                {
                    "name": "Empty2",
                    "properties": {
                        "Empty2": None
                    }
                }
            ]
        }
        doc = NormalizedDocument(to_be_validated)
        assert doc.attributes == expected


    def test_validate_fails(self):
        to_be_tested = copy.deepcopy(to_be_validated)
        to_be_tested['providerUpdatedDateTime'] = 'Yesterday'
        with pytest.raises(ValidationError) as e:
            doc = NormalizedDocument(to_be_tested)

        with pytest.raises(ValidationError) as e:
            doc = NormalizedDocument(to_be_tested, validate=False)
            doc.validate()



to_be_validated = {
    "description": "This is a  test",
    "contributors": [
        {
            "name": "Test Testerson Jr",
            "givenName": "Test",
            "familyName": "Testerson",
            "additionalName": "",
            "sameAs": [],
        }
    ],
    'title': '',
    'tags': ['', '', ''],
    'subjects': ['', 'Math'],
    'uris': {
        "canonicalUri": "http://example.com"
    },
    "providerUpdatedDateTime": "2015-02-02T00:00:00Z",
    "shareProperties": {
        "source": "test",
        "docID": "1"
    },
    "otherProperties": [
        {
            "name": "Empty2",
            "properties": {
                "Empty2": None
            }
        }
    ]
}


