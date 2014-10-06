from scrapi.linter.util import lint


class BaseDocument(object):

    """
        For file objects. Automatically lints input to ensure
        compatibility with scrAPI.
    """

    REQUIRED_FIELDS = {}

    def __init__(self, attributes):
        lint(attributes, self.REQUIRED_FIELDS)

        self.attributes = attributes

    def get(self, attribute):
        """
            Maintains compatibility with previous dictionary implementation of scrAPI
            :: str -> str
        """
        return self.attributes.get(attribute)

    def __getitem__(self, attr):
        return self.attributes[attr]

    def __setitem__(self, attr, val):
        self.attributes[attr] = val

    def __delitem__(self, attr):
        del self.attributes[attr]


class RawDocument(BaseDocument):

    REQUIRED_FIELDS = {
        'doc': str,
        'docID': unicode,
        'source': unicode,
        'filetype': unicode
    }


class NormalizedDocument(BaseDocument):
    CONTRIBUTOR_FIELD = {
        'email': unicode,
        'prefix': unicode,
        'given': unicode,
        'middle': unicode,
        'family': unicode,
        'suffix': unicode
    }
    ID_FIELD = {
        'url': unicode,
        'doi': unicode,
        'serviceID': unicode
    }

    REQUIRED_FIELDS = {
        'title': unicode,
        'contributors': [CONTRIBUTOR_FIELD],
        'id': ID_FIELD,
        'source': unicode,
        'timestamp': unicode,
        'description': unicode,
        'tags': [unicode],
        'dateUpdated': unicode,
        'dateCreated': unicode
    }
