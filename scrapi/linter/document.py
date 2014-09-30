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
        'doc': basestring,
        'docID': basestring,
        'source': basestring,
        'filetype': basestring
    }


class NormalizedDocument(BaseDocument):
    CONTRIBUTOR_FIELD = {
        'email': basestring,
        'prefix': basestring,
        'given': basestring,
        'middle': basestring,
        'family': basestring,
        'suffix': basestring
    }
    ID_FIELD = {
        'url': basestring,
        'doi': basestring,
        'serviceID': basestring
    }

    REQUIRED_FIELDS = {
        'title': basestring,
        'contributors': [CONTRIBUTOR_FIELD],
        'id': ID_FIELD,
        'source': basestring,
        'timestamp': basestring,
        'description': basestring,
        'tags': [basestring],
        'dateUpdated': basestring,
        'dateCreated': basestring
    }
