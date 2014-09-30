class BaseDocument(object):

    """
        For file objects. Automatically lints input to ensure
        compatibility with scrAPI.
    """

    REQUIRED_FIELDS = {}

    def __init__(self, attributes):
        self._lint(self.REQUIRED_FIELDS, attributes)

        self.attributes = attributes

    def _lint(self, types, actual):
        for field_name, field_type in types.items():
            try:
                if isinstance(field_type, dict):
                    self._list(field_type, actual[field_name])
                elif isinstance(field_type, tuple):
                    assert isinstance(actual[field_name], field_type[0])
                    for item in actual[field_name]:
                        if isinstance(field_type[1], object):
                            self._list(field_type[1], item)
                        else:
                            assert isinstance(item, field_type[1])
                else:
                    assert isinstance(actual[field_name], field_type)
            except AssertionError:
                raise TypeError('Field "{}" must be of type {}'.format(field_name, field_type))

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
        'docID': str,
        'source': str,
        'filetype': str
    }


class NormalizedDocument(BaseDocument):
    CONTRIBUTOR_FIELD = {
        'email': str,
        'prefix': str,
        'given': str,
        'middle': str,
        'family': str,
        'suffix': str
    }

    REQUIRED_FIELDS = {
        'title': str,
        'contributors': (list, CONTRIBUTOR_FIELD),
        'id': {
            'url': str,
            'doi': str,
            'serviceID': str
        },
        'source': str,
        'timestamp': str,
        'description': str,
        'tags': (list, str),
        'dateUpdated': str,
        'dateCreated': str
    }
