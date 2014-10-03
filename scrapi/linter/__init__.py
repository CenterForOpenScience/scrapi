import json
import pickle
from datetime import datetime

from scrapi.linter.document import RawDocument, NormalizedDocument


def is_serializable(dill, name=''):
    for key, val in dill.items():
        if isinstance(val, dict):
            is_serializable(val, name=name + ' ' + key)
        else:
            is_pickable(val, name=name + ' ' + key)
            json.dumps(val)


def is_pickable(val, name='root'):
    try:
        pickle.dumps(val)
    except TypeError:
        if isinstance(val, list):
            val = type(val[0])
        else:
            val = type(val)
        name = ''.join(["['{}']".format(x) for x in name.split(' ')])

        raise TypeError('Cannot pickle type {} from {}'.format(val, name))


def lint(consume, normalize):
    """
        Runs the consume and normalize functions, ensuring that
        they match the requirements of scrAPI.
    """
    output = consume()

    if not isinstance(output, list):
        raise TypeError("{} does not return type list".format(consume))

    if len(output) == 0:
        raise ValueError('{} returned an empty list'.format(consume))

    normalized_output = []

    for doc in output:
        if not isinstance(doc, RawDocument):
            raise TypeError("{} returned a list containing a non-RawDocument item".format(consume))

        normalized_output.append(normalize(doc, datetime.now()))

    for doc in normalized_output:
        if not isinstance(doc, NormalizedDocument) and doc:
            raise TypeError("{} does not return type NormalizedDocument".format(consume))

    is_serializable(output[0].attributes)
    is_serializable(normalized_output[0].attributes)

    return 'Linting passed with No Errors'
