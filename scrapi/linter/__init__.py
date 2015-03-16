import json
import pickle

from scrapi.linter.document import RawDocument, NormalizedDocument


def is_serializable(dill, name=''):
    for key, val in dill.items():
        if isinstance(val, dict):
            is_serializable(val, name=name + ' ' + key)
        else:
            is_picklable(val, name=name + ' ' + key)
            json.dumps(val)


def is_picklable(val, name='root'):
    try:
        pickle.dumps(val)
    except (TypeError, pickle.PicklingError):
        if isinstance(val, list):
            val = type(val[0])
        else:
            val = type(val)
        name = ''.join(["['{}']".format(x) for x in name.split(' ')])

        raise TypeError('Cannot pickle type {} from {}'.format(val, name))


def lint(harvest, normalize):
    """
        Runs the harvest and normalize functions, ensuring that
        they match the requirements of scrAPI.
    """

    output = harvest()

    if not isinstance(output, list):
        raise TypeError("{} does not return type list".format(harvest))

    if len(output) == 0:
        raise ValueError('{} returned an empty list'.format(harvest))

    normalized_output = []

    for doc in output:
        if not isinstance(doc, RawDocument):
            raise TypeError("{} returned a list containing a non-RawDocument item".format(harvest))

        normalized_output.append(normalize(doc))

    for doc, raw_doc in zip(normalized_output, output):
        if not isinstance(doc, NormalizedDocument) and doc:
            raise TypeError("{} does not return type NormalizedDocument".format(harvest))
        if doc and doc['id']['serviceID'] != raw_doc['docID']:
            raise ValueError('Service ID {} does not match {}'.format(doc['id']['serviceID'], raw_doc['docID']))
        if doc and not doc['id']['url']:
            raise ValueError('No url provided')

    output = [x for x in output if x]
    normalized_output = [x for x in normalized_output if x]

    if output:
        is_serializable(output[0].attributes)
    else:
        raise ValueError('No lintable values gathered.')

    if normalized_output:
        is_serializable(normalized_output[0].attributes)
    else:
        raise ValueError('No lintable values in normalized.')

    return 'Linting passed with No Errors'
