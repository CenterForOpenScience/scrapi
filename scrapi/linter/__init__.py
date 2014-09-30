from datetime import datetime

from scrapi.linter.document import RawDocument, NormalizedDocument


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
        if not isinstance(doc, NormalizedDocument):
            raise TypeError("{} does not return type NormalizedDocument".format(consume))

    return 'Linting passed with No Errors'
