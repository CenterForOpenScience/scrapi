from types import FunctionType


def lint_dict(actual, types, name=''):
    for field_name, expected in types.items():
        value = actual[field_name]
        new_name = '{} {}'.format(name, field_name)
        lint(value, expected, name=new_name)


def lint(actual, expected, name=''):
    if isinstance(expected, tuple) and isinstance(expected[0], FunctionType):
        func = expected[0]
        func(actual, *expected[1:], name=name)
    elif isinstance(expected, dict):
        pretty_isinstance(actual, dict, name)
        lint_dict(actual, expected, name=name)
    elif isinstance(expected, list):
        pretty_isinstance(actual, list, name)
        for item in actual:
            lint(item, expected[0], name=name)
    else:
        pretty_isinstance(actual, expected, name)


def pretty_isinstance(actual, expected, name):
    if not isinstance(actual, expected):
        actual = type(actual)
        name = ''.join(["['{}']".format(x) for x in name.split(' ') if x])
        errmsg = 'Expected "root{name}" to be of type {expected} but found {actual}'
        raise TypeError(errmsg.format(**locals()))


def truthy(actual, expected, name):
    pretty_isinstance(actual, expected, name)

    if not actual:
        raise TypeError('Expected {name} to exist, but it does not').format(name=name)
