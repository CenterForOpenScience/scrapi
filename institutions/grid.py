import six
import json
import logging

from schema_transformer.transformer import JSONTransformer

from .institutions import Institution

logger = logging.getLogger(__name__)

schema = {
    'name': ('/name'),
    'location': {
        'street_address': ('/addresses', lambda x: x[0]['line_1'] if x else None),
        'city': ('/addresses', lambda x: x[0]['city'] if x else None),
        'state': ('/addresses', lambda x: x[0]['state'] if x else None),
        'ext_code': ('/addresses', lambda x: x[0]['postcode'] if x else None),
        'country': ('/addresses', lambda x: x[0]['country'] if x else None)
    },
    'web_url': ('/links', lambda x: x[0] if x else None),
    'id_': '/id',
    'other_names': ('/aliases', '/acronyms', lambda x, y: (x or []) + (y or []))
}


def get_jsons(grid_file):
    with open(grid_file) as f:
        f.readline()  # Pop off the top
        f.readline()
        for line in f:
            try:
                yield json.loads(line[:-2])
            except ValueError:
                yield json.loads(line)
                break


def populate(grid_file):
    transformer = JSONTransformer(schema)
    for doc in get_jsons(grid_file):
        transformed = transformer.transform(doc, load=False)
        try:
            # Prevent logger output encoding errors from stopping script
            logger.info('Adding {0}.'.format(transformed['name']))
        except Exception:
            pass
        for key, val in six.iteritems(transformed):
            if isinstance(val, six.binary_type):
                transformed[key] = str(val)
        inst = Institution(**transformed)

        inst.save()
