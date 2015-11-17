from __future__ import unicode_literals
import six
import csv
import codecs
import logging

from schema_transformer.transformer import CSVTransformer

from .institutions import Institution

logger = logging.getLogger(__name__)

class IpedsTransformer(CSVTransformer):
    def _transform_string(self, val, doc):
        val = super(IpedsTransformer, self)._transform_string(val, doc)
        #try:
         #   return val.encode('utf-8')
        #except AttributeError:
        return six.u(val)

    def load(self, doc):
        return doc

schema = {
    'name': 'INSTNM',
    'location': {
        'street_address': 'ADDR',
        'city': 'CITY',
        'state': 'STABBR',
        'ext_code': 'ZIP'
    },
    'web_url': 'WEBADDR',
    'id_': 'UNITID',
    'public': ('CONTROL', lambda x: int(x) == 2),
    'for_profit': ('CONTROL', lambda x: int(x) == 3),
    'degree': ('UGOFFER', lambda x: int(x) == 1)
}

def populate(ipeds_file):
    with codecs.open(ipeds_file, encoding='Windows-1252', errors='replace') as f:
        reader = csv.reader(f)

        transformer = IpedsTransformer(schema, next(reader))
        for row in reader:
            transformed = transformer.transform(row)
            logger.info('Adding {0}.'.format(transformed['name']))

            inst = Institution(country='United States', **transformed)
            inst.save()
