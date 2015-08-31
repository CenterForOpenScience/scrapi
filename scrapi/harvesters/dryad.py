'''
Harvester for Dryad for the SHARE project

Example API call: http://www.datadryad.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

import logging
from lxml import etree

from scrapi.base import helpers
from scrapi.base import OAIHarvester

logger = logging.getLogger(__name__)


def format_dois_dryad(*args):
    prefix = 'http://dx.doi.org/{}'
    urls = []
    for arg in args:
        if isinstance(arg, list):
            for url in arg:
                if 'doi:' in url:
                    urls.append(prefix.format(url.replace('doi:', '')))
        elif arg:
            if 'doi:' in arg:
                urls.append(prefix.format(arg.replace('doi:', '')))
    return urls


class DryadHarvester(OAIHarvester):
    short_name = 'dryad'
    long_name = 'Dryad Data Repository'
    url = 'http://www.datadryad.org/'

    base_url = 'http://www.datadryad.org/oai/request'
    property_list = ['rights', 'format', 'relation', 'date',
                     'identifier', 'type', 'setSpec']
    timezone_granularity = True

    @property
    def schema(self):
        return helpers.updated_schema(self._schema, {
            "uris": {
                "objectUris": ('//dc:relation/node()', '//dc:identifier/node()', format_dois_dryad)
            }
        })

    def normalize(self, raw_doc):
        result = etree.XML(raw_doc['doc'])

        status = (result.xpath('//dc:status/node()', namespaces=self.namespaces) or [''])[0]
        if str(status).lower() in ['deleted', 'item is not available']:
            logger.info('Not normalizing record with ID {}, status {}'.format(raw_doc['docID'], status))
            return None
        doc_type = (result.xpath('//dc:type/node()', namespaces=self.namespaces) or [''])[0]
        if not doc_type.lower() == 'article':
            logger.info('Not normalizing record with ID {}, type {}'.format(raw_doc['docID'], doc_type))
            return None

        return super(OAIHarvester, self).normalize(raw_doc)
