'''
Harvester for Dryad for the SHARE project

Example API call: http://www.datadryad.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from lxml import etree
import logging

from scrapi.base import OAIHarvester

logger = logging.getLogger(__name__)


class DryadHarvester(OAIHarvester):
    short_name = 'dryad'
    long_name = 'Dryad Data Repository'
    url = 'http://www.datadryad.org/oai/request'

    base_url = 'http://www.datadryad.org/oai/request'
    property_list = ['rights', 'format', 'relation', 'date',
                     'identifier', 'type', 'setSpec']
    timezone_granularity = True

    def normalize(self, raw_doc):
        result = etree.XML(raw_doc['doc'])

        status = (result.xpath('//dc:status/node()', namespaces=self.namespaces) or [''])[0]
        if str(status).lower() in ['deleted', 'item is not available']:
            logger.info('Not normalizing record with ID {}, status {}'.format(raw_doc['docID'], status))
            return None

        return super(OAIHarvester, self).normalize(raw_doc)
