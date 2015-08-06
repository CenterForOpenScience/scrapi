"""
Harvester for the SHAREOK Repository Repository for the SHARE project

Example API call: https://shareok.org/oai/request?verb=ListRecords&metadataPrefix=oai_dc
"""
from __future__ import unicode_literals

import logging
from lxml import etree

from scrapi.base import OAIHarvester

logger = logging.getLogger(__name__)


class ShareOKHarvester(OAIHarvester):
    short_name = 'shareok'
    long_name = 'SHAREOK Repository'
    url = 'https://shareok.org'
    timezone_granularity = True
    verify = False

    base_url = 'https://shareok.org/oai/request'

    # TODO - add date back in once we fix elasticsearch mapping
    property_list = [
        'type', 'source', 'format',
        'description', 'setSpec'
    ]
    approved_sets = [
        'com_11244_14447',
        'com_11244_1',
        'col_11244_14248',
        'com_11244_6231',
        'col_11244_7929',
        'col_11244_7920',
        'col_11244_10476',
        'com_11244_10465',
        'com_11244_10460',
        'col_11244_10466',
        'col_11244_10464',
        'col_11244_10462',
        'com_11244_15231'
    ]

    def normalize(self, raw_doc):
        str_result = raw_doc.get('doc')
        result = etree.XML(str_result)

        set_spec = result.xpath(
            'ns0:header/ns0:setSpec/node()',
            namespaces=self.namespaces
        )
        # check if all of the sets in the record are in the approved set list.
        # If all of them aren't, don't normalize.
        actual = {x.replace('publication:', '') for x in set_spec}
        if not len(set(self.approved_sets).intersection(actual)) == len(actual):
            logger.info('Series {} not in approved list'.format(set_spec))
            return None

        status = result.xpath('ns0:header/@status', namespaces=self.namespaces)
        if status and status[0] == 'deleted':
            logger.info('Deleted record, not normalizing {}'.format(raw_doc['docID']))
            return None

        return super(OAIHarvester, self).normalize(raw_doc)
