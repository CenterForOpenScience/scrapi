'''
Harvester for the Speech and Language Data Repository (SLDR/ORTOLANG) for the SHARE project

Example API call: http://sldr.org/oai-pmh.php?verb=ListRecords&metadataPrefix=oai_dc
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class SldrHarvester(OAIHarvester):
    short_name = 'sldr'
    long_name = 'Speech and Language Data Repository (SLDR/ORTOLANG)'
    url = 'http://sldr.org'

    base_url = 'http://sldr.org/oai-pmh.php'
    property_list = ['source', 'type', 'rights', 'modified', 'temporal', 'extent', 'spatial', 'identifier', 'abstract', 'date', 'created', 'license', 'bibliographicCitation', 'isPartOf', 'tableOfContents', 'accessRights', 'setSpec']
    approved_sets = ['publisher',
                     'date',
                     'language',
                     'rights',
                     'license',
                     'format',
                     'isPartOf',
                     'created',
                     'accessRights',
                     'temporal',
                     'source',
                     'bibliographicCitation',
                     'modified',
                     'spatial',
                     'requires',
                     'identifier',
                     'type',
                     'tableOfContents',
                     'ortolang',
                     'archive:long-term']
    timezone_granularity = False
