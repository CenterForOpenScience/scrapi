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
    property_list = ['source', 'type', 'rights', '{http://purl.org/dc/terms/}modified', '{http://purl.org/dc/terms/}temporal', '{http://purl.org/dc/terms/}extent', '{http://purl.org/dc/terms/}spatial', 'identifier', '{http://purl.org/dc/terms/}abstract', 'date', '{http://purl.org/dc/terms/}created', '{http://purl.org/dc/terms/}license', '{http://purl.org/dc/terms/}bibliographicCitation', '{http://purl.org/dc/terms/}isPartOf', '{http://purl.org/dc/terms/}tableOfContents', '{http://purl.org/dc/terms/}accessRights', 'setSpec']
    approved_sets =  ['publisher',
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
    ]
    timezone_granularity = False
