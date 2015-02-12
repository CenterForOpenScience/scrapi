"""
Harvests Virginia Tech VTechWorks metadata for ingestion into the SHARE service

Information about VTechWorks at https://github.com/CenterForOpenScience/SHARE/blob/master/providers/edu.vt.vtechworks.md

Example API call: http://vtechworks.lib.vt.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from=2014-09-29
"""

from __future__ import unicode_literals

from scrapi.base import OAIHarvester


vtechworks = OAIHarvester(
    name='vtechworks',
    base_url='http://vtechworks.lib.vt.edu/oai/',
    property_list=['type', 'source', 'publisher', 'format', 'date',
                   'identifier', 'setSpec', 'rights', 'language', 'relation']
)

consume = vtechworks.harvest
normalize = vtechworks.normalize
