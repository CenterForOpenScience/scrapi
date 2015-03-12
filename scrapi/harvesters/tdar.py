"""
Harvester for the The Digital Archaeological Record (tDAR) for the SHARE project

More information at https://github.com/CenterForOpenScience/SHARE/blob/master/providers/org.tdar.md

Example API call: http://core.tdar.org/oai-pmh/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2014-10-05
"""


from __future__ import unicode_literals

from scrapi import util
from scrapi.base import OAIHarvester


class TDARHarvester(OAIHarvester):
    short_name = 'tdar'
    long_name = 'The Digital Archaeological Record'
    base_url = 'http://core.tdar.org/oai-pmh/oai'
    property_list = ['type', 'date', 'setSpec', 'type', 'publisher', 'coverage']

    def get_ids(self, result, doc):
        """
        Gather the DOI and url from identifiers, if possible.
        Tries to save the DOI alone without a url extension.
        Tries to save a link to the original content at the source,
        instead of direct to a PDF, which is usually linked with viewcontent.cgi?
        in the url field
        """
        serviceID = doc.get('docID')

        url = 'http://core.tdar.org/document/' + serviceID.replace('oai:tdar.org:Resource:', '')

        return {
            'serviceID': serviceID,
            'url': util.copy_to_unicode(url),
            'doi': ''
        }
