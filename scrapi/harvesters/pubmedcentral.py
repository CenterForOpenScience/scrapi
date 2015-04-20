"""
<<<<<<< HEAD:scrapi/harvesters/pubmedcentral.py
Harvester of PubMed Central for the SHARE notification service

Example API call: http://www.pubmedcentral.nih.gov/oai/oai.cgi?verb=ListRecords&metadataPrefix=oai_dc&from=2015-04-13&until=2015-04-14
=======
Harvester of pubmed central for the SHARE notification service
>>>>>>> 10981892c236658dd3d4d9cd6caceedf300088bb:scrapi/harvesters/pubmedcentral.py
"""


from __future__ import unicode_literals

from scrapi.base import schemas
from scrapi.base import helpers
from scrapi.base import OAIHarvester


def oai_extract_url_pubmedcentral(identifiers):
    identifiers = [identifiers] if not isinstance(identifiers, list) else identifiers
    for item in identifiers:
        try:
            found_url = helpers.URL_REGEX.search(item).group()
            if 'viewcontent' not in found_url and '/pubmed/' in found_url:
                return found_url.decode('utf-8')
        except AttributeError:
            continue


class PubMedCentralHarvester(OAIHarvester):
    short_name = 'pubmedcentral'
    long_name = 'PubMed Central'
    url = 'http://www.ncbi.nlm.nih.gov/pmc/'

    schema = helpers.updated_schema(
        schemas.OAISCHEMA,
        {
            "uris": {
                "canonicalUri": ('//dc:identifier/node()', oai_extract_url_pubmedcentral)
            }
        }
    )

    base_url = 'http://www.pubmedcentral.nih.gov/oai/oai.cgi'
    property_list = [
        'type', 'source', 'publisher', 'rights',
        'format', 'setSpec', 'date', 'identifier'
    ]
