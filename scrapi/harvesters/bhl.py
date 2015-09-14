"""Harvests Biodiversity Heritage Library OAI Repository (BHL) metadata for ingestion into the SHARE service.
Example API call: http://www.biodiversitylibrary.org/oai?verb=ListRecords&metadataPrefix=oai_dc&from=2015-02-01
"""
import re
from scrapi.base import OAIHarvester
from scrapi.base.helpers import updated_schema, default_name_parser


def institution_name_parser(names):
    ''' Parse institution names '''
    contributor_list = []
    for inst in names:
        contributor = {
            'name': inst,
            'sameAs': []
        }
        contributor_list.append(contributor)

    return contributor_list


def aoi_process_contributors_bhl(*args):
    ''' Parse people name for BHL'''
    names = []
    for arg in args:
        if isinstance(arg, list):
            for name in arg:
                names.append(name)
        elif arg:
            names.append(arg)
    # Filter people names and clean dates and extra spaces.
    people = [re.sub(r'\d+-(\d+)?', r'', n).strip() for n in filter(lambda x: ', ' in x, names)] or []
    # Filter institution names and clean tabs.
    inst = [re.sub(r'\\t', r'', n).strip() for n in filter(lambda x: ', ' not in x, names) or []]
    # Parse names differently if they're people's or institutions' names.
    if len(people) > 0 and len(inst) > 0:
        return default_name_parser(people) + institution_name_parser(inst)
    elif len(inst) > 0:
        return institution_name_parser(inst)
    if len(people) > 0:
        return default_name_parser(people)
    else:
        return ''


class BHLHarvester(OAIHarvester):
    short_name = 'bhl'
    long_name = 'Biodiversity Heritage Library OAI Repository'
    url = 'http://www.biodiversitylibrary.org/'

    base_url = 'http://www.biodiversitylibrary.org/oai'

    @property
    def schema(self):
        return updated_schema(self._schema, {
            'contributors': ('//dc:creator/node()', '//dc:contributor/node()', aoi_process_contributors_bhl)
        })

    property_list = [
        'type', 'date', 'relation', 'setSpec', 'rights'
    ]
