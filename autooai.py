import re
import shutil
import logging

import furl
from lxml import etree

from scrapi import requests

URL_RE = re.compile(r'(https?:\/\/[^\/]*)')

NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/',
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'}

BASE_SCHEMA = ['title', 'contributor', 'creator', 'subject',
               'description', 'language', 'publisher']

OAI_TEMPLATE = """'''
Harvester for the {0} for the SHARE project
Example API call: {1}
'''
from __future__ import unicode_literals

from scrapi.base import OAIHarvester


class {2}Harvester(OAIHarvester):
    short_name = '{3}'
    long_name = '{4}'
    url = '{5}'

    base_url = '{6}'
    property_list = {7}
    timezone_granularity = {8}
"""

logger = logging.getLogger(__name__)


def get_oai_properties(base_url, shortname):
    """ Makes a request to the provided base URL for the list of properties
        returns a dict with list of properties
    """

    try:
        prop_base = furl.furl(base_url)
        prop_base.args['verb'] = 'ListRecords'
        prop_base.args['metadataPrefix'] = 'oai_dc'

        prop_data_request = requests.requests.get(prop_base.url)
        all_prop_content = etree.XML(prop_data_request.content)
        try:
            pre_names = all_prop_content.xpath('//ns0:metadata', namespaces=NAMESPACES)[0].getchildren()[0].getchildren()
        except IndexError:
            pre_names = None

        all_names = [name.tag.replace('{' + NAMESPACES['dc'] + '}', '') for name in pre_names]
        return list({name for name in all_names if name not in BASE_SCHEMA}) + ['setSpec']

    # If anything at all goes wrong, just render a blank form...
    except Exception as e:
        raise ValueError('OAI Processing Error - {}'.format(e))


def formatted_oai(ex_call, class_name, shortname, longname, normal_url, oai_url, prop_list, tz_gran):
    return OAI_TEMPLATE.format(longname, ex_call, class_name, shortname, longname, normal_url, oai_url, prop_list, tz_gran)


def get_id_props(baseurl):
    identify_url = baseurl + '?verb=Identify'
    id_data_request = requests.requests.get(identify_url)
    id_content = etree.XML(id_data_request.content)
    return id_content.xpath('//ns0:repositoryName/node()', namespaces=NAMESPACES)[0], id_content.xpath('//ns0:granularity/node()', namespaces=NAMESPACES)[0]


def get_favicon(baseurl, shortname):
    r = requests.requests.get('http://grabicon.com/icon?domain={}&origin=cos.io&size=16'.format(baseurl), stream=True)
    if r.status_code == 200:
        with open('img/favicons/{}_favicon.ico'.format(shortname), 'w') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)


def generate_oai(baseurl, shortname):
    prop_list = get_oai_properties(baseurl, shortname)
    ex_call = baseurl + '?verb=ListRecords&metadataPrefix=oai_dc'

    class_name = shortname.capitalize()

    longname, tz_gran = get_id_props(baseurl)

    found_url = URL_RE.search(baseurl).group()

    if 'hh:mm:ss' in tz_gran:
        tz_gran = True
    else:
        tz_gran = False

    return formatted_oai(ex_call, class_name, shortname, longname, found_url, baseurl, prop_list, tz_gran)


def generate_oai_harvester(shortname, baseurl=None, favicon=False):
    if baseurl:
        text = generate_oai(baseurl, shortname)

        with open('scrapi/harvesters/{}.py'.format(shortname), 'w') as outfile:
            outfile.write(text)

    if favicon:
        get_favicon(baseurl, shortname)
