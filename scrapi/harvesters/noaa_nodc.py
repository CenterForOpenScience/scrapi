"""Harvester of National Oceanic and Atmosphere Administration's National Oceanographic Data Center

Example API call: http://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.nodc:00001339;view=xml

NODC provides an OAI-PMH interface at
http://data.nodc.noaa.gov/cgi-bin/oai-pmh, but it does not contain the
contributors key mandated by the SHARE-schema spec.  Therefore, we
have to use the XML interface, which contains much more metadata.
However, the XML interface is not searchable, so we still use the
OAI-PMH interface for queries. Fun!

"""

from __future__ import unicode_literals

import time
import logging
from datetime import date, timedelta

from lxml import etree
from nameparser import HumanName
from requests import exceptions

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.base.helpers import compose, single_result, date_formatter, language_codes, coerce_to_list
from scrapi.linter.document import RawDocument
from scrapi.util import copy_to_unicode

logger = logging.getLogger(__name__)


def text_only_list(elems):
    return [text_only(elem) for elem in elems]


def text_only(elem):
    etree.strip_tags(elem, '*')
    inner_text = elem.text
    if inner_text:
        return inner_text.strip()
    return ''


def filter_to_publishers(parties):
    return filter_responsible_parties(parties, ['publisher'])


def filter_to_contributors(parties):
    return filter_responsible_parties(parties, ['resourceProvider'])


def filter_responsible_parties(parties, roles):
    return [party for party in parties if party.xpath('./gmd:role/gmd:CI_RoleCode/node()', namespaces=party.nsmap)[0] in roles]


def parse_contributors(parties):
    contributors = []
    for party in parties:
        contributor = {}
        individual = extract_individual(party)
        organization = extract_organization(party)
        if individual:
            contributor = individual
            if organization:
                contributor['affiliation'] = [organization]
        elif organization:
            contributor = organization
        else:
            raise NotImplementedError

        email = party.xpath('./gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress', namespaces=party.nsmap)
        if len(email) > 0:
            contributor['email'] = text_only(email[0])
        contributors.append(contributor)

    return contributors


def extract_individual(party):
    individual = party.xpath('./gmd:individualName/gmx:Anchor', namespaces=party.nsmap)
    if len(individual) > 0:
        name = HumanName(individual[0].text)
        ns_prefix = party.nsmap['xlink']
        sameAs = individual[0].get('{' + ns_prefix + '}href')
        return {
            'name': individual[0].text,
            'givenName': name.first,
            'additionalName': name.middle,
            'familyName': name.last,
            'sameAs': [sameAs],
        }


def extract_organization(party):
    organization = party.xpath('./gmd:organisationName/gmx:Anchor', namespaces=party.nsmap)
    if len(organization) > 0:
        ns_prefix = party.nsmap['xlink']
        sameAs = organization[0].get('{' + ns_prefix + '}href')
        return {
            'name': organization[0].text,
            'sameAs': [sameAs],
        }


def turn_lots_into_some(keyword_groups):
    keywords = []
    for keyword_group in keyword_groups:
        keyword_type = keyword_group.xpath('./gmd:type/gmd:MD_KeywordTypeCode/node()', namespaces=keyword_group.nsmap)
        if len(keyword_type) > 0 and keyword_type[0] != 'datacenter':
            keywords.extend(keyword_group.xpath('./gmd:keyword/gmx:Anchor/node()', namespaces=keyword_group.nsmap))
    return keywords


class NODCHarvester(XMLHarvester):
    short_name = 'noaa_nodc'
    long_name = 'National Oceanographic Data Center'
    url = 'https://www.nodc.noaa.gov/'

    DEFAULT_ENCODING = 'UTF-8'
    record_encoding = None

    canonical_base_url = 'http://data.nodc.noaa.gov/cgi-bin/iso?id={}'

    namespaces = {
        'gco': 'http://www.isotc211.org/2005/gco',
        'gmd': 'http://www.isotc211.org/2005/gmd',
        'gmi': 'http://www.isotc211.org/2005/gmi',
        'gml': 'http://www.opengis.net/gml/3.2',
        'gmx': 'http://www.isotc211.org/2005/gmx',
        'gsr': 'http://www.isotc211.org/2005/gsr',
        'gss': 'http://www.isotc211.org/2005/gss',
        'gts': 'http://www.isotc211.org/2005/gts',
        'srv': 'http://www.isotc211.org/2005/srv',
        'xlink': 'http://www.w3.org/1999/xlink',
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
        # xsi:schemaLocation="http://www.ngdc.noaa.gov/metadata/published/xsd/schema.xsd",
    }

    id_stanza = './gmd:identificationInfo/gmd:MD_DataIdentification/'
    cite_stanza = id_stanza + 'gmd:citation/gmd:CI_Citation/'
    schema = {
        'title': (cite_stanza + 'gmd:title', compose(text_only, single_result)),
        'description': (id_stanza + 'gmd:abstract', compose(text_only, single_result)),
        'contributors': (cite_stanza + 'gmd:citedResponsibleParty/gmd:CI_ResponsibleParty', compose(parse_contributors, filter_to_contributors)),
        'uris': {
            'canonicalUri': (
                './gmd:fileIdentifier',
                compose(lambda x: 'http://data.nodc.noaa.gov/cgi-bin/iso?id={}'.format(x), text_only, single_result)
            ),
        },
        'publisher': (
            cite_stanza + 'gmd:citedResponsibleParty/gmd:CI_ResponsibleParty',
            compose(extract_organization, single_result, filter_to_publishers),
        ),
        'providerUpdatedDateTime': ('./gmd:dateStamp/gco:DateTime/node()', compose(date_formatter, single_result)),
        'languages': ('./gmd:language/gmd:LanguageCode', compose(language_codes, text_only_list, coerce_to_list)),
        'subjects': (id_stanza + 'gmd:descriptiveKeywords/gmd:MD_Keywords', lambda x: turn_lots_into_some(x)),
    }

    def harvest(self, start_date=None, end_date=None):
        """ First, get a list of all recently updated study urls,
        then get the xml one by one and save it into a list
        of docs including other information """

        start_date = (start_date or date.today() - timedelta(settings.DAYS_BACK)).isoformat()
        end_date = (end_date or date.today()).isoformat()
        start_date += 'T00:00:00Z'
        end_date += 'T00:00:00Z'

        search_base_url = 'http://data.nodc.noaa.gov/cgi-bin/oai-pmh?verb=ListRecords'
        search_url_end = '&metadataPrefix=oai_dc&from=' + start_date + '&until=' + end_date
        search_url = search_base_url + search_url_end

        oai_ns = {
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'dc': 'http://purl.org/dc/elements/1.1/',
        }

        dataset_ids = []
        while True:

            record_list = requests.get(search_url)
            record_list_xml = etree.XML(record_list.content)
            if record_list_xml.xpath('./oai_dc:error', namespaces=oai_ns):
                break

            for dataset in record_list_xml.xpath('./oai_dc:ListRecords/oai_dc:record', namespaces=oai_ns):
                dataset_ids.append(dataset.xpath('./oai_dc:header/oai_dc:identifier/node()', namespaces=oai_ns)[0])

            token = record_list_xml.xpath('./oai_dc:ListRecords/oai_dc:resumptionToken/node()', namespaces=oai_ns)
            if not token:
                break

            search_url = search_base_url + '&resumptionToken=' + token[0]

        # grab each of those urls for full content
        logger.info('There are {} urls to harvest - be patient...'.format(len(dataset_ids)))
        count = 0
        xml_list = []
        xml_base_url = self.canonical_base_url + '&view=xml'
        for dataset_id in dataset_ids:
            try:
                item_url = str(xml_base_url).format(dataset_id)
                content = requests.get(item_url)
            except exceptions.ConnectionError as e:
                logger.info('Connection error: {}, wait a bit...'.format(e))
                time.sleep(30)
                continue
            doc = etree.XML(content.content)

            record = etree.tostring(doc, encoding=self.DEFAULT_ENCODING)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.short_name,
                'docID': copy_to_unicode(dataset_id),
                'filetype': 'xml',
            }))
            count += 1
            if count % 100 == 0:
                logger.info('You\'ve requested {} studies, keep going!'.format(count))

        return xml_list
