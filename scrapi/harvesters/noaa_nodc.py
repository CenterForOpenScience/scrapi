'''Harvester of National Oceanic and Atmosphere Administration's National Oceanographic Data Center

Example API call: http://data.nodc.noaa.gov/cgi-bin/iso?id=gov.noaa.nodc:00001339;view=xml

NODC provides an OAI-PMH interface at
http://data.nodc.noaa.gov/cgi-bin/oai-pmh, but it does not contain the
contributors key mandated by the SHARE-schema spec.  Therefore, we
have to use the XML interface, which contains much more metadata.
However, the XML interface is not searchable, so we still use the
OAI-PMH interface for queries. Fun!

'''

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
from scrapi.base.helpers import (
    compose,
    single_result,
    datetime_formatter,
    language_codes,
    coerce_to_list,
    xml_text_only_list,
    xml_text_only
)
from scrapi.linter.document import RawDocument
from scrapi.util import copy_to_unicode

logger = logging.getLogger(__name__)


def filter_to_publishers(parties):
    '''Reduce list of ResponsibleParty elements to just publishers'''
    return filter_responsible_parties(parties, ['publisher'])


def filter_to_contributors(parties):
    '''Reduce list of ResponsibleParty elements to just contributors'''
    return filter_responsible_parties(parties, ['resourceProvider'])


def filter_responsible_parties(parties, roles):
    '''Return list of ResponsibleParties whose role is in roles param'''
    return [party for party in parties if party.xpath(
        './gmd:role/gmd:CI_RoleCode/node()', namespaces=party.nsmap
    )[0] in roles]


def parse_contributors(parties):
    '''Turn list of gmd:CI_ResponsibleParty elements into
    SHARE-compliant contributors list
    '''

    # The NODC schema doesn't explicitly distinguish between
    # organizations and individuals.  Every party has an
    # organizationName tag, individuals have an individualName tag as well.
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

        # the email address is tied to the party, so wait until we know
        # who the contributor is before extracting
        email = party.xpath('./gmd:contactInfo/gmd:CI_Contact/gmd:address/gmd:CI_Address/gmd:electronicMailAddress', namespaces=party.nsmap)
        if email:
            contributor['email'] = xml_text_only(email[0])
        contributors.append(contributor)

    return contributors


def extract_individual(party):
    '''Return SHARE-compliant person object from a CI_ResponsibleParty element'''
    individual = party.xpath('./gmd:individualName/gmx:Anchor', namespaces=party.nsmap)
    if individual:
        name = HumanName(individual[0].text)
        ns_prefix = party.nsmap['xlink']
        sameAs = individual[0].get('{' + ns_prefix + '}href')

        individual = {
            'name': individual[0].text,
            'givenName': name.first,
            'additionalName': name.middle,
            'familyName': name.last
        }

        if sameAs:
            individual['sameAs'] = [sameAs]

        return individual


def extract_organization(party):
    '''Return SHARE-compliant organization object from a CI_ResponsibleParty element'''
    organization = party.xpath('./gmd:organisationName/gmx:Anchor', namespaces=party.nsmap)
    if organization:
        ns_prefix = party.nsmap['xlink']
        sameAs = organization[0].get('{' + ns_prefix + '}href')

        organization = {
            'name': organization[0].text
        }

        if sameAs:
            organization['smaeAs'] = sameAs

        return organization


def filter_keywords(keyword_groups):
    '''Filter out the NODC org name from the keywords to reduce noise
    and flatten nested lists'''
    keywords = []
    for keyword_group in keyword_groups:
        keyword_type = keyword_group.xpath('./gmd:type/gmd:MD_KeywordTypeCode/node()', namespaces=keyword_group.nsmap)
        if keyword_type and keyword_type[0] != 'datacenter':
            keywords.extend(keyword_group.xpath('./gmd:keyword/gmx:Anchor/node()', namespaces=keyword_group.nsmap))
    return keywords


class NODCHarvester(XMLHarvester):
    short_name = 'noaa_nodc'
    long_name = 'National Oceanographic Data Center'
    url = 'https://www.nodc.noaa.gov/'

    DEFAULT_ENCODING = 'UTF-8'
    record_encoding = None

    canonical_base_url = 'http://data.nodc.noaa.gov/cgi-bin/iso?id={}'

    search_base_url = 'http://data.nodc.noaa.gov/cgi-bin/oai-pmh?verb=ListRecords'
    oai_ns = {
        'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
        'dc': 'http://purl.org/dc/elements/1.1/',
    }

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
        'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
    }

    @property
    def schema(self):
        id_stanza = './gmd:identificationInfo/gmd:MD_DataIdentification/'
        cite_stanza = id_stanza + 'gmd:citation/gmd:CI_Citation/'
        return {
            'title': (cite_stanza + 'gmd:title', compose(xml_text_only, single_result)),
            'description': (id_stanza + 'gmd:abstract', compose(xml_text_only, single_result)),
            'contributors': (cite_stanza + 'gmd:citedResponsibleParty/gmd:CI_ResponsibleParty', compose(parse_contributors, filter_to_contributors)),
            'uris': {
                'canonicalUri': (
                    './gmd:fileIdentifier',
                    compose(lambda x: str(self.canonical_base_url).format(x), xml_text_only, single_result)
                ),
            },
            'publisher': (
                cite_stanza + 'gmd:citedResponsibleParty/gmd:CI_ResponsibleParty',
                compose(extract_organization, single_result, filter_to_publishers),
            ),
            'providerUpdatedDateTime': ('./gmd:dateStamp/gco:DateTime/node()', compose(datetime_formatter, single_result)),
            'languages': ('./gmd:language/gmd:LanguageCode', compose(language_codes, xml_text_only_list, coerce_to_list)),
            'subjects': (id_stanza + 'gmd:descriptiveKeywords/gmd:MD_Keywords', lambda x: filter_keywords(x)),
        }

    def query_by_date(self, start_date, end_date):
        '''Use OAI-PMH interface to get a list of dataset ids for the given date range'''
        search_url_end = '&metadataPrefix=oai_dc&from={}&until={}'.format(start_date, end_date)
        search_url = self.search_base_url + search_url_end

        while True:
            record_list = requests.get(search_url)
            record_list_xml = etree.XML(record_list.content)
            if record_list_xml.xpath('./oai_dc:error', namespaces=self.oai_ns):
                break

            for dataset in record_list_xml.xpath('./oai_dc:ListRecords/oai_dc:record', namespaces=self.oai_ns):
                yield dataset.xpath('./oai_dc:header/oai_dc:identifier/node()', namespaces=self.oai_ns)[0]

            token = record_list_xml.xpath('./oai_dc:ListRecords/oai_dc:resumptionToken/node()', namespaces=self.oai_ns)
            if not token:
                break

            search_url = self.search_base_url + '&resumptionToken=' + token[0]

    def harvest(self, start_date=None, end_date=None):
        ''' First, get a list of all recently updated study urls,
        then get the xml one by one and save it into a list
        of docs including other information '''

        start_date = (start_date or date.today() - timedelta(settings.DAYS_BACK)).isoformat()
        end_date = (end_date or date.today()).isoformat()
        start_date += 'T00:00:00Z'
        end_date += 'T00:00:00Z'

        # grab each of those urls for full content
        xml_list = []
        xml_base_url = self.canonical_base_url + '&view=xml'
        for dataset_id in self.query_by_date(start_date, end_date):
            try:
                item_url = str(xml_base_url).format(dataset_id)
                content = requests.get(item_url, throttle=2)
            except exceptions.ConnectionError as e:
                logger.info('Connection error: {}, wait a bit...'.format(e))
                time.sleep(30)
                content = requests.get(item_url)
            doc = etree.XML(content.content)

            record = etree.tostring(doc, encoding=self.DEFAULT_ENCODING)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.short_name,
                'docID': copy_to_unicode(dataset_id),
                'filetype': 'xml',
            }))

        return xml_list
