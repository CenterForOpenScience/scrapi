"""
API harvester for ClinicalTrials.gov for the SHARE Notification Service

http://clinicaltrials.gov/ct2/results?lup_s=04%2F26%2F2015%2F&lup_e=04%2F27%2F2015&displayxml=true
iindividual result: http://ClinicalTrials.gov/show/NCT02425332?displayxml=true

"""

from __future__ import unicode_literals

import time
import logging
from datetime import date, timedelta

import xmltodict
from lxml import etree

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.util import copy_to_unicode
from scrapi.linter.document import RawDocument
from scrapi.base.schemas import default_name_parser
from scrapi.base.helpers import compose, single_result, build_properties, datetime_formatter

logger = logging.getLogger(__name__)


element_to_dict = compose(xmltodict.parse, etree.tostring)


def non_string(item):
    return not isinstance(item, str)


class ClinicalTrialsHarvester(XMLHarvester):

    short_name = 'clinicaltrials'
    long_name = 'ClinicalTrials.gov'
    url = 'https://clinicaltrials.gov/'

    DEFAULT_ENCODING = 'UTF-8'
    record_encoding = None

    # TODO - clinicaltrials elements have a lot of extra metadata - at some
    # point in the future we should do a more thorough audit.
    schema = {
        "contributors": ('//overall_official/last_name/node()', default_name_parser),
        "uris": {
            "canonicalUri": ("//required_header/url/node()", single_result)
        },
        "providerUpdatedDateTime": ("lastchanged_date/node()", compose(datetime_formatter, single_result)),
        "title": ('//official_title/node()', '//brief_title/node()', lambda x, y: single_result(x) or single_result(y)),
        "description": ('//brief_summary/textblock/node()', '//brief_summary/textblock/node()', lambda x, y: single_result(x) or single_result(y)),
        "tags": ("//keyword/node()", lambda tags: [tag.lower() for tag in tags]),
        "sponsorships": [
            {
                "sponsor": {
                    "sponsorName": ("//sponsors/lead_sponsor/agency/node()", single_result)
                }
            },
            {
                "sponsor": {
                    "sponsorName": ("//sponsors/collaborator/agency/node()", single_result)
                }
            }
        ],
        "otherProperties": build_properties(
            ("serviceID", "//nct_id/node()"),
            ('oversightAuthority', '//oversight_info/authority/node()'),
            ('studyDesign', '//study_design/node()'),
            ('numberOfArms', '//number_of_arms/node()'),
            ('source', '//source/node()'),
            ('verificationDate', '//verification_date/node()'),
            ('lastChanged', '//lastchanged_date/node()'),
            ('condition', '//condition/node()'),
            ('verificationDate', '//verification_date/node()'),
            ('lastChanged', '//lastchanged_date/node()'),
            ('status', '//status/node()'),
            ('locationCountries', '//location_countries/country/node()'),
            ('isFDARegulated', '//is_fda_regulated/node()'),
            ('isSection801', '//is_section_801/node()'),
            ('hasExpandedAccess', '//has_expanded_access/node()'),
            ('leadSponsorAgencyClass', '//lead_sponsor/agency_class/node()'),
            ('collaborator', '//collaborator/agency/node()'),
            ('collaboratorAgencyClass', '//collaborator/agency_class/node()'),
            ('measure', '//primary_outcome/measure/node()'),
            ('timeFrame', '//primary_outcome/time_frame/node()'),
            ('safetyIssue', '//primary_outcome/safety_issue/node()'),
            ('secondaryOutcomes', '//secondary_outcome/measure/node()'),
            ('enrollment', '//enrollment/node()'),
            ('armGroup', '//arm_group/arm_group_label/node()'),
            ('intervention', '//intervention/intervention_type/node()'),
            ('eligibility', ('//eligibility/node()', compose(
                lambda x: map(element_to_dict, x),
                lambda x: filter(non_string, x)
            ))),
            ('link', '//link/url/node()'),
            ('responsible_party', '//responsible_party/responsible_party_full_name/node()')
        )
    }

    @property
    def namespaces(self):
        return None

    def harvest(self, start_date=None, end_date=None):
        """ First, get a list of all recently updated study urls,
        then get the xml one by one and save it into a list
        of docs including other information """

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        end_month = end_date.strftime('%m')
        end_day = end_date.strftime('%d')
        end_year = end_date.strftime('%Y')

        start_month = start_date.strftime('%m')
        start_day = start_date.strftime('%d')
        start_year = start_date.strftime('%Y')

        base_url = 'http://clinicaltrials.gov/ct2/results?lup_s='
        url_end = '{}%2F{}%2F{}&lup_e={}%2F{}%2F{}&displayxml=true'.\
            format(start_month, start_day, start_year, end_month, end_day, end_year)

        url = base_url + url_end

        # grab the total number of studies
        initial_request = requests.get(url)
        record_encoding = initial_request.encoding
        initial_request_xml = etree.XML(initial_request.content)
        count = int(initial_request_xml.xpath('//search_results/@count')[0])
        xml_list = []
        if int(count) > 0:
            # get a new url with all results in it
            url = url + '&count=' + str(count)
            total_requests = requests.get(url)
            initial_doc = etree.XML(total_requests.content)

            # make a list of urls from that full list of studies
            study_urls = []
            for study in initial_doc.xpath('//clinical_study'):
                study_urls.append(study.xpath('url/node()')[0] + '?displayxml=true')

            # grab each of those urls for full content
            logger.info("There are {} urls to harvest - be patient...".format(len(study_urls)))
            count = 0
            official_count = 0
            for study_url in study_urls:
                try:
                    content = requests.get(study_url)
                except requests.exceptions.ConnectionError as e:
                    logger.info('Connection error: {}, wait a bit...'.format(e))
                    time.sleep(30)
                    continue
                doc = etree.XML(content.content)
                record = etree.tostring(doc, encoding=record_encoding)
                doc_id = doc.xpath('//nct_id/node()')[0]
                xml_list.append(RawDocument({
                    'doc': record,
                    'source': self.short_name,
                    'docID': copy_to_unicode(doc_id),
                    'filetype': 'xml',
                }))
                official_count += 1
                count += 1
                if count % 100 == 0:
                    logger.info("You've requested {} studies, keep going!".format(official_count))
                    count = 0

        return xml_list
