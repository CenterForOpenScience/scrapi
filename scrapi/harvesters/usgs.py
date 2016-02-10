from __future__ import unicode_literals

import json
import logging
from datetime import date, timedelta

import six
from nameparser import HumanName
from scrapi import requests, settings
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, datetime_formatter

logger = logging.getLogger(__name__)


def process_contributors(authors):
    all_processed_authors = []

    for author in authors:
        author_d = {}
        author_d['name'] = author['text']
        name = HumanName(
            author['text']
        )
        author_d['additionalName'] = name.middle
        author_d['givenName'] = name.first
        author_d['familyName'] = name.last
    return all_processed_authors


class USGSHarvester(JSONHarvester):
    short_name = 'usgs'
    long_name = 'United States Geological Survey'
    url = 'https://pubs.er.usgs.gov/'
    DEFAULT_ENCODING = 'UTF-8'

    URL = 'https://pubs.er.usgs.gov/pubs-services/publication?'

    schema = {
        'title': '/title',
        'description': '/docAbstract',
        'providerUpdatedDateTime': ('/lastModifiedDate', datetime_formatter),
        'uris': {
            'canonicalUri': ('/id', 'https://pubs.er.usgs.gov/publication/{}'.format),
            'providerUris': [('/id', 'https://pubs.er.usgs.gov/publication/{}'.format)],
            'descriptorUris': [('/doi', 'https://dx.doi.org/{}'.format)]
        },

        'contributors': ('/contributors/authors', process_contributors),
        'otherProperties': build_properties(
            ('serviceID', ('/id', str)),
            ('definedType', '/defined_type'),
            ('type', '/type'),
            ('links', '/links'),
            ('publisher', '/publisher'),
            ('publishedDate', '/displayToPublicDate'),
            ('publicationYear', '/publicationYear'),
            ('issue', '/issue'),
            ('volume', '/volume'),
            ('language', '/language'),
            ('indexId', '/indexId'),
            ('publicationSubtype', '/publicationSubtype'),
            ('startPage', '/startPage'),
            ('endPage', '/endPage'),
            ('onlineOnly', '/onlineOnly'),
            ('additionalOnlineFiles', '/additionalOnlineFiles'),
            ('country', '/country'),
            ('state', '/state'),
            ('ipdsId', '/ipdsId'),
            ('links', '/links'),
            ('doi', '/doi'),
            ('contributors', '/contributors'),
            ('otherGeospatial', '/otherGeospatial'),
            ('geographicExtents', '/geographicExtents'),

        )
    }

    def harvest(self, start_date=None, end_date=None):

        # This API does not support date ranges
        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)

        # days_back = the number of days between start_date and now, defaulting to settings.DAYS_BACK
        days_back = settings.DAYS_BACK
        search_url = '{0}mod_x_days={1}'.format(
            self.URL,
            days_back
        )

        record_list = []
        for record in self.get_records(search_url):
            doc_id = record['id']

            record_list.append(
                RawDocument(
                    {
                        'doc': json.dumps(record),
                        'source': self.short_name,
                        'docID': six.text_type(doc_id),
                        'filetype': 'json'
                    }
                )
            )

        return record_list

    def get_records(self, search_url):
        records = requests.get(search_url)
        total_records = records.json()['recordCount']
        logger.info('Harvesting {} records'.format(total_records))
        page_number = 1
        count = 0

        while records.json()['records']:
            record_list = records.json()['records']
            for record in record_list:
                count += 1
                yield record

            page_number += 1
            records = requests.get(search_url + '&page_number={}'.format(page_number), throttle=3)
            logger.info('{} documents harvested'.format(count))
