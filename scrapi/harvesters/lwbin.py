"""
A Lake Winnipeg Basin Information Network (BIN) harvester for the SHARE project

Example API request: http://130.179.67.140/api/3/action/package_search?q=

"""

from __future__ import unicode_literals

import json
import logging

from datetime import date, timedelta

from six.moves import xrange
from nameparser import HumanName
from dateutil.parser import parse

from scrapi import requests
from scrapi import settings
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, compose

logger = logging.getLogger(__name__)


def process_contributors(authors):

    authors = authors.strip().replace('<span class="author-names">', '').replace('</span>', '')
    authors = authors.split(',')

    if ' and ' in authors[-1] or ' <em>et al.</em>' in authors[-1]:
        split_name = authors.pop(-1).replace(' <em>et al.</em>', '').split(' and ')
        authors.extend(split_name)

    contributor_list = []
    for person in authors:
        name = HumanName(person)
        contributor = {
            'name': person,
            'givenName': name.first,
            'additionalName': name.middle,
            'familyName': name.last,
            'email': '',
            'sameAs': [],
        }
        contributor_list.append(contributor)

    return contributor_list


class LWBINHarvester(JSONHarvester):
    short_name = 'lwbin'
    long_name = 'Lake Winnipeg Basin Information Network'
    url = 'http://130.179.67.140'

    DEFAULT_ENCODING = 'UTF-8'

    record_encoding = None

    @property
    def schema(self):
        return {
            'title': ('/title', lambda x: x[0] if x else ''),
            'description': ('/notes', lambda x: x[0] if (isinstance(x, list) and x) else x or ''),
            'providerUpdatedDateTime': ('/metadata_modified', lambda x: parse(x).isoformat()),
            'uris': {
                'canonicalUri': '/url'
            },
            'contributors': ('/author', process_contributors),
            'otherProperties': build_properties(
                ('tags', ('/tags', lambda x: [tag['name'].lower() for tag in (x or [])])),
                ('licenseTitle', '/license_title'),
                ('maintainer', '/maintainer'),
                ('isPrivate', '/private'),
                ('maintainerEmail', '/maintainer_email'),
                ('revisionTimestamp', ('/revision_timestamp', lambda x: parse(x).isoformat())),
                ('id', '/id'),
                ('metadataCreated', ('/metadata_created', lambda x: parse(x).isoformat())),
                ('authorEmail', '/author_email'),
                ('state', '/state'),
                ('version', '/version'),
                ('creatorUserId', '/creator_user_id'),
                ('type', '/type'),
                ('licenseId', '/license_id'),
                ('numberOfResources', '/num_resources'),
                ('numberOfTags', '/num_tags'),
                ('isOpen', '/isopen')
                # ('DOI', '/extras/') how to extract value by key within a property?
                # organization
            )
        }

    def harvest(self, start_date=None, end_date=None):
        """
        start_date and end_date are not supported by LWBIN CKAN API. all datasets have to be scanned each time.
        :returns: a list of documents (metadata)
        """

        base_url = 'http://130.179.67.140/api/3/action/package_search?q='
        total = requests.get(base_url).json()['result']['count']
        logger.info('{} documents to be harvested'.format(total))

        doc_list = []
        records = requests.get(base_url).json()['result']['results']
        names = ",/n".join([records[i]['title'] for i in xrange(0,len(records))])
        logger.info(names)
        logger.info('Harvested {} documents'.format(len(records)))

        for record in records:
            doc_id = record['id']
            doc_list.append(RawDocument({
                'doc': json.dumps(record),
                'source': self.short_name,
                'docID': doc_id,
                'filetype': 'json'
            }))

        return doc_list
