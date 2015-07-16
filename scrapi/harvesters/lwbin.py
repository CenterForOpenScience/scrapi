"""
A Lake Winnipeg Basin Information Network (BIN) harvester for the SHARE project

Example API request: http://130.179.67.140/api/3/action/package_search?q= (problematic)
http://130.179.67.140/api/3/action/current_package_list_with_resources (currently using) 
It oddly returns 5 more datasets than all searchable ones on LWBIN data hub.

Known issues:
1 -- Five datasets can be searched but cannot be accessed via LWBIN. 
Clicking on the searching result would result in linking to a redirected page like this:
http://130.179.67.140/user/login?came_from=http://130.179.67.140/dataset/mpca-surface-water-data-access-interactive-map
Within each dataset there are resouces that contain urls to source pages. For future work considering using resources 
urls as canonical urls. 
2 -- Resouces properties contained in raw metadata of the datasets are not added to the normalized metadata at this point. 
For future work.
3 -- Lots of DOIs were missing in the original data. Excluded them at current point.
4 -- Author emails are stored in otherProperties instead of contributors.
5 -- Single name contributors can be used as filters or an invalid query will be returned. Has nothing to do with scrapi but the frontend.
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

from urlparse import urljoin


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

def construct_url(url, dataset_path, end_point):
    """Return a url that directs back to the page on LBWIN Data Hub instead of the source page.
    
    Keyword arguments:
    url -- host url
    dataset_path -- parent path of all datasets
    end_point -- name of datasets
    """

    return urljoin(url, "/".join([dataset_path, end_point]))


class LWBINHarvester(JSONHarvester):
    short_name = 'lwbin'
    long_name = 'Lake Winnipeg Basin Information Network'
    url = 'http://130.179.67.140'
    dataset_path =  "dataset"   # dataset base url for constructing urls that go back to LWBIN instead of source pages. 
    
    DEFAULT_ENCODING = 'UTF-8'

    record_encoding = None

    @property
    def schema(self):
        return {
            'title': ('/title', lambda x: x if x else ''),
            'description': ('/notes', lambda x: x[0] if (isinstance(x, list) and x) else x or ''),
            'providerUpdatedDateTime': ('/metadata_modified', lambda x: parse(x).isoformat()),
            'uris': {
                'canonicalUri': ('/name', lambda x: construct_url(self.url, self.dataset_path, x)), # Construct new urls directing to LWBIN
                'objectUris':'/url' # Default urls from the metadata directing to source pages
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
                ('isOpen', '/isopen'),
                ('name', '/name')
            )
        }

    def harvest(self):
        """Returns a list of Rawdocuments (metadata)
        
        Searching by time is not supported by LWBIN CKAN API. all datasets have to be scanned each time.
        """

        base_url = 'http://130.179.67.140/api/3/action/current_package_list_with_resources'

        doc_list = []
        records = requests.get(base_url).json()['result']
        total = len(records) # Total number of documents
        logger.info('{} documents to be harvested'.format(total))

        for record in records:
            doc_id = record['id']
            doc_list.append(RawDocument({
                'doc': json.dumps(record),
                'source': self.short_name,
                'docID': doc_id,
                'filetype': 'json'
            }))

        return doc_list
