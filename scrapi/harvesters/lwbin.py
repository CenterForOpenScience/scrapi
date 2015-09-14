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
2 -- Resouces properties contained in raw metadata of the datasets are not added to the normalized metadata at this
point.
3 -- Single name contributors can be used as filters or an invalid query will be returned. Has nothing to do with scrapi but the frontend.
"""

from __future__ import unicode_literals

import json
import logging

from nameparser import HumanName
from dateutil.parser import parse

from scrapi import requests
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, date_formatter


logger = logging.getLogger(__name__)

ORGANIZATIONS = (
    "organization", "Organization", "fund", "Fund", "canada", "Canada", "agriculture", "Agriculture", "commitee",
    "Commitee", "international", "International", "council", "Council", "office", "Office", "of", "Of",
    "observation", "Observation", "LWBIN", "CoCoRaHS", "USGS", "NSIDC"
)


def is_organization(name):
    """Return a boolean to indicate if the name passed to the function is an organization
    """
    words = name.split(' ')
    return any(word.strip(";") in ORGANIZATIONS for word in words)


def process_contributors(authors, emails):
    """Process authors and add author emails
    If multiple authors and one email, put email in a new author
    """

    authors = authors.strip().replace('<span class="author-names">', '').replace('</span>', '')
    authors = authors.split(',')
    emails = emails.split(',')

    for author in authors:
        if is_organization(author):
            break
    else:
        if ' and ' in authors[-1] or ' <em>et al.</em>' in authors[-1]:
            split_name = authors.pop(-1).replace(' <em>et al.</em>', '').split(' and ')
            authors.extend(split_name)

    contributor_list = []
    append_emails = True
    if len(authors) != 1 or len(emails) != 1 or emails[0] == u'':
        append_emails = False  # append the email to the author only when 1 record is observed

    for ind, person in enumerate(authors):
        name = HumanName(person)
        contributor = {
            'name': person,
            'givenName': name.first,
            'additionalName': name.middle,
            'familyName': name.last,
            'sameAs': []
        }
        if append_emails:
            contributor['email'] = emails[ind]
        contributor_list.append(contributor)

    if not append_emails and emails[0] != u'':
        for email in emails:
            contributor = {
                'name': '',
                'givenName': '',
                'additionalName': '',
                'familyName': '',
                'email': email,
                'sameAs': [],
            }
            contributor_list.append(contributor)

    return contributor_list


def process_licenses(license_title, license_url, license_id):
    """Process licenses to comply with the noormalized schema
    """

    if not license_url:
        return []
    else:
        license = {
            'uri': license_url,
            'description': "{} ({})".format(license_title, license_id) or ""
        }

        return [license]


def construct_url(url, dataset_path, end_point):
    """Return a url that directs back to the page on LBWIN Data Hub instead of the source page.

    Keyword arguments:
    url -- host url
    dataset_path -- parent path of all datasets
    end_point -- name of datasets
    """

    return "/".join([url, dataset_path, end_point])


def process_object_uris(url, extras):
    """Extract doi from /extras, and return a list or object uris including /url and doi if it exists.
    """
    doi = ""
    for d in extras:
        if d['key'] == "DOI" or d['key'] == "DOI:":
            doi = d['value']
            break
    if doi == "":
        return [url]
    else:
        return [url, doi]


class LWBINHarvester(JSONHarvester):
    short_name = 'lwbin'
    long_name = 'Lake Winnipeg Basin Information Network'
    url = 'http://130.179.67.140'
    dataset_path = "dataset"   # dataset base url for constructing urls that go back to LWBIN instead of source pages.

    DEFAULT_ENCODING = 'UTF-8'

    record_encoding = None

    @property
    def schema(self):
        return {
            'title': ('/title', lambda x: x if x else ''),
            'description': ('/notes', lambda x: x[0] if (isinstance(x, list) and x) else x or ''),
            'providerUpdatedDateTime': ('/metadata_modified', date_formatter),
            'uris': {
                'canonicalUri': ('/name', lambda x: construct_url(self.url, self.dataset_path, x)),  # Construct new urls directing to LWBIN
                'objectUris': ('/url', '/extras', process_object_uris)  # Default urls from the metadata directing to source pages
            },
            'contributors': ('/author', '/author_email', process_contributors),
            'licenses': ('/license_title', '/license_url', '/license_id', process_licenses),
            'tags': ('/tags', lambda x: [tag['name'].lower() for tag in (x or [])]),
            'freeToRead': {
                'startDate': ('/isopen', '/metadata_created', lambda x, y: parse(y).date().isoformat() if x else None)
            },
            'otherProperties': build_properties(
                ('maintainer', '/maintainer'),
                ('maintainerEmail', '/maintainer_email'),
                ('revisionTimestamp', ('/revision_timestamp', date_formatter)),
                ('id', '/id'),
                ('metadataCreated', ('/metadata_created', date_formatter)),
                ('state', '/state'),
                ('version', '/version'),
                ('creatorUserId', '/creator_user_id'),
                ('type', '/type'),
                ('numberOfResources', '/num_resources'),
                ('numberOfTags', '/num_tags'),
                ('name', '/name'),
                ('groups', '/groups'),
            )
        }

    def harvest(self, start_date=None, end_date=None):
        """Returns a list of Rawdocuments (metadata)
        Searching by time is not supported by LWBIN CKAN API. all datasets have to be scanned each time.
        """

        base_url = 'http://130.179.67.140/api/3/action/current_package_list_with_resources'

        doc_list = []
        records = requests.get(base_url).json()['result']
        total = len(records)  # Total number of documents
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
