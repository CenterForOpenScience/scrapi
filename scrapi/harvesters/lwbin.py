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

from dateutil.parser import parse

from scrapi import requests
from scrapi.base import JSONHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, datetime_formatter, parse_name, single_result


logger = logging.getLogger(__name__)

ORGANIZATIONS = (
    "organization", "fund", "canada", "agriculture", "commitee", "international", "council", "office", "of",
    "observation", "institute", "lwbin", "cocorahs", "usgs", "nsidc"
)


def is_organization(name):
    """Return a boolean to indicate if the name passed to the function is an organization
    """
    words = name.split(' ')
    return any(word.strip(";").lower() in ORGANIZATIONS for word in words)


def clean_authors(authors):
    """Cleam authors list.
    """
    authors = authors.strip().replace('<span class="author-names">', '').replace('</span>', '')
    authors = authors.split(',')

    new_authors = []
    for author in authors:
        if is_organization(author):
            new_authors.append(author)
        else:
            if ' and ' in author or ' <em>et al.</em>' in author:
                split_name = author.replace(' <em>et al.</em>', '').split(' and ')
                new_authors.extend(split_name)
            else:
                new_authors.append(author)
    return new_authors


def process_contributors(authors, emails):
    """Process authors and add author emails
    If multiple authors and one email, put email in a new author
    """
    emails = emails.split(',')
    authors = clean_authors(authors)
    contributor_list = []
    append_emails = len(authors) == 1 and len(emails) == 1 and not emails[0] == u''  # append the email to the author only when 1 record is observed

    for i, author in enumerate(authors):
        if is_organization(author):
            contributor = {
                'name': author
            }
        else:
            contributor = parse_name(author)

        if append_emails:
            contributor['email'] = emails[i]
        contributor_list.append(contributor)

    if not append_emails and emails[0] != u'':
        for email in emails:
            contributor = {
                'name': '',
                'email': email
            }
            contributor_list.append(contributor)

    return contributor_list


def process_licenses(license_title, license_url, license_id):
    """Process licenses to comply with the normalized schema
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
    """
    :return: a url that directs back to the page on LBWIN Data Hub instead of the source page.
    :param url: host url
    :param dataset_path: parent path of all datasets
    :param end_point: name of datasets
    """

    return "/".join([url, dataset_path, end_point])


def process_object_uris(url, extras):
    """Extract doi from /extras, and return a list of object uris including /url and doi if it exists.
    """
    doi = []
    for d in extras:
        if d['key'] == "DOI" or d['key'] == "DOI:":
            doi.append(d['value'])
    if doi == []:
        return [url]
    else:
        return [url].extend(doi)


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
            'title': ('/title', lambda x: x or ''),
            'description': ('/notes', lambda x: single_result(x, x) or ''),
            'providerUpdatedDateTime': ('/metadata_modified', datetime_formatter),
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
                ('revisionTimestamp', ('/revision_timestamp', datetime_formatter)),
                ('id', '/id'),
                ('metadataCreated', ('/metadata_created', datetime_formatter)),
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

        records = requests.get(base_url).json()['result']
        total = len(records)  # Total number of documents
        logger.info('{} documents to be harvested'.format(total))

        return [
            RawDocument({
                'doc': json.dumps(record),
                'source': self.short_name,
                'docID': record['id'],
                'filetype': 'json'
            }) for record in records
        ]
