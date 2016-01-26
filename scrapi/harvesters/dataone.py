"""API harvester for DataOne - for the SHARE project

Example query: https://cn.dataone.org/cn/v1/query/solr/?q=dateModified:[NOW-5DAY%20TO%20*]&rows=10
"""

from __future__ import unicode_literals

import logging
from datetime import timedelta, date

from lxml import etree
from functools import partial
from dateutil.parser import parse
from xml.etree import ElementTree

from nameparser import HumanName

from scrapi import requests
from scrapi import settings
from scrapi.base import helpers
from scrapi.base import XMLHarvester
from scrapi.util import copy_to_unicode
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import compose, single_result, build_properties, datetime_formatter

logger = logging.getLogger(__name__)

DEFAULT_ENCODING = 'UTF-8'
DATAONE_SOLR_ENDPOINT = 'https://cn.dataone.org/cn/v1/query/solr/'


def process_contributors(author, submitters, contributors,
                         investigators):
    if not author:
        author = ''
    elif isinstance(author, list):
        author = author[0]

    if not isinstance(contributors, list):
        contributors = [contributors]

    if not isinstance(investigators, list):
        investigators = [investigators]

    unique_contributors = list(set([author] + contributors + investigators))

    if len(unique_contributors) < 1:
        return []

    # this is the index of the author in the unique_contributors list
    if author != '':
        author_index = unique_contributors.index(author)
    else:
        author_index = None

    # grabs the email if there is one, this should go with the author index
    email = ''
    for submitter in submitters:
        if '@' in submitter:
            email = submitter

    contributor_list = []
    for index, contributor in enumerate(unique_contributors):
        if author_index is not None and index == author_index:
            # if contributor == NAME and email != '':
            #     # TODO - maybe add this back in someday
            #       sometimes this yields really weird names like mjg4
            #     # TODO - names not always perfectly lined up with emails...
            #     contributor = name_from_email(email)
            name = HumanName(contributor)
            contributor_dict = {
                'name': contributor,
                'givenName': name.first,
                'additionalName': name.middle,
                'familyName': name.last,
            }
            if email:
                contributor_dict['email'] = email
            contributor_list.append(contributor_dict)
        else:
            name = HumanName(contributor)
            contributor_list.append({
                'name': contributor,
                'givenName': name.first,
                'additionalName': name.middle,
                'familyName': name.last,
            })

    return contributor_list


class DataOneHarvester(XMLHarvester):
    short_name = 'dataone'
    long_name = 'DataONE: Data Observation Network for Earth'
    url = 'https://www.dataone.org/'

    namespaces = {}

    record_encoding = None

    schema = {
        'otherProperties': build_properties(
            ('authorGivenName', ("str[@name='authorGivenName']/node()")),
            ('authorSurName', ("str[@name='authorSurName']/node()")),
            ('authoritativeMN', ("str[@name='authoritativeMN']/node()")),
            ('checksum', ("str[@name='checksum']/node()")),
            ('checksumAlgorithm', ("str[@name='checksumAlgorithm']/node()")),
            ('datasource', ("str[@name='datasource']/node()")),
            ('datePublished', ("date[@name='datePublished']/node()")),
            ('dateUploaded', ("date[@name='dateUploaded']/node()")),
            ('pubDate', ("date[@name='pubDate']/node()")),
            ('updateDate', ("date[@name='updateDate']/node()")),
            ('fileID', ("str[@name='fileID']/node()")),
            ('formatId', ("str[@name='formatId']/node()")),
            ('formatType', ("str[@name='formatType']/node()")),
            ('identifier', ("str[@name='identifier']/node()")),
            ('readPermission', "arr[@name='readPermission']/str/node()"),
            ('replicaMN', "arr[@name='replicaMN']/str/node()"),
            ('replicaVerifiedDate', "arr[@name='replicaVerifiedDate']/date/node()"),
            ('replicationAllowed', ("bool[@name='replicationAllowed']/node()")),
            ('numberReplicas', ("int[@name='numberReplicas']/node()")),
            ('preferredReplicationMN', "arr[@name='preferredReplicationMN']/str/node()"),
            ('rightsHolder', ("str[@name='rightsHolder']/node()")),
            ('scientificName', "arr[@name='scientificName']/str/node()"),
            ('site', "arr[@name='site']/str/node()"),
            ('size', ("long[@name='size']/node()")),
            ('isDocumentedBy', "arr[@name='isDocumentedBy']/str/node()"),
            ('serviceID', "str[@name='id']/node()"),
            ('sku', "str[@name='sku']/node()")
        ),
        'freeToRead': {
            'startDate': ("bool[@name='isPublic']/node()", "date[@name='dateModified']/node()", lambda x, y: parse(y[0]).date().isoformat() if x else None)
        },
        'contributors': ("str[@name='author']/node()", "str[@name='submitter']/node()", "arr[@name='origin']/str/node()", "arr[@name='investigator']/str/node()", process_contributors),
        'uris': ("str[@name='id']/node()", "//str[@name='dataUrl']/node()", "arr[@name='resourceMap']/str/node()", partial(helpers.oai_process_uris, use_doi=True)),
        'tags': ("//arr[@name='keywords']/str/node()", lambda x: x if isinstance(x, list) else [x]),
        'providerUpdatedDateTime': ("str[@name='dateModified']/node()", compose(datetime_formatter, single_result)),
        'title': ("str[@name='title']/node()", single_result),
        'description': ("str[@name='abstract']/node()", single_result)
    }

    def harvest(self, start_date=None, end_date=None):

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        records = self.get_records(start_date, end_date)

        xml_list = []
        for record in records:
            # This ID is unique per data package, but won't unify multiple packages for the smae project
            doc_id = record.xpath("str[@name='id']")[0].text
            record = ElementTree.tostring(record, encoding=self.record_encoding)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.short_name,
                'docID': copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list

    def get_records(self, start_date, end_date):
        ''' helper function to get a response from the DataONE
        API, with the specified number of rows.
        Returns an etree element with results '''

        query = 'dateModified:[{}T00:00:00Z TO {}T00:00:00Z]'.format(start_date.isoformat(), end_date.isoformat())
        doc = requests.get(DATAONE_SOLR_ENDPOINT, params={
            'q': query,
            'start': 0,
            'rows': 1
        })
        doc = etree.XML(doc.content)
        rows = int(doc.xpath("//result/@numFound")[0])

        n = 0
        while n < rows:
            data = requests.get(DATAONE_SOLR_ENDPOINT, params={
                'q': query,
                'start': n,
                'rows': 1000
            })
            docs = etree.XML(data.content).xpath('//doc')
            for doc in docs:
                yield doc
            n += 1000
