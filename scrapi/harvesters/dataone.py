"""API harvester for DataOne - for the SHARE project

Example query: https://cn.dataone.org/cn/v1/query/solr/?q=dateModified:[NOW-5DAY%20TO%20*]&rows=10
"""


## harvester for DataONE SOLR search API
from __future__ import unicode_literals

import re

import logging
from datetime import datetime
from datetime import timedelta

from lxml import etree
from dateutil.parser import *
from xml.etree import ElementTree

from nameparser import HumanName

from scrapi import requests
from scrapi.base import XMLHarvester
from scrapi.linter.document import RawDocument

logger = logging.getLogger(__name__)

DEFAULT_ENCODING = 'UTF-8'
DATAONE_SOLR_ENDPOINT = 'https://cn.dataone.org/cn/v1/query/solr/'


def process_doi(service_id, doc_doi):
    doi_re = '10\\.\\d{4}/\\w*\\.\\w*(/\\w*)?'

    doi_list = map(lambda x: x.replace('doi', ''), doc_doi) if isinstance(doc_doi, list) else [doc_doi.replace('doi', '')]

    for item in [service_id] + doi_list:
        try:
            return re.search(doi_re, item).group(0)
        except AttributeError:
            continue
    return ''


def process_contributors(author, submitters, contributors):
    if not isinstance(contributors, list):
        contributors = [contributors]
    unique_contributors = list(set([author] + contributors))

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
                'email': unicode(email)
            }
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
        'otherProperties': {
            'author': "str[@name='author']/node()",
            'authorGivenName': ("str[@name='authorGivenName']/node()"),
            'authorSurName': ("str[@name='authorSurName']/node()"),
            'authoritativeMN': ("str[@name='authoritativeMN']/node()"),
            'checksum': ("str[@name='checksum']/node()"),
            'checksumAlgorithm': ("str[@name='checksumAlgorithm']/node()"),
            'dataUrl': ("str[@name='dataUrl']/node()"),
            'datasource': ("str[@name='datasource']/node()"),
            'documents': "arr[@name='documents']/str/node()",
            'dateModified': ("date[@name='dateModified']/node()"),
            'datePublished': ("date[@name='datePublished']/node()"),
            'dateUploaded': ("date[@name='dateUploaded']/node()"),
            'pubDate': ("date[@name='pubDate']/node()"),
            'updateDate': ("date[@name='updateDate']/node()"),
            'fileID': ("str[@name='fileID']/node()"),
            'formatId': ("str[@name='formatId']/node()"),
            'formatType': ("str[@name='formatType']/node()"),
            'identifier': ("str[@name='identifier']/node()"),
            'investigator': "arr[@name='investigator']/str/node()",
            'origin': "arr[@name='origin']/str/node()",
            'isPublic': ("bool[@name='isPublic']/node()"),
            'readPermission': "arr[@name='readPermission']/str/node()",
            'replicaMN': "arr[@name='replicaMN']/str/node()",
            'replicaVerifiedDate': "arr[@name='replicaVerifiedDate']/date/node()",
            'replicationAllowed': ("bool[@name='replicationAllowed']/node()"),
            'numberReplicas': ("int[@name='numberReplicas']/node()"),
            'preferredReplicationMN': "arr[@name='preferredReplicationMN']/str/node()",
            'resourceMap': "arr[@name='resourceMap']/str/node()",
            'rightsHolder': ("str[@name='rightsHolder']/node()"),
            'scientificName': "arr[@name='scientificName']/str/node()",
            'site': "arr[@name='site']/str/node()",
            'size': ("long[@name='size']/node()"),
            'sku': ("str[@name='sku']/node()"),
            'isDocumentedBy': "arr[@name='isDocumentedBy']/str/node()",
            'serviceID': "str[@name='id']/node()"
        },
        'contributor': ("str[@name='author']/node()", "str[@name='submitter']/node()", "arr[@name='origin']/str/node()", process_contributors),
        'notificationLink': ("str[@name='id']/node()", "//str[@name='dataUrl']/node()", lambda x, y: y if 'http' in y else x if 'http' in x else ''),
        'directLink': ("str[@name='id']/node()", "//str[@name='dataUrl']/node()", lambda x, y: y if 'http' in y else x if 'http' in x else ''),
        'resourceIdentifier': ("str[@name='id']/node()", "//str[@name='dataUrl']/node()", lambda x, y: y if 'http' in y else x if 'http' in x else ''),
        'tags': ("//arr[@name='keywords']/str/node()", lambda x: x if isinstance(x, list) else [x]),
        'releaseDate': ("str[@name='dateModified']/node()", lambda x: parse(x).date().isoformat().decode('utf-8')),
        'title': "str[@name='title']/node()",
        'description': "str[@name='abstract']/node()",
        'relation': ("str[@name='id']/node()", "arr[@name='isDocumentedBy']/str/node()", lambda x, y: [process_doi(x, y)]),
    }

    def copy_to_unicode(self, element):
        encoding = self.record_encoding or DEFAULT_ENCODING
        element = ''.join(element)
        if isinstance(element, unicode):
            return element
        else:
            return unicode(element, encoding=encoding)

    def harvest(self, days_back=1):
        records = self.get_records(days_back)

        xml_list = []
        for record in records:
            doc_id = record.xpath("str[@name='id']")[0].text
            record = ElementTree.tostring(record, encoding=self.record_encoding)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.short_name,
                'docID': self.copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list

    def get_records(self, days_back):
        ''' helper function to get a response from the DataONE
        API, with the specified number of rows.
        Returns an etree element with results '''
        to_date = datetime.utcnow()
        from_date = (datetime.utcnow() - timedelta(days=days_back))

        to_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0)
        from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)

        query = 'dateModified:[{}Z TO {}Z]'.format(from_date.isoformat(), to_date.isoformat())
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
