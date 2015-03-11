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
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

logger = logging.getLogger(__name__)

NAME = "dataone"

DEFAULT_ENCODING = 'UTF-8'
DATAONE_SOLR_ENDPOINT = 'https://cn.dataone.org/cn/v1/query/solr/'

record_encoding = None


def copy_to_unicode(element):
    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def harvest(days_back=5):
    doc = get_response(1, days_back)[0]
    rows = int(doc.xpath("//result/@numFound")[0])
    responses = get_response(rows, days_back)
    records = []
    for response in responses:
        records.extend(response.xpath('//doc'))

    xml_list = []
    for record in records:
        doc_id = record.xpath("str[@name='id']")[0].text
        record = ElementTree.tostring(record, encoding=record_encoding)
        xml_list.append(RawDocument({
            'doc': record,
            'source': NAME,
            'docID': copy_to_unicode(doc_id),
            'filetype': 'xml'
        }))

    return xml_list


def get_response(rows, days_back):
    ''' helper function to get a response from the DataONE
    API, with the specified number of rows.
    Returns an etree element with results '''
    to_date = datetime.utcnow()
    from_date = (datetime.utcnow() - timedelta(days=days_back))

    to_date = to_date.replace(hour=0, minute=0, second=0, microsecond=0)
    from_date = from_date.replace(hour=0, minute=0, second=0, microsecond=0)

    query = 'dateModified:[{}Z TO {}Z]'.format(from_date.isoformat(), to_date.isoformat())
    logger.info(query)

    n = 0
    contents = []
    while n < rows:
        data = requests.get(DATAONE_SOLR_ENDPOINT, params={
            'q': query,
            'start': n,
            'rows': 1000
        })
        contents.append(data.content)
        n += 1000

    docs = [etree.XML(content) for content in contents]

    return docs


def get_properties(doc):
    properties = {
        'author': (doc.xpath("str[@name='author']/node()") or [''])[0],
        'authorGivenName': (doc.xpath("str[@name='authorGivenName']/node()") or [''])[0],
        'authorSurName': (doc.xpath("str[@name='authorSurName']/node()") or [''])[0],
        'authoritativeMN': (doc.xpath("str[@name='authoritativeMN']/node()") or [''])[0],
        'checksum': (doc.xpath("str[@name='checksum']/node()") or [''])[0],
        'checksumAlgorithm': (doc.xpath("str[@name='checksumAlgorithm']/node()") or [''])[0],
        'dataUrl': (doc.xpath("str[@name='dataUrl']/node()") or [''])[0],
        'datasource': (doc.xpath("str[@name='datasource']/node()") or [''])[0],
        'documents': doc.xpath("arr[@name='documents']/str/node()"),
        'dateModified': (doc.xpath("date[@name='dateModified']/node()") or [''])[0],
        'datePublished': (doc.xpath("date[@name='datePublished']/node()") or [''])[0],
        'dateUploaded': (doc.xpath("date[@name='dateUploaded']/node()") or [''])[0],
        'pubDate': (doc.xpath("date[@name='pubDate']/node()") or [''])[0],
        'updateDate': (doc.xpath("date[@name='updateDate']/node()") or [''])[0],
        'fileID': (doc.xpath("str[@name='fileID']/node()") or [''])[0],
        'formatId': (doc.xpath("str[@name='formatId']/node()") or [''])[0],
        'formatType': (doc.xpath("str[@name='formatType']/node()") or [''])[0],
        'identifier': (doc.xpath("str[@name='identifier']/node()") or [''])[0],
        'investigator': doc.xpath("arr[@name='investigator']/str/node()"),
        'origin': doc.xpath("arr[@name='origin']/str/node()"),
        'isPublic': (doc.xpath("bool[@name='isPublic']/node()") or [''])[0],
        'readPermission': doc.xpath("arr[@name='readPermission']/str/node()"),
        'replicaMN': doc.xpath("arr[@name='replicaMN']/str/node()"),
        'replicaVerifiedDate': doc.xpath("arr[@name='replicaVerifiedDate']/date/node()"),
        'replicationAllowed': (doc.xpath("bool[@name='replicationAllowed']/node()") or [''])[0],
        'numberReplicas': (doc.xpath("int[@name='numberReplicas']/node()") or [''])[0],
        'preferredReplicationMN': doc.xpath("arr[@name='preferredReplicationMN']/str/node()"),
        'resourceMap': doc.xpath("arr[@name='resourceMap']/str/node()"),
        'rightsHolder': (doc.xpath("str[@name='rightsHolder']/node()") or [''])[0],
        'scientificName': doc.xpath("arr[@name='scientificName']/str/node()"),
        'site': doc.xpath("arr[@name='site']/str/node()"),
        'size': (doc.xpath("long[@name='size']/node()") or [''])[0],
        'sku': (doc.xpath("str[@name='sku']/node()") or [''])[0],
        'isDocumentedBy': doc.xpath("arr[@name='isDocumentedBy']/str/node()"),
    }

    # make sure everything in propeties is unicode
    for key, value in properties.iteritems():
        if isinstance(value, etree._ElementStringResult) or isinstance(value, str):
            properties[key] = copy_to_unicode(value)
        elif isinstance(value, list):
            unicode_list = []
            for item in value:
                unicode_list.append(copy_to_unicode(item))
            properties[key] = unicode_list

    properties = {key: value for key, value in properties.items()
                  if value is not u''}

    return properties


# currently unused - but maybe in the future?
def name_from_email(email):
    email_re = '(.+?)@'
    name = re.search(email_re, email).group(1)
    if '.' in name:
        name = name.replace('.', ' ').title()
    return name


def get_contributors(doc):
    author = (doc.xpath("str[@name='author']/node()") or [''])[0]
    submitters = doc.xpath("str[@name='submitter']/node()")
    contributors = doc.xpath("arr[@name='origin']/str/node()")

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
                'prefix': name.title,
                'given': name.first,
                'middle': name.middle,
                'family': name.last,
                'suffix': name.suffix,
                'email': copy_to_unicode(email),
                'ORCID': ''
            }
            contributor_list.append(contributor_dict)
        else:
            name = HumanName(contributor)
            contributor_list.append({
                'prefix': name.title,
                'given': name.first,
                'middle': name.middle,
                'family': name.last,
                'suffix': name.suffix,
                'email': '',
                'ORCID': ''
            })

    return contributor_list


def get_ids(doc, raw_doc):
    # id
    doi = ''
    service_id = raw_doc.get('docID')
    # regex for just getting doi out of crazy urls and sometimes not urls
    doi_re = '10\\.\\d{4}/\\w*\\.\\w*(/\\w*)?'
    try:
        regexed_doi = re.search(doi_re, service_id).group(0)
        doi = regexed_doi
    except AttributeError:
        doi = ''
    if doi == '':
        try:
            doc_doi = doc.xpath("arr[@name='isDocumentedBy']/str/node()")[0]
            doc_doi = doc_doi.replace('doi:', '')
            regexed_doi = re.search(doi_re, doc_doi).group(0)
        except (IndexError, AttributeError):
            doi = ''

    url = (doc.xpath('//str[@name="dataUrl"]/node()') or [''])[0]
    if 'http' not in url and 'http' in service_id:
        url == service_id

    if url == '':
        raise Exception('Warning: No url provided!')

    ids = {'serviceID': service_id, 'doi': copy_to_unicode(doi), 'url': copy_to_unicode(url)}

    return ids


def get_tags(doc):
    tags = doc.xpath("//arr[@name='keywords']/str/node()")
    return [copy_to_unicode(tag.lower()) for tag in tags]


def get_date_updated(doc):
    date_updated = (doc.xpath('//date[@name="dateModified"]/node()') or [''])[0]
    date = parse(date_updated).isoformat()
    return copy_to_unicode(date)


def normalize(raw_doc):
    raw_doc_text = raw_doc.get('doc')
    doc = etree.XML(raw_doc_text)

    title = (doc.xpath("str[@name='title']/node()") or [''])[0]
    description = (doc.xpath("str[@name='abstract']/node()") or [''])[0]

    normalized_dict = {
        'title': copy_to_unicode(title),
        'contributors': get_contributors(doc),
        'properties': get_properties(doc),
        'description': copy_to_unicode(description),
        'id': get_ids(doc, raw_doc),
        'tags': get_tags(doc),
        'source': NAME,
        'dateUpdated': get_date_updated(doc)
    }

    # Return none if no url - not good for notification service
    if normalized_dict['id']['url'] == u'':
        logger.info('Document with ID {} has no URL, not being normalized'.format(normalized_dict['id']['serviceID']))
        return None

    # DATA and RESOURCE info is included in the METADATA's documents
    # and resourceMap fields aldready - no need to return these
    if normalized_dict['properties']['formatType'] != 'METADATA':
        logger.info('Document with ID {} has formatType {}, not being normalized'.format(normalized_dict['id']['serviceID'], normalized_dict['properties']['formatType']))
        return None

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(harvest, normalize))
