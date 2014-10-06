
''' Consumer and Normalizer for the eScholarship Repo 
at the University of California's California Digital Library
'''

from __future__ import unicode_literals

from lxml import etree
from xml.etree import ElementTree
from datetime import date, timedelta, datetime
from dateutil.parser import *
import requests
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument
from nameparser import HumanName

TODAY = date.today()
NAME = u'uceschol'
OAI_DC_BASE_URL = 'http://www.escholarship.org/uc/oai?verb=ListRecords&metadataPrefix=oai_dc&from='
DEFAULT = datetime(1970, 01, 01)
DEFAULT_ENCODING = 'utf-8'
record_encoding = None
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
             'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
             'ns0': 'http://www.openarchives.org/OAI/2.0/'}


def consume(days_back=10):
    start_date = TODAY - timedelta(days_back)
    url = OAI_DC_BASE_URL + str(start_date)

    records = get_records(url)
    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=NAMESPACES)[0].text
        record = ElementTree.tostring(record, encoding=(record_encoding or DEFAULT_ENCODING))
        xml_list.append(RawDocument({
            'doc': record,
            'source': NAME,
            'docID': copy_to_unicode(doc_id),
            'filetype': u'xml'
        }))
    return xml_list


def copy_to_unicode(element):
    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def get_records(url):
    data = requests.get(url)
    record_encoding = data.encoding
    doc =  etree.XML(data.content)
    records = doc.xpath('//oai_dc:record', namespaces=NAMESPACES)
    return records


def get_contributors(result):
    contributors = result.xpath('//dc:creator/node()', namespaces=NAMESPACES) or []
    contributor_list = []
    for person in contributors:
        name = HumanName(person)
        contributor = {
            'prefix': name.title,
            'given': name.first,
            'middle': name.middle,
            'family': name.last,
            'suffix': name.suffix,
            'email': u'',
            'ORCID': u'',
            }
        contributor_list.append(contributor)
    return contributor_list


def get_tags(result):
    tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES)
    return [copy_to_unicode(tag.lower()) for tag in tags]


def get_ids(result, raw_doc):
    service_id = raw_doc.get('docID')
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    citation = (result.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    if 'doi' in citation:
        stringtuple = citation.partition('doi:')
        stringlist = stringtuple[2].split(' ')
        doi = stringlist[1]
        if doi[-1] == '.':
            doi = doi[:-1]
    for item in identifiers:
        if 'escholarship.org' in item:
            url = item
    return {'serviceID': copy_to_unicode(service_id),
            'url': copy_to_unicode(url),
            'doi': copy_to_unicode(doi)}


def get_properties(result):
    citation = (result.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    dc_type = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    format = (result.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0]
    coverage = (result.xpath('//dc:coverage/node()', namespaces=NAMESPACES) or [''])[0]
    relation = (result.xpath('//dc:relation/node()', namespaces=NAMESPACES) or [''])[0]
    rights = (result.xpath('//dc:rights/node()', namespaces=NAMESPACES) or [''])[0]
    publisher = (result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]

    props = {
        'type': copy_to_unicode(dc_type),
        'format': copy_to_unicode(format),
        'relation': copy_to_unicode(relation),
        'permissions': {
            'copyrightStatement': copy_to_unicode(rights),
        },
        'publisherInfo': {
          'publisher': copy_to_unicode(publisher),
        },
        'coverage': copy_to_unicode(coverage),
        'citation': copy_to_unicode(citation),
    }
    return props


def get_date_created(result):
    datecreated = result.xpath('//dc:date', namespaces=NAMESPACES)[0].text
    date_created = parse(datecreated, yearfirst=True, default=DEFAULT).isoformat()
    return date_created


def get_date_updated(result):
    dateupdated = result.xpath('//ns0:datestamp', namespaces=NAMESPACES)[0].text
    date_updated = parse(dateupdated, yearfirst=True, default=DEFAULT).isoformat()
    return date_updated


def normalize(raw_doc, timestamp):
    result = raw_doc.get('doc')
    try:
        result = etree.XML(result)
    except etree.XMLSyntaxError:
        print('Error in namespaces! Skipping this one...')
        return None

    title = (result.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0]
    description = (result.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]
    description = description.replace(u'\u00a0', ' ') # someone put in a bunch of non-breaking spaces

    payload = {
        'title': copy_to_unicode(title),
        'contributors': get_contributors(result),
        'properties': get_properties(result),
        'description': copy_to_unicode(description),
        'tags': get_tags(result),
        'id': get_ids(result, raw_doc),
        'source': NAME,
        'dateUpdated': copy_to_unicode(get_date_updated(result)),
        'dateCreated': copy_to_unicode(get_date_created(result)),
        'timestamp': copy_to_unicode(timestamp),
    }

    # import json
    # print(json.dumps(payload, indent=4))
    return NormalizedDocument(payload)
        

if __name__ == '__main__':
    print(lint(consume, normalize))
