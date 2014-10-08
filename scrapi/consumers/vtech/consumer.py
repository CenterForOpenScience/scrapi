# VTechWorks consumer by Erin & Coral

from __future__ import unicode_literals

import requests
from datetime import date, timedelta, datetime
from dateutil.parser import *
import time
from lxml import etree
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument
from nameparser import HumanName


NAME = 'vtechworks'
OAI_DC_BASE = 'http://vtechworks.lib.vt.edu/oai/request?verb=ListRecords'
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'}
DEFAULT = datetime(1970, 01, 01)
DEFAULT_ENCODING = 'utf-8'
record_encoding = None


def consume(days_back=5):
    start_date = date.today() - timedelta(days_back)
    url = '{}&metadataPrefix=oai_dc&from={}'.format(OAI_DC_BASE, str(start_date))
    records = get_records(url)
    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
        record = etree.tostring(record, encoding=(record_encoding or DEFAULT_ENCODING))
        xml_list.append(RawDocument({
            'doc': record,
            'source': NAME,
            'docID': copy_to_unicode(doc_id),
            'filetype': 'xml'
        }))

    return xml_list


def get_records(url):
    data = requests.get(url)
    record_encoding = data.encoding
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(0.5)
        url ='{}&resumptionToken={}'.format(OAI_DC_BASE, token[0])
        records += get_records(url)

    return records


def copy_to_unicode(element):
    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def get_contributors(result):
    dctype = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    contributors = result.xpath('//dc:contributor/node()', namespaces=NAMESPACES)
    creators = result.xpath('//dc:creator/node()', namespaces=NAMESPACES)

    if 'thesis' not in dctype.lower() and 'dissertation' not in dctype.lower():
        all_contributors = contributors + creators
    else:
        all_contributors = creators

    contributor_list = []
    for person in all_contributors:
        name = HumanName(person)
        contributor = {
            'prefix': name.title,
            'given': name.first,
            'middle': name.middle,
            'family': name.last,
            'suffix': name.suffix,
            'email': '',
            'ORCID': '',
        }
        contributor_list.append(contributor)
    return contributor_list


def get_tags(result):
    tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    return [copy_to_unicode(tag.lower()) for tag in tags]


def get_ids(result, doc):
    service_id = doc.get('docID')
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for item in identifiers:
        if 'hdl.handle.net' in item:
            url = item
        if 'doi' in item or 'DOI' in item:
            doi = item
            doi = doi.replace('doi:', '')
            doi = doi.replace('DOI:', '')
            doi = doi.replace('http://dx.doi.org/', '')
            doi = doi.strip(' ')

    if url == '':
        raise Exception('Warning: No url provided!')

    return {
        'serviceID': service_id,
        'url': copy_to_unicode(url),
        'doi': copy_to_unicode(doi)
    }


def get_properties(result):
    result_type = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    rights = (result.xpath('//dc:rights/node()', namespaces=NAMESPACES) or [''])
    if len(rights) > 1:
        copyrightt = ' '.join(rights)
    else:
        copyrightt = rights
    publisher = (result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    relation = (result.xpath('//dc:relation/node()', namespaces=NAMESPACES) or [''])[0]
    language = (result.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0]
    props = {
        'type': copy_to_unicode(result_type),
        'language': copy_to_unicode(language),
        'relation': copy_to_unicode(relation),
        'publisherInfo': {
            'publisher': copy_to_unicode(publisher),
        },
        'permissions': {
            'copyrightStatement': copy_to_unicode(copyrightt),
        },
    }
    return props


def get_date_created(result):
    dates = result.xpath('//dc:date/node()', namespaces=NAMESPACES)
    date_list = []
    for item in dates:
        a_date = parse(str(item)[:10], yearfirst=True,  default=DEFAULT).isoformat()
        date_list.append(a_date)
    min_date = min(date_list)
    return copy_to_unicode(min_date)


def get_date_updated(result):
    dateupdated = result.xpath('//ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES)[0]
    date_updated = parse(dateupdated).isoformat()
    return copy_to_unicode(date_updated)


def normalize(raw_doc):
    result = raw_doc.get('doc')
    try:
        result = etree.XML(result)
    except etree.XMLSyntaxError:
        print('Error in namespaces! Skipping this one...')
        return None

    title = copy_to_unicode((result.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0])
    description = copy_to_unicode((result.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0])

    payload = {
        'title': title,
        'contributors': get_contributors(result),
        'properties': get_properties(result),
        'description': description,
        'tags': get_tags(result),
        'id': get_ids(result, raw_doc),
        'source': NAME,
        'dateUpdated': get_date_updated(result),
        'dateCreated': get_date_created(result),
    }

    return NormalizedDocument(payload)


if __name__ == '__main__':
    print(lint(consume, normalize))
