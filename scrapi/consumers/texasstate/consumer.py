# Consumer for Texas State University
from __future__ import unicode_literals

import os
import time
from dateutil.parser import parse
from datetime import date, timedelta, datetime

import requests

from lxml import etree

from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'texasstate'
TODAY = date.today()
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/',
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'}
DEFAULT = datetime(1970, 01, 01)
DEFAULT_ENCODING = 'UTF-8'

record_encoding = None


def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)


def consume(days_back=10):
    start_date = str(date.today() - timedelta(days_back))
    base_url = 'http://digital.library.txstate.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from='
    start_date = TODAY - timedelta(days_back)
    # YYYY-MM-DD hh:mm:ss
    url = base_url + str(start_date) + ' 00:00:00'
    records = get_records(url)
    record_encoding = requests.get(url).encoding

    xml_list = []
    for record in records:
        doc_id = record.xpath(
            'ns0:header/ns0:identifier', namespaces=NAMESPACES)[0].text
        record = etree.tostring(record, encoding=record_encoding)
        xml_list.append(RawDocument({
            'doc': record,
            'source': NAME,
            'docID': copy_to_unicode(doc_id),
            'filetype': 'xml'
        }))

    return xml_list


def get_records(url):
    print(url)
    data = requests.get(url)
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    return records


def getcontributors(result):
    contributors = result.xpath(
        '//dc:contributor/node()', namespaces=NAMESPACES) or ['']
    creators = result.xpath(
        '//dc:creator/node()', namespaces=NAMESPACES) or ['']

    all_contributors = contributors + creators

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
            'ORCID': ''
        }
        contributor_list.append(contributor)
    return contributor_list


def gettags(result):
    tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    return [copy_to_unicode(tag.lower()) for tag in tags]


def get_ids(result, doc):
    serviceID = doc.get('docID')
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for item in identifiers:
        if 'digital.library.txstate.edu' in item or 'hdl.handle.net' in item:
            url = item
        if 'doi' in item or 'DOI' in item:
            doi = item
            doi = doi.replace('doi:', '')
            doi = doi.replace('DOI:', '')
            doi = doi.replace('http://dx.doi.org/', '')
            doi = doi.strip(' ')

    return {'serviceID': serviceID, 'url': copy_to_unicode(url), 'doi': copy_to_unicode(doi)}


def get_properties(result):
    result_type = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    rights = result.xpath('//dc:rights/node()', namespaces=NAMESPACES) or ['']
    if len(rights) > 1:
        copyright = ' '.join(rights)
    else:
        copyright = rights
    publisher = (result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    relation = (result.xpath('//dc:relation/node()', namespaces=NAMESPACES) or [''])[0]
    language = (result.xpath('//dc:language/node()', namespaces=NAMESPACES) or [''])[0]
    dates = result.xpath('//dc:date/node()', namespaces=NAMESPACES) or ['']
    set_spec = result.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]
    props = {
        'type': copy_to_unicode(result_type),
        'dates': copy_to_unicode(dates),
        'language': copy_to_unicode(language),
        'relation': copy_to_unicode(relation),
        'publisherInfo': {
            'publisher': copy_to_unicode(publisher),
        },
        'permissions': {
            'copyrightStatement': copy_to_unicode(copyright),
        }
    }
    return props


def get_date_created(result):
    dates = (result.xpath('//dc:date/node()', namespaces=NAMESPACES) or [''])
    date = copy_to_unicode(dates[0])
    return date


def get_date_updated(result):
    dateupdated = result.xpath('//ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES)[0]
    date_updated = parse(dateupdated).isoformat()
    return copy_to_unicode(date_updated)


def normalize(raw_doc):
    result = raw_doc.get('doc')
    try:
        result = etree.XML(result)
    except etree.XMLSyntaxError:
        print "Error in namespaces! Skipping this one..."
        return None

    with open(os.path.join(os.path.dirname(__file__), 'series_names.txt')) as series_names:
        series_name_list = [word.replace('\n', '') for word in series_names]

    set_spec = result.xpath('ns0:header/ns0:setSpec/node()', namespaces=NAMESPACES)[0]

    if set_spec.replace('publication:', '') not in series_name_list:
        print('{} not in approved list, not normalizing...'.format(set_spec))
        return None

    title = result.xpath('//dc:title/node()', namespaces=NAMESPACES)[0]
    description = (result.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]

    payload = {
        'title': copy_to_unicode(title),
        'contributors': getcontributors(result),
        'properties': get_properties(result),
        'description': copy_to_unicode(description),
        'tags': gettags(result),
        'id': get_ids(result, raw_doc),
        'source': NAME,
        'dateCreated': get_date_created(result),
        'dateUpdated': get_date_updated(result)
    }

    if payload['id']['url'] == '':
        print "Warning, no URL provided, not normalizing..."
        return None

    return NormalizedDocument(payload)


if __name__ == '__main__':
    print(lint(consume, normalize))
