# University of Washington consumer

from __future__ import unicode_literals

import requests
from datetime import date, timedelta, datetime
from dateutil.parser import *
import time
from lxml import etree
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument
from nameparser import HumanName


NAME = 'cmu'
OAI_DC_BASE = 'http://repository.cmu.edu/do/oai/'
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
              'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
              'ns0': 'http://www.openarchives.org/OAI/2.0/'}
DEFAULT = datetime(1970, 01, 01)
DEFAULT_ENCODING = 'utf-8'
record_encoding = None


def consume(days_back=1):
    base_url = OAI_DC_BASE + '?verb=ListRecords&metadataPrefix=oai_dc&from='
    start_date = date.today() - timedelta(days_back)
    url = base_url + str(start_date)
    records = get_records(url)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=NAMESPACES)[0].text
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
        base_url = OAI_DC_BASE + '?verb=ListRecords&resumptionToken='
        url = base_url + token[0]
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

    if 'hesis' not in dctype and 'issertation' not in dctype:
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
            'email': u'',
            'ORCID': u'',
        }
        contributor_list.append(contributor)
    return contributor_list


def get_tags(result):
    tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    thetags = []
    for tag in tags:
        if ';' in tag:
            moretags = tag.split(';')
            moretags = [word.strip() for word in moretags]
        elif '::' in tag:
            moretags = tag.split('::')
            moretags = [word.strip() for word in moretags]
        else:
            moretags = []
        thetags += moretags
    return [copy_to_unicode(tag.lower()) for tag in tags]


def get_ids(result, doc):
    serviceID = doc.get('docID')
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for item in identifiers:
        if 'hdl.handle.net' in item:
            url = item
        if 'doi' in item or 'DOI' in item:
            url = item
            doi = item
            doi = doi.replace('http://dx.doi.org/', '')
            doi = doi.strip(' ')
        if 'repository.cmu.edu' in item and 'viewcontent.cgi' not in item:
            url = item

    if url == '':
        raise Exception('Warning: No url provided!')

    return {'serviceID': serviceID,
            'url': copy_to_unicode(url),
            'doi': copy_to_unicode(doi)}


def get_properties(result):
    result_type = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    rights = result.xpath('//dc:rights/node()', namespaces=NAMESPACES) or ['']
    if len(rights) > 1:
        copyright = ' '.join(rights)
    else:
        copyright = rights
    publisher = (result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    source = (result.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    props = {
        'type': copy_to_unicode(result_type),
        'publisherInfo': {
            'publisher': copy_to_unicode(publisher),
        },
        'source': copy_to_unicode(source),
    }
    return props


def get_date_created(result):
    dates = result.xpath('//dc:date/node()', namespaces=NAMESPACES)
    date_list = []
    for item in dates:
        a_date = parse(str(item)[:10], yearfirst=True,  default=DEFAULT).isoformat()
        date_list.append(a_date)
    min_date = min(date_list)
    return min_date


def get_date_updated(result):
    dateupdated = result.xpath('//ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES)[0]
    date_updated = parse(dateupdated).isoformat()
    return date_updated


def normalize(raw_doc):
    result = raw_doc.get('doc')
    try:
        result = etree.XML(result)
    except etree.XMLSyntaxError:
        print('Error in namespaces! Skipping this one...')
        return None

    title = result.xpath('//dc:title/node()', namespaces=NAMESPACES)[0]
    description = (result.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]


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
    }

    # import json
    # print(json.dumps(payload, indent=4))
    return NormalizedDocument(payload)


if __name__ == '__main__':
    print(lint(consume, normalize))
