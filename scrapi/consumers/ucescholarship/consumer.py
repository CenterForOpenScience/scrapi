
''' Consumer and Normalizer for the eScholarship Repo 
at the University of California's California Digital Library
'''

from lxml import etree
from xml.etree import ElementTree
from datetime import date, timedelta, datetime
from dateutil.parser import *
import requests
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument
from nameparser import HumanName

TODAY = date.today()
NAME = 'escholarship'
OAI_DC_BASE_URL = 'http://www.escholarship.org/uc/oai?verb=ListRecords&metadataPrefix=oai_dc&from='
DEFAULT = datetime(1970, 01, 01)

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
        record = ElementTree.tostring(record)
        record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
        xml_list.append(RawDocument({
                    'doc': record,
                    'source': NAME,
                    'docID': doc_id,
                    'filetype': 'xml'
                }))
    return xml_list

def get_records(url):
    data = requests.get(url)
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
            'email': '',
            'ORCID': '',
            }
        contributor_list.append(contributor)
    return contributor_list

def get_tags(result):
    tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES)
    return [tag.lower() for tag in tags]

def get_ids(result, raw_doc):
    service_id = raw_doc.get('docID')
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)

    for item in identifiers:
        if 'escholarship.org' in item:
            url = item
    # TODO: get serviceID from source
    ids = {'url': url, 'serviceID': service_id, 'doi': ''}
    return ids

def get_properties(result):
    citation = (result.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    dc_type = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    format = (result.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0]
    coverage = (result.xpath('//dc:coverage/node()', namespaces=NAMESPACES) or [''])[0]
    relation = (result.xpath('//dc:relation/node()', namespaces=NAMESPACES) or [''])[0]
    rights = (result.xpath('//dc:rights/node()', namespaces=NAMESPACES) or [''])[0]
    publisher = (result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]

    props = {
        'type': dc_type,
        'format': format,
        'relation': relation,
        'permissions': {
            'copyrightStatement': rights,
        },
        'publisherInfo': {
          'publisher': publisher,
        },
        'coverage': coverage,
        'citation': citation,
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

    normalized_dict = {
        'title': title,
        'contributors': get_contributors(result),
        'properties': get_properties(result),
        'description': description,
        'tags': get_tags(result),
        'id': get_ids(result,raw_doc),
        'source': NAME,
        'dateUpdated': get_date_updated(result),
        'dateCreated': get_date_created(result),
        'timestamp': str(timestamp),
    }

    return NormalizedDocument(normalized_dict)
        

if __name__ == '__main__':
    print(lint(consume, normalize))
