# Consumer for UPenn Scholarly Commons

import requests
from datetime import date, timedelta, datetime
from dateutil.parser import *
import time
from lxml import etree
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument
from nameparser import HumanName

NAME = 'upenn'
TODAY = date.today()
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/'}
OAI_DC_BASE_URL = 'http://repository.upenn.edu/do/oai/?verb=ListRecords'
DEFAULT = datetime(1970, 01, 01)

def consume(days_back=1):
    url = OAI_DC_BASE_URL + '&metadataPrefix=oai_dc&from='
    start_date = TODAY - timedelta(days_back)
    # YYYY-MM-DD hh:mm:ss
    url += str(start_date)
    records = get_records(url)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=NAMESPACES)[0].text
        record = etree.tostring(record)
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
    doc = etree.XML(data.content)
    records = doc.xpath('//ns0:record', namespaces=NAMESPACES)
    token = doc.xpath('//ns0:resumptionToken/node()', namespaces=NAMESPACES)

    if len(token) == 1:
        time.sleep(0.5)
        url += '&resumptionToken=' + token[0]
        records += get_records(url)

    return records


def get_contributors(result):
    contributors = result.xpath('//dc:contributor/node()', namespaces=NAMESPACES) or []
    creators = result.xpath('//dc:creator/node()', namespaces=NAMESPACES) or []
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
            'ORCID': '',
            }
        contributor_list.append(contributor)
    return contributor_list

def get_tags(result):
    subjects = result.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    tags = []
    #<dc:subject>Biology, General|Biology, Evolution and Development|Biology, Bioinformatics</dc:subject>
    for subject in subjects:
        alist = subject.split('|')
        tags += alist
    return [tag.lower() for tag in tags]

def get_ids(result, doc):
    serviceID = doc.get('docID')
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for item in identifiers:
        if 'hdl.handle.net' in item:
            url = item
        if 'dx.doi.org' in item:
            url = item
            doi = item.replace('http://dx.doi.org/', '')
        if 'repository.upenn.edu' in item:
            if url == '':
                url = item
        if 'works.bepress.com' in item:
            if url == '':
                url = item
    if url == '':
        raise Exception('Warning: No url provided!')
    return {'serviceID': serviceID, 'url': url, 'doi': doi}

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

def get_properties(result):
    publisher = (result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or [''])[0]
    dcformat = (result.xpath('//dc:format/node()', namespaces=NAMESPACES) or [''])[0]
    dctype = (result.xpath('//dc:type/node()', namespaces=NAMESPACES) or [''])[0]
    source = (result.xpath('//dc:source/node()', namespaces=NAMESPACES) or [''])[0]
    props = {
        'format': dcformat,
        'type': dctype,
        'source': source,
        'publisherInfo': {
            'publisher': publisher,
        },
    }
    return props

def normalize(raw_doc, timestamp):
    result = raw_doc.get('doc')
    try:
        result = etree.XML(result)
    except etree.XMLSyntaxError:
        print "Error in namespaces! Skipping this one..."
        return None

    title = result.xpath('//dc:title/node()', namespaces=NAMESPACES)[0]
    description = (result.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0]

    payload = {
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

    #import json
    #print(json.dumps(payload, indent=4))
    return NormalizedDocument(payload)

if __name__ == '__main__':
    print(lint(consume, normalize))
