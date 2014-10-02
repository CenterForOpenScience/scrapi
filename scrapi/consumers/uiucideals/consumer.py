# Consumer for UIUC-IDEALS

import requests
from datetime import date, timedelta
import time
from lxml import etree 
from nameparser import HumanName

from dateutil.parser import *

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'uiuc-ideals'
TODAY = date.today()
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/'}
OAI_DC_BASE = 'http://ideals.uiuc.edu/dspace-oai/request'

def consume(days_back=100):
    # days back is set so high because uiuc ideals consumer had nothing for the last three months when consumer was built
    start_date = str(date.today() - timedelta(days_back))
    base_url = OAI_DC_BASE + '?verb=ListRecords&metadataPrefix=oai_dc&from={} 00:00:00'
    url = base_url.format(start_date)
    print url

    records = get_records(url)

    xml_list = []
    for record in records:
        doc_id = record.xpath('ns0:header/ns0:identifier', namespaces=NAMESPACES)[0].text
        record = etree.tostring(record, encoding="UTF-8")
        # record = '<?xml version="1.0" encoding="UTF-8"?>\n' + record
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
    
    records_collected = 0

    if len(token) == 1:
        time.sleep(0.5)
        url = OAI_DC_BASE + '?verb=ListRecords&resumptionToken={}'.format(token[0])
        records += get_records(url)
    return records


def get_contributors(result):
    contributors = result.xpath('//dc:contributor/node()', namespaces=NAMESPACES) or ['']
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
    all_tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    tags = []
    for tag in all_tags:
        if ',' in tag:
            tags += tag.split(',')
        else:
            tags.append(tag)
    return [tag.lower().strip() for tag in tags]

def getabstract(result):
    return abstract[0]

def get_ids(result):
    service_id = result.xpath('ns0:header/ns0:identifier/node()', namespaces=NAMESPACES)[0]
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES)
    url = ''
    doi = ''
    for item in identifiers:
        if 'hdl.handle.net' in item:
            url = item
    # TODO some DOIs might be buried within identifiers - hard to find, but there

    if url == '':
        raise Exception('Warning: No url provided!')

    return {'serviceID': service_id, 'url': url, 'doi': doi}

# TODO - this function is unused for now - might implement this later
def get_earliest_date(result):
    dates = result.xpath('//dc:date/node()', namespaces=NAMESPACES)
    date_list = []
    for item in dates:
        try:
            a_date = time.strptime(str(item)[:10], '%Y-%m-%d')
        except ValueError:
            try:
                a_date = time.strptime(str(item)[:10], '%Y')
            except ValueError:
                try:
                    a_date = time.strptime(str(item)[:10], '%m/%d/%Y')
                except ValueError:
                    try:
                        a_date = time.strptime(str(item)[:10], '%Y-%d-%m')
                    except ValueError:
                        a_date = time.strptime(str(item)[:10], '%Y-%m')
        date_list.append(a_date)
    min_date =  min(date_list) 
    min_date = time.strftime('%Y-%m-%d', min_date)

    return min_date

def get_properties(result):
    result_type = result.xpath('//dc:type/node()', namespaces=NAMESPACES) or ['']
    rights = result.xpath('//dc:rights/node()', namespaces=NAMESPACES) or ['']
    identifiers = result.xpath('//dc:identifier/node()', namespaces=NAMESPACES) or ['']
    publisher = result.xpath('//dc:publisher/node()', namespaces=NAMESPACES) or ['']

    properties = {
        'type': result_type[0],
        'rights': rights[0],
        'identifiers': identifiers,
        'publisher': publisher[0]
    }
    return properties

def get_date_created(result):
    dates = result.xpath('//dc:date/node()', namespaces=NAMESPACES)
    return parse(dates[0]).isoformat()

def get_date_updated(result):
    date_updated = result.xpath('ns0:header/ns0:datestamp', namespaces=NAMESPACES)[0].text
    return parse(date_updated).isoformat()

def normalize(raw_doc, timestamp):
    result = raw_doc.get('doc')
    try:
        result = etree.XML(result)
    except etree.XMLSyntaxError:
        print "Error in namespaces! Skipping this one..."
        return None

    payload = {
        'title': (result.xpath('//dc:title/node()', namespaces=NAMESPACES) or [''])[0],
        'contributors': get_contributors(result),
        'properties': get_properties(result),
        'description': (result.xpath('//dc:description/node()', namespaces=NAMESPACES) or [''])[0],
        'tags': get_tags(result),
        'id': get_ids(result),
        'source': NAME,
        'dateCreated': get_date_created(result),
        'dateUpdated': get_date_updated(result),
        'timestamp': str(timestamp)

    }
    import json; print json.dumps(payload['tags'], indent=4)
    return NormalizedDocument(payload)

if __name__ == '__main__':
    print(lint(consume, normalize))
