# Consumer for Texas State University

import requests
from datetime import date, timedelta, datetime
from dateutil.parser import *
import time
from lxml import etree
from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument
from nameparser import HumanName
import json

NAME = 'DSpace at Texas State University'
TODAY = date.today()
NAMESPACES = {'dc': 'http://purl.org/dc/elements/1.1/', 
            'oai_dc': 'http://www.openarchives.org/OAI/2.0/',
            'ns0': 'http://www.openarchives.org/OAI/2.0/'}
DEFAULT = datetime(1970, 01, 01)

def consume(days_back=250):
    # days back is set so high because texas state consumer had nothing for the last six months plus when consumer was built
    start_date = str(date.today() - timedelta(days_back))
    base_url = 'http://digital.library.txstate.edu/oai/request?verb=ListRecords&metadataPrefix=oai_dc&from='
    start_date = TODAY - timedelta(days_back)
    #YYYY-MM-DD hh:mm:ss
    url = base_url + str(start_date) + ' 00:00:00'

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

    return records


def getcontributors(result):
    contributors = result.xpath('//dc:contributor/node()', namespaces=NAMESPACES) or ['']
    creators = result.xpath('//dc:creator/node()', namespaces=NAMESPACES) or ['']
    # contributor_list = []
    # for person in contributors:
    #     contributor_list.append({'full_name': person, 'email': ''})
    # return contributor_list

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

def gettags(result):
    tags = result.xpath('//dc:subject/node()', namespaces=NAMESPACES) or []
    return [tag.lower() for tag in tags]

def get_ids(result, doc):
    serviceID = doc.get('docID')
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

    #if url == '':
    #   raise Exception('Warning: No url provided!')

    return {'serviceID': serviceID, 'url': url, 'doi': doi}

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
    props = {
        'type': result_type,
        'dates': dates,
        'language': language,
        'relation': relation,
        'publisherInfo': {
            'publisher': publisher,
        },
        'permissions': {
            'copyrightStatement': copyright,
        },
    }
    return props

def get_date_created(result):
    dates = result.xpath('//dc:date/node()', namespaces=NAMESPACES) or ['']
    return dates[0]
    # date_list = []
    # for item in dates:
    #     try: 
    #         a_date = parse(str(item)[:10], yearfirst=True,  default=DEFAULT).isoformat()
    #     except ValueError: 
    #         import pdb; pdb.set_trace()
    #     date_list.append(a_date)
    # min_date = date_list[0]
    # return min_date

def get_date_updated(result):
    dateupdated = result.xpath('//ns0:header/ns0:datestamp/node()', namespaces=NAMESPACES)[0]
    date_updated = parse(dateupdated).isoformat()
    return date_updated

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
        'contributors': getcontributors(result),
        'properties': get_properties(result),
        'description': description,
        'tags': gettags(result),
        'id': get_ids(result,raw_doc),
        'source': NAME,
        'dateCreated': get_date_created(result),
        'dateUpdated': get_date_updated(result),
        'timestamp': str(timestamp),

    }
    #print payload
    return NormalizedDocument(payload)
    ## TODO catch namespace exception

if __name__ == '__main__':
    print(lint(consume, normalize))
