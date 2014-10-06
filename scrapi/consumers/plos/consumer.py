from __future__ import unicode_literals

import time
from datetime import date, timedelta

import requests

from lxml import etree

from dateutil.parser import *

from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

try:
    from settings import PLOS_API_KEY
except ImportError:
    from scrapi.settings import PLOS_API_KEY

MAX_ROWS_PER_REQUEST = 999

NAME = 'plos'

DEFAULT_ENCODING = 'UTF-8'

record_encoding = None

def copy_to_unicode(element):

    encoding = record_encoding or DEFAULT_ENCODING
    element = ''.join(element)
    if isinstance(element, unicode):
        return element
    else:
        return unicode(element, encoding=encoding)

def consume(days_back=1):
    if not PLOS_API_KEY:
        return []
    payload = {"api_key": PLOS_API_KEY, "rows": "0"}
    START_DATE = str(date.today() - timedelta(days_back)) + "T00:00:00Z"
    TODAY = str(date.today()) + "T00:00:00Z"
    base_url = 'http://api.plos.org/search?q=publication_date:'
    base_url += '[{}%20TO%20{}]'.format(START_DATE, TODAY)
    plos_request = requests.get(base_url, params=payload)
    record_encoding = plos_request.encoding
    xml_response = etree.XML(plos_request.content)
    num_results = int(xml_response.xpath('//result/@numFound')[0])

    start = 0
    rows = MAX_ROWS_PER_REQUEST
    doc_list = []

    while rows < num_results + MAX_ROWS_PER_REQUEST:
        payload = {"api_key": PLOS_API_KEY, "rows": rows, "start": start}
        results = requests.get(base_url, params=payload)
        tick = time.time()
        xml_doc = etree.XML(results.content)
        all_docs = xml_doc.xpath('//doc')

        for result in all_docs:
            has_authors_or_abstract = False
            all_children = result.getchildren()
            for element in all_children:
                name = element.attrib.get('name')
                if name == 'author_display' or name == 'abstract':
                    has_authors_or_abstract = True
                if name == 'id':
                    docID = element.text
            if has_authors_or_abstract == True:
                doc_list.append(RawDocument({
                    'doc': etree.tostring(result),
                    'source': NAME,
                    'docID': copy_to_unicode(docID),
                    'filetype': 'xml',
                }))

        start += MAX_ROWS_PER_REQUEST
        rows += MAX_ROWS_PER_REQUEST

        if time.time() - tick < 5:
            time.sleep(5 - (time.time() - tick))

    return doc_list


def get_ids(raw_doc, record):
    doi = record.xpath('//str[@name="id"]/node()')[0]
    ids = {
        'doi': copy_to_unicode(doi),
        'serviceID': raw_doc.get('docID'),
        'url': 'http://dx.doi.org/{}'.format(doi)
    }
    return ids

def get_contributors(record):
    contributor_list = []
    contributors = record.xpath('//arr[@name="author_display"]/str/node()') or ['']
    for person in contributors:
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


def get_properties(record):
    properties = {
        'journal': (record.xpath('//str[@name="journal"]/node()') or [''])[0],
        'eissn': (record.xpath('//str[@name="eissn"]/node()') or [''])[0],
        'articleType': (record.xpath('//str[@name="article_type"]/node()') or [''])[0],
        'score': (record.xpath('//float[@name="score"]/node()') or [''])[0],
    }

    # ensure everything is in unicode
    for key, value in properties.iteritems():
        properties[key] = copy_to_unicode(value)

    return properties

def get_date_created(record):
    date_created =  (record.xpath('//date[@name="publication_date"]/node()') or [''])[0]
    date = parse(date_created).isoformat()
    return copy_to_unicode(date)

# TODO - PLoS doesn't seem to return date updated, so just putting 
# date published here... 
def get_date_updated(record):
    return get_date_created(record)

# No tags... 
def get_tags(record):
    return []

def normalize(raw_doc):
    raw_doc_string = raw_doc.get('doc')
    record = etree.XML(raw_doc_string)

    title = record.xpath('//str[@name="title_display"]/node()')[0]
    description = (record.xpath('//arr[@name="abstract"]/str/node()') or [''])[0],

    normalized_dict = {
        'title': copy_to_unicode(title),
        'contributors': get_contributors(record),
        'description': copy_to_unicode(description),
        'properties': get_properties(record),
        'id': get_ids(raw_doc, record),
        'source': NAME,
        'dateCreated': get_date_created(record),
        'dateUpdated': get_date_updated(record),
        'tags': get_tags(record)
    }

    # deal with Corrections having "PLoS Staff" listed as contributors
    # fix correction title
    if normalized_dict['properties']['articleType'] == 'Correction':
        normalized_dict['title'] = normalized_dict['title'].replace('Correction: ', '')
        normalized_dict['contributors'] = [{
            'prefix': '',
            'given': '',
            'middle': '',
            'family': '',
            'suffix': '',
            'email': '',
            'ORCID': ''
        }]
    return NormalizedDocument(normalized_dict)

if __name__ == '__main__':
    print(lint(consume, normalize))
