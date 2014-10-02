from __future__ import unicode_literals

import time
from datetime import date, timedelta

import requests

from lxml import etree

from dateutil.parser import *

from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

import settings

TODAY = str(date.today()) + "T00:00:00Z"
MAX_ROWS_PER_REQUEST = 999

NAME = 'plos'


def consume(days_back=1):
    if not settings.API_KEY:
        return []
    payload = {"api_key": settings.API_KEY, "rows": "0"}
    START_DATE = str(date.today() - timedelta(days_back)) + "T00:00:00Z"
    base_url = 'http://api.plos.org/search?q=publication_date:'
    base_url += '[{}%20TO%20{}]'.format(START_DATE, TODAY)
    plos_request = requests.get(base_url, params=payload)
    xml_response = etree.XML(plos_request.content)
    num_results = int(xml_response.xpath('//result/@numFound')[0])

    start = 0
    rows = MAX_ROWS_PER_REQUEST
    doc_list = []

    while rows < num_results + MAX_ROWS_PER_REQUEST:
        payload = {"api_key": settings.API_KEY, "rows": rows, "start": start}
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
                    'docID': docID,
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
        'doi': doi,
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
    return properties


def get_date_created(record):
    date_created =  (record.xpath('//date[@name="publication_date"]/node()') or [''])[0]
    return parse(date_created).isoformat()

# PLoS doesn't seem to return date updated, so just putting 
# date consumed here... 
def get_date_updated(timestamp):
    return str(timestamp)

# No tags... 
def get_tags(record):
    return []

def normalize(raw_doc, timestamp):
    raw_doc_string = raw_doc.get('doc')
    record = etree.XML(raw_doc_string)
    normalized_dict = {
        'title': record.xpath('//str[@name="title_display"]/node()')[0],
        'contributors': get_contributors(record),
        'description': (record.xpath('//arr[@name="abstract"]/str/node()') or [''])[0],
        'properties': get_properties(record),
        'id': get_ids(raw_doc, record),
        'source': NAME,
        'timestamp': str(timestamp),
        'dateCreated': get_date_created(record),
        'dateUpdated': get_date_updated(timestamp),
        'tags': get_tags(record),
    }

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
