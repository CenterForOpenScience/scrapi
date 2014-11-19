''' Consumer for figshare '''
from __future__ import unicode_literals


import os
import time
import json
from dateutil.parser import parse
from datetime import date, timedelta, datetime

import requests


from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'figshare'
BASE_URL = 'http://api.figshare.com/v1/articles/search?search_for=*&from_date='

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


def list_to_unicode(str_list):
    return [copy_to_unicode(item) for item in str_list]


def consume(days_back=0):
    start_date = date.today() - timedelta(days_back)
    url = '{0}{1}-{2}-{3}'.format(BASE_URL, start_date.year, start_date.month, start_date.day)

    record_encoding = requests.get(url).encoding

    records = get_records(url)

    record_list = []
    for record in records:
        doc_id = record['article_id']

        record_list.append(RawDocument({
            'doc': json.dumps(record),
            'source': NAME,
            'docID': unicode(doc_id),
            'filetype': 'json'
        }))

    return record_list


def get_records(url):
    records = requests.get(url)
    total_records = records.json()['items_found']
    all_records = []
    page = 1

    while len(all_records) < total_records:
        record_list = records.json()['items']

        for record in record_list:
            all_records.append(record)
        page += 1
        url += '&page={}'.format(page)
        records = requests.get(url)
        time.sleep(3)

    return all_records


def get_contributors(record):

    contributors = record['authors']

    contributor_list = []
    for person in contributors:
        name = HumanName(person['author_name'])
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


def get_ids(record):

    return {
        'serviceID': unicode(record['article_id']), 
        'url': record['url'], 
        'doi': record['DOI'].replace('http://dx.doi.org/', '')
    }


def get_properties(record):

    return {
        'type': record['type'],
        'defined_type': record['defined_type'], 
        'article_id': record['article_id']
    }


def normalize(raw_doc):
    doc = raw_doc.get('doc')
    record = json.loads(doc)

    normalized_dict = {
        'title': record['title'],
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'description': record['description'],
        'tags': [],
        'id': get_ids(record),
        'source': NAME,
        'dateUpdated': unicode(parse(record['modified_date']).isoformat()),
        'dateCreated': unicode(parse(record['published_date']).isoformat()),
    }

    print(json.dumps(normalized_dict, indent=4))
    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
