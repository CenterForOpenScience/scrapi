''' Consumer for figshare '''
from __future__ import unicode_literals


import os
import xml
import time
import json
from dateutil.parser import parse
from datetime import date, timedelta, datetime

import requests


from nameparser import HumanName

from scrapi.linter import lint
from scrapi.linter.document import RawDocument, NormalizedDocument

NAME = 'figshare'
URL = 'http://api.figshare.com/v1/articles/search?search_for=*&from_date='


def consume(days_back=0):
    start_date = date.today() - timedelta(days_back) - timedelta(1)
    end_date = date.today() - timedelta(1)
    search_url = '{0}{1}-{2}-{3}&end_date={4}-{5}-{6}'.format(URL,
                                         start_date.year,
                                         start_date.month,
                                         start_date.day,
                                         end_date.year,
                                         end_date.month,
                                         end_date.day)
    import pdb; pdb.set_trace()
    print search_url

    records = get_records(search_url)

    record_list = []
    for record in records:
        doc_id = record['article_id']

        record_list.append(
            RawDocument(
                {
                    'doc': json.dumps(record),
                    'source': NAME,
                    'docID': unicode(doc_id),
                    'filetype': 'json'
                }
            )
        )

    return record_list


def get_records(search_url):
    records = requests.get(search_url)
    total_records = records.json()['items_found']
    page = 1
    # full_records = requests.get(article_url + str(page))
    all_records = []
    while len(all_records) < total_records:
        record_list = records.json()['items']

        for record in record_list:
            if len(all_records) < total_records:
                all_records.append(record)

        page += 1
        records = requests.get(search_url + '&page='.format(str(page)))
        time.sleep(3)

    return all_records


def get_contributors(record):

    authors = record['authors']

    contributor_list = []
    for person in authors:
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
        'article_id': record['article_id'],
        'defined_type': record['defined_type'],
        'type': record['type'],
        'links': record['links']
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

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
