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
URL = 'http://api.figshare.com/v1/articles/search?search_for=*&from_date='
ARTICLE_URL = 'http://api.figshare.com/v1/articles?page='


def consume(days_back=0):
    start_date = date.today() - timedelta(days_back)
    search_url = '{0}{1}-{2}-{3}'.format(URL,
                                         start_date.year,
                                         start_date.month,
                                         start_date.day)

    records = get_records(search_url, ARTICLE_URL)

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


def get_records(search_url, article_url):
    records = requests.get(search_url)
    print(records.url)
    total_records = records.json()['items_found']
    page = 1
    full_records = requests.get(article_url + str(page))
    print(full_records.url)
    all_records = []
    while len(all_records) < total_records:
        record_list = full_records.json()['items']

        for record in record_list:
            if len(all_records) < total_records:
                all_records.append(record)

        page += 1
        full_records = requests.get(article_url + str(page))
        print(full_records.url)
        time.sleep(3)

    return all_records


def get_contributors(record):

    authors = record['authors']

    contributor_list = []
    for person in authors:
        name = HumanName(person['full_name'])
        contributor = {
            'prefix': name.title,
            'given': person['first_name'],
            'middle': name.middle,
            'family': person['last_name'],
            'suffix': name.suffix,
            'email': '',
            'ORCID': '',
        }
        contributor_list.append(contributor)
    return contributor_list


def get_ids(record):

    return {
        'serviceID': unicode(record['article_id']),
        'url': record['figshare_url'],
        'doi': record['doi'].replace('http://dx.doi.org/', '')
    }


def get_properties(record):

    return {
        'article_id': record['article_id'],
        'views': record['views'],
        'downloads': record['downloads'],
        'shares': record['shares'],
        'publisher_doi': record['publisher_doi'],
        'publisher_citation': record['publisher_citation'],
        'master_publisher_id': record['master_publisher_id'],
        'status': record['status'],
        'version': record['version'],
        'description': record['description'],
        'total_size': record['total_size'],
        'defined_type': record['defined_type'],
        'files': record['files'],
        'owner': record['owner'],
        'tags': record['tags'],
        'categories': record['categories'],
        'links': record['links']
    }


def normalize(raw_doc):
    doc = raw_doc.get('doc')
    record = json.loads(doc)

    normalized_dict = {
        'title': record['title'],
        'contributors': get_contributors(record),
        'properties': get_properties(record),
        'description': record['description_nohtml'],
        'tags': [tag['name'] for tag in record['tags']] + [cat['name'] for cat in record['categories']],
        'id': get_ids(record),
        'source': NAME,
        'dateUpdated': unicode(parse(record['published_date']).isoformat()),
        'dateCreated': unicode(parse(record['published_date']).isoformat()),
    }

    # TODO - The modifiedDate does not appear in the extended records
    # This might lead to a bug in us collecting duplicate records that haven't
    # been updated, and us not getting updated articles. 
    # the articles route only seems to show new, while the search shows updated

    return NormalizedDocument(normalized_dict)


if __name__ == '__main__':
    print(lint(consume, normalize))
