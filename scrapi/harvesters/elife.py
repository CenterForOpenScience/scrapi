"""
A harvester for metadata from eLife on Github for the SHARE project

Sample API call:
"""

from __future__ import unicode_literals

import logging
from datetime import date, timedelta

from lxml import etree
import json

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import default_name_parser, build_properties, compose, single_result, datetime_formatter

logger = logging.getLogger(__name__)

#gather commits, each commit has a date associated with it, note max 100 per page and 3 pages pages
#https://api.github.com/repos/elifesciences/elife-articles/commits?page=1&per_page=100
#https://api.github.com/repos/elifesciences/elife-articles/commits?page=1&per_page=100&since=2014-01-01&until=2014-01-04

#go to commit url

#go to files, scrape xml file names

#grab files from https://raw.githubusercontent.com/elifesciences/elife-articles/master/[xml file name]

#apply xml scraper
#<pub-date publication-format> </pub-date>

#for a specific article - you can reach the full journal article at http://dx.doi.org/10.7554/eLife.00181
#where 00181 is the elocation-id

#example: https://raw.githubusercontent.com/elifesciences/elife-articles/master/elife06011.xml


def fetch_commits(base_url, start_date, end_date):
    resp = requests.get(base_url, params={
        'since': start_date,
        'until': end_date,
        'page': '1', #will need to update this to pull from multiple pages
        'per_page': 100,
    })

    jsonstr = resp.content.decode('utf-8')
    jsonstr = jsonstr.replace('},{', '}\\n{')
    jsonstr = jsonstr[1:-1]
    jsonarr = jsonstr.split('\\n')

    shas = []
    for jsonstr in jsonarr:
        jsonobj = json.loads(jsonstr)
        shas.append(jsonobj['sha'])
    return shas


def fetch_file_names(commit_url, sha):
    resp = requests.get(commit_url.format(sha))

    jsonstr = resp.content.decode('utf-8')
    jsonobj = json.loads(jsonstr)

    files = [d['filename'] for d in jsonobj['files']]
    return files


def fetch_xml(xml_url, filename):
    xml_text = requests.get(xml_url.format(filename)).content
    xml = etree.fromstring(xml_text)
    return xml


def clean_title(title):
    new_title = title.encode('utf-8')
    return new_title


class ELifeHarvester(XMLHarvester):
    short_name = 'elife'
    long_name = 'eLife Sciences'
    url = 'http://elifesciences.org/'
    DEFAULT_ENCODING = 'UTF-8'
    record_encoding = None

    namespaces = {}

    MAX_ROWS_PER_REQUEST = 999
    BASE_URL = 'https://api.github.com/repos/elifesciences/elife-articles/commits?'
    BASE_COMMIT_URL = 'https://api.github.com/repos/elifesciences/elife-articles/commits/{}'
    BASE_DATA_URL = 'https://raw.githubusercontent.com/elifesciences/elife-articles/master/{}'
    #BASE_PROJECT_URL = 'http://dx.doi.org/10.7554/eLife.{}'

    def harvest(self, start_date=None, end_date=None):

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        shas = fetch_commits(self.BASE_URL, start_date.isoformat(), end_date.isoformat())

        files = []
        for sha in shas:
            files = files + fetch_file_names(self.BASE_COMMIT_URL, sha)
            files = list(set(files))

        xml_records = []
        for file in files:
            xml_records.append(fetch_xml(self.BASE_DATA_URL, file))

        test = xml_records[0]
        #print(etree.tostring(test))
        #print(test.xpath('//abstract/p')[0].text)

        return [
            RawDocument({
                'filetype': 'xml',
                'source': self.short_name,
                'doc': etree.tostring(record),
                'docID': record.xpath('//article-id')[1].text,
            }) for record in xml_records
        ]

    schema = {
        'uris': {
            'canonicalUri': ('//article-id/node()', compose('http://dx.doi.org/10.7554/eLife.{}'.format, single_result)),
        },
        'contributors': ('//contrib[@contrib-type="author"]/node()', default_name_parser), #need to do custom
        'providerUpdatedDateTime': ('//pub-date/month/node()', compose(datetime_formatter, single_result)), #need to do custom
        'title': ('//article-title/node()'), #need to remove bold and italics from titles
        'description': ('//abstract/node()', single_result), #need to deal with bold and italics
        'publisher': {
            'name': ('//publisher-name/node()', single_result)
        },
        'otherProperties': build_properties(
            ('eissn', '//str[@name="eissn"]/node()'),
            ('articleType', '//str[@name="article_type"]/node()'),
            ('score', '//float[@name="score"]/node()')
        )
    }
