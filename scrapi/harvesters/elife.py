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


class ELifeHarvester(XMLHarvester):
    short_name = 'elife'
    long_name = 'eLife Sciences'
    url = 'https://github.com/elifesciences'
    DEFAULT_ENCODING = 'UTF-8'
    record_encoding = None

    namespaces = {}

    MAX_ROWS_PER_REQUEST = 999
    BASE_URL = 'https://api.github.com/repos/elifesciences/elife-articles/commits?'
    BASE_COMMIT_URL = 'https://api.github.com/repos/elifesciences/elife-articles/git/commits/{}'
    #BASE_DATA_URL = 'https://raw.githubusercontent.com/elifesciences/elife-articles/master/{}'
    #BASE_PROJECT_URL = 'http://dx.doi.org/10.7554/eLife.{}'

    def fetch_commits(self, start_date, end_date):

        resp = requests.get(self.BASE_URL, params={
            'since': start_date,
            'until': end_date,
            'page': '1',
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

    def fetch_file_names(self, sha):

        files = requests.get(self.BASE_COMMIT_URL('f7f18ca1b3d0ea18694ce913e0d56e7e99b26007'))
        print(files.content.decode('utf-8'))

        #decoder = json.JSONDecoder(strict=False)
        #text = resp.content.decode('utf-8')
        #json_files = decoder.raw_decode(text)
        #print(text)
        #commits = json.dumps(json_files)
        #commits_json = json.loads(text)
        #print(commits_json[0])
        #print(type(commits_json))
        #print(type(commits_json))

        #t = etree.XML(text)
        #print(t)
        #t = etree.parse(resp.content, parser)
        #commits = t.findall('sha')
        #print(commits)

    def harvest(self, start_date=None, end_date=None):

        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        return [
            RawDocument({
                'filetype': 'xml',
                'source': self.short_name,
                'doc': etree.tostring(row),
                'docID': row.xpath("str[@name='id']")[0].text,
            })
            for row in
            self.fetch_commits(start_date.isoformat(), end_date.isoformat())
            if row.xpath("arr[@name='abstract']")
            or row.xpath("str[@name='author_display']")
        ]

    schema = {
        'uris': {
            'canonicalUri': ('//str[@name="id"]/node()', compose('http://dx.doi.org/{}'.format, single_result)),
        },
        'contributors': ('//arr[@name="author_display"]/str/node()', default_name_parser),
        'providerUpdatedDateTime': ('//date[@name="publication_data"]/node()', compose(datetime_formatter, single_result)),
        'title': ('//str[@name="title_display"]/node()', single_result),
        'description': ('//arr[@name="abstract"]/str/node()', single_result),
        'publisher': {
            'name': ('//str[@name="journal"]/node()', single_result)
        },
        'otherProperties': build_properties(
            ('eissn', '//str[@name="eissn"]/node()'),
            ('articleType', '//str[@name="article_type"]/node()'),
            ('score', '//float[@name="score"]/node()')
        )
    }
