# -*- coding: utf-8 -*- t
"""
A harvester for metadata from eLife on Github for the SHARE project

Sample API call:https://api.github.com/repos/elifesciences/elife-articles/commits?since=2015-12-22&until=2015-12-31&page=1&per_page=100
Will need to set up Personal access token
"""

from __future__ import unicode_literals

import datetime
import json
import logging
import sys
from itertools import chain

from lxml import etree

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.base.helpers import build_properties, compose, single_result, datetime_formatter
from scrapi.linter.document import RawDocument

logger = logging.getLogger(__name__)


def collapse_list(list_of_strings):
    text = ''.join(list_of_strings)
    return text


def elife_name_parser(names):
    contributors = []
    for i in range(0, len(names), 2):
        chunka = names[i:i + 2]
        chunkb = chunka[1].split(" ")
        name = (chunka + chunkb)
        del name[1]
        contributors.append(name)

    parsed_contributors = []
    for contributor in contributors:
        if sys.version_info < (3,):
            contributor = map(lambda x: x.encode('utf-8'), contributor)

        if len(contributor) == 3:
            full_name = contributor[1] + str(" ") + contributor[2] + str(" ") + contributor[0]
            first_name = contributor[1]
            middle_name = contributor[2]
            last_name = contributor[0]

        else:
            full_name = contributor[1] + str(" ") + contributor[0]
            first_name = contributor[1]
            middle_name = ""
            last_name = contributor[0]

        contributor_dict = {
            'name': full_name,
            'givenName': first_name,
            'additionalName': middle_name,
            'familyName': last_name
        }

        parsed_contributors.append(contributor_dict)

    return parsed_contributors


def elife_date_parser(date):
    date_form = datetime.datetime(int(date[2]), int(date[1]), int(date[0]), 0, 0)
    return date_form.date().isoformat()


def fetch_commits(base_url, start_date, end_date):

    jsonstr = ""
    i = 1
    while True:
        resp = requests.get(base_url, params={
            'since': start_date,
            'until': end_date,
            'page': i,
            'per_page': 100,
        })

        jsonchunk = resp.content.decode('utf-8')
        if len(jsonchunk) <= 2:
            break
        i += 1

        jsonchunk = jsonchunk.replace('},{', '}\\n{')
        jsonchunk = jsonchunk[1:-1]
        jsonstr = jsonstr + "\\n" + jsonchunk

    jsonarr = jsonstr.split('\\n')[1:]

    shas = []
    for jsonstring in jsonarr:
        jsonobj = json.loads(jsonstring)
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

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date or datetime.date.today() - datetime.timedelta(settings.DAYS_BACK)
        end_date = end_date or datetime.date.today()

        shas = fetch_commits(self.BASE_URL, start_date.isoformat(), end_date.isoformat())

        files = list(set(chain.from_iterable([
            fetch_file_names(self.BASE_COMMIT_URL, sha)
            for sha in shas])))

        files = filter(lambda filename: filename.endswith('.xml'), files)

        xml_records = [
            fetch_xml(self.BASE_DATA_URL, filename)
            for filename in files
        ]

        return [
            RawDocument({
                'filetype': 'xml',
                'source': self.short_name,
                'doc': etree.tostring(record),
                'docID': record.xpath('//article-id[@*]')[0].text,
            }) for record in xml_records
        ]

    schema = {
        'uris': {
            'canonicalUri': ('//article-id/node()', compose('http://dx.doi.org/10.7554/eLife.{}'.format,
                                                            single_result)),
            'objectUri': ('//article-id/node()', compose('http://dx.doi.org/10.7554/eLife.{}'.format, single_result))
        },
        'contributors': ('//article-meta/contrib-group/contrib/name/*[not(self::suffix)]/node()', elife_name_parser),
        'providerUpdatedDateTime': ('//article-meta/pub-date[@publication-format="electronic"]/*/node()',
                                    compose(datetime_formatter, elife_date_parser)),
        'title': ('//article-meta/title-group/article-title//text()', collapse_list),
        'description': ('//abstract[not(@abstract-type="executive-summary")]/p[1]//text()', collapse_list),
        'publisher': {
            'name': ('//publisher-name/node()', single_result)
        },
        'subjects': '//article-meta/article-categories/descendant::text()',
        'freeToRead': {
            'startDate': ('//article-meta/pub-date[@publication-format="electronic"]/*/node()',
                          elife_date_parser)
        },
        'tags': '//kwd/text()',
        'otherProperties': build_properties(
                ('rights', ('//permissions/license/license-p/ext-link/text()', single_result))
        )
    }
