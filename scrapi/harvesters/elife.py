"""
A harvester for metadata from eLife on Github for the SHARE project

Sample API call:https://api.github.com/repos/elifesciences/elife-articles/commits?since=2015-12-22&until=2015-12-31&page=1&per_page=100
"""

from __future__ import unicode_literals

import logging
import datetime

from lxml import etree
import json

from scrapi import requests
from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.linter.document import RawDocument
from scrapi.base.helpers import build_properties, compose, single_result, datetime_formatter

logger = logging.getLogger(__name__)


def title_parser(title):
    parsed_title = ''.join(title)
    return parsed_title


def description_parser(description):
    try:
        if "DOI:" in description[-3]:
            del description[-3:]
    except:
        pass
    parsed_description = ''.join(description)
    return parsed_description


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
        if len(contributor) == 3:
            contributor_dict = {
                'name': str(contributor[1] + " " + contributor[2] + " " + contributor[0]),
                'givenName': contributor[1],
                'additionalName': contributor[2],
                'familyName': contributor[0],
            }
        else:
            contributor_dict = {
                'name': str(contributor[1] + " " + contributor[0]),
                'givenName': contributor[1],
                'additionalName': "",
                'familyName': contributor[0],
            }
        parsed_contributors.append(contributor_dict)

    return parsed_contributors


def elife_date_parser(date):
    date_form = datetime.datetime(int(date[2]), int(date[1]), int(date[0]), 0, 0)
    return date_form.date().isoformat()


# will need to update this to pull from multiple pages (right now there are 3 total)
def fetch_commits(base_url, start_date, end_date):
    resp = requests.get(base_url, params={
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

        files = []
        for sha in shas:
            files = files + fetch_file_names(self.BASE_COMMIT_URL, sha)
            files = list(set(files))

        xml_records = []
        for file in files:
            xml_records.append(fetch_xml(self.BASE_DATA_URL, file))

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
        },
        'contributors': ('//article-meta/contrib-group/contrib/name/*[not(self::suffix)]/node()', elife_name_parser),
        'providerUpdatedDateTime': ('//article-meta/pub-date[@publication-format="electronic"]/*/node()',
                                    compose(datetime_formatter, elife_date_parser)),
        'title': ('//article-meta/title-group/article-title//text()', title_parser),
        'description': ('//abstract[not(@abstract-type="executive-summary")]/p//text()', description_parser),
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
                ('license', ('//permissions/license/license-p/ext-link/text()', single_result))
        )
    }
