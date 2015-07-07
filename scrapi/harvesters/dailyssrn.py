from __future__ import unicode_literals

from dateutil.parser import parse
# from datetime import date, timedelta

import furl
from lxml import etree

from scrapi import requests
# from scrapi import settings
from scrapi.base import XMLHarvester
from scrapi.linter import RawDocument
from scrapi.util import copy_to_unicode
from scrapi.base.helpers import compose, single_result


class DailyssrnHarvester(XMLHarvester):
    short_name = 'dailyssrn'
    long_name = 'RSS Feed from the Social Science Research Network'
    url = 'http://papers.ssrn.com/'

    schema = {
        "description": ('//description/node()', compose(lambda x: x.strip(), single_result)),
        "title": ('//title/node()', compose(lambda x: x.strip(), single_result)),
        "providerUpdatedDateTime": ('//pubDate/node()', compose(lambda x: x.isoformat(), parse, lambda x: x.strip(), single_result)),
        "contributors": '//contributors/node()',
        "uris": {
            "canonicalUri": ('//link/node()', compose(lambda x: x.strip(), single_result)),
        }
    }

    def harvest(self, start_date=None, end_date=None):

        url = 'http://dailyssrn.com/rss/rss-all-2.0.xml'

        data = requests.get(url)
        doc = etree.XML(data.content)

        records = doc.xpath('channel/item')

        xml_list = []
        for record in records:
            doc_id = parse_id_from_url(record.xpath('link/node()'))
            record = etree.tostring(record)
            xml_list.append(RawDocument({
                'doc': record,
                'source': self.short_name,
                'docID': copy_to_unicode(doc_id),
                'filetype': 'xml'
            }))

        return xml_list


def parse_id_from_url(url):
    parsed_url = furl.furl(url[0])
    return parsed_url.args['abstract_id']
