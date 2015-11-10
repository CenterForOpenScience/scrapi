import six
from datetime import date
import json
from datetime import timedelta

from scrapi import requests
from scrapi import settings
from scrapi.base import BaseHarvester, HarvesterMeta, helpers
from scrapi.linter.document import RawDocument, NormalizedDocument


def gen_harvesters():
    return {
        source['short_name']: gen_harvester(**source)
        for source in get_sources()
    }


def get_sources():
    response = helpers.null_on_error(requests.get('{}/provider_list'.format(settings.SHARE_REG_URL), force=True).json)()
    while True:
        if (not response) or (not response.get('results')):
            break
        for source in response['results']:
            yield source
        response = helpers.null_on_error(requests.get(response['next']).json)()


def gen_harvester(short_name=None, long_name=None, url=None, favicon_dataurl=None, **kwargs):
    assert short_name and long_name and url and favicon_dataurl

    from scrapi.processing.elasticsearch import DatabaseManager
    dm = DatabaseManager()
    dm.setup()
    es = dm.es

    es.index(
        'share_providers',
        short_name,
        body={
            'favicon': favicon_dataurl,
            'short_name': short_name,
            'long_name': long_name,
            'url': url
        },
        id=short_name,
        refresh=True
    )
    return type(
        '{}Harvester'.format(short_name.lower().capitalize()),
        (PushApiHarvester, ),
        dict(short_name=short_name, long_name=long_name, url=url)
    )


@six.add_metaclass(HarvesterMeta)
class PushApiHarvester(BaseHarvester):
    file_format = 'json'

    def harvest(self, start_date=None, end_date=None):
        start_date = start_date or date.today() - timedelta(settings.DAYS_BACK)
        end_date = end_date or date.today()

        return [
            RawDocument({
                'doc': json.dumps(record),
                'source': self.short_name,
                'docID': record['shareProperties']['docID'],
                'filetype': 'json'
            }) for record in self.get_records()
        ]

    def get_records(self, start_date, end_date):
        response = requests.get(
            '{}/established'.format('settings.SHARE_REG_URL'),
            params={'from': start_date.isoformat(), 'to': end_date.isoformat()}
        ).json()
        for record in response['results']:
            yield record

        while response.get('next'):
            response = requests.get(response['next'])
            for record in response['results']:
                yield record

    def normalize(self, raw):
        return NormalizedDocument(json.loads(raw['doc']))

    def run_at(self):
        return {
            'minute': '*/15'
        }


harvesters = gen_harvesters()
